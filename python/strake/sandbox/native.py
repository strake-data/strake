import logging
import uuid
import os
import sys
import multiprocessing as mp
import subprocess
import time
import asyncio
import json
from typing import Optional, Union, Any

from .base import SandboxManager, SandboxResult, SandboxErrorMessages
from .core import validate_ast, _sandbox_worker_inner
from dataclasses import dataclass

logger = logging.getLogger("strake.sandbox.native")

# Single pre-spawned context to avoid module reinitialization overhead repeatedly.
_MP_CTX = mp.get_context("spawn")


@dataclass(frozen=True)
class SandboxConfig:
    """Centralized configuration for sandbox limits and parameters."""

    timeout_secs: float = 30.0
    memory_limit_bytes: int = 512 * 1024 * 1024
    max_output_size: int = 10 * 1024 * 1024
    max_code_size: int = 1024 * 1024
    fc_socket_retries: int = 50
    fc_retry_delay_secs: float = 0.01
    marker_start: str = "===RESULT_START==="
    marker_end: str = "===RESULT_END==="

    @classmethod
    def from_env(cls) -> "SandboxConfig":
        """Loads configuration from environment variables with safe fallbacks."""
        try:
            timeout = float(os.environ.get("SANDBOX_TIMEOUT_SECS", "30.0"))
        except ValueError:
            logger.warning("Invalid SANDBOX_TIMEOUT_SECS, using default")
            timeout = 30.0

        try:
            memory = int(
                os.environ.get("SANDBOX_MEMORY_LIMIT", str(512 * 1024 * 1024))
            )
        except ValueError:
            logger.warning("Invalid SANDBOX_MEMORY_LIMIT, using default")
            memory = 512 * 1024 * 1024

        return cls(timeout_secs=timeout, memory_limit_bytes=memory)


def _sandbox_worker(
    os_type: str,
    code: str,
    queue: mp.Queue,
    config_path: str,
    workspace_root: Optional[str] = None,
):

    # IMPORTANT: Create the StrakeConnection BEFORE applying sandbox constraints.
    try:
        from strake import StrakeConnection

        if config_path:
            connection = StrakeConnection(config_path)
            os.environ.pop("STRAKE_CONFIG", None)
        else:
            token = os.environ.pop("STRAKE_TOKEN", None)
            try:
                url = os.environ.pop("STRAKE_URL", "grpc://127.0.0.1:50051")
                connection = StrakeConnection(url, api_key=token)
            finally:
                token = None  # Ensure token is cleared even if connection fails

    except Exception as e:
        import traceback

        logger.error(
            f"Sandbox initialization failed: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        )
        queue.put(
            SandboxResult(stdout="", stderr=SandboxErrorMessages.CONNECTION_FAILED, result=None).to_dict()
        )
        return

    # Now apply sandbox constraints
    try:
        from strake.policy import SandboxPolicy, SandboxAttestation

        policy = SandboxPolicy(workspace_root=workspace_root)
        applied = policy.apply_for_os(os_type)
        if not applied and os_type != "none":
            logger.warning(f"Failed to apply any sandbox constraints for {os_type}!")
            if os.environ.get("SANDBOX_FAIL_CLOSED", "false").lower() == "true":
                queue.put(SandboxResult(
                    stdout="", 
                    stderr=f"Security Error: Sandbox constraints failed to apply for {os_type}", 
                    result=None
                ).to_dict())
                return

    except Exception as e:
        logger.warning(f"Could not apply sandbox constraints via policy: {e}")
        if os.environ.get("SANDBOX_FAIL_CLOSED", "false").lower() == "true":
            queue.put(SandboxResult(
                stdout="", 
                stderr=f"Security Error: Sandbox constraints failed: {e}", 
                result=None
            ).to_dict())
            return

    _sandbox_worker_inner(code, queue, connection)


class NativeOSSandboxManager(SandboxManager):
    """
    Base class for sandboxes that rely on native OS capabilities
    like namespaces, cgroups, or job objects.
    """

    def __init__(
        self,
        connection,
        config_path: Optional[str] = None,
        workspace_root: Optional[str] = None,
    ):
        super().__init__(connection, config_path)
        self.workspace_root = workspace_root or os.getcwd()
        import concurrent.futures
        import weakref
        # Dedicated executor for blocking queue operations
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._finalizer = weakref.finalize(self, self._cleanup_executor, self._executor)

    @staticmethod
    def _cleanup_executor(executor):
        """Clean up the thread executor securely."""
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    def _get_sandbox_name(self) -> str:
        return "Native OS Sandbox"

    def _get_os_type(self) -> str:
        return "none"

    def _get_timeout(self, timeout_secs: Optional[float]) -> float:
        config = SandboxConfig.from_env()
        return timeout_secs if timeout_secs is not None else config.timeout_secs

    async def run(
        self, code: str, timeout_secs: Optional[float] = None
    ) -> "SandboxResult":
        logger.info(f"Initializing {self._get_sandbox_name()}...")

        # 1. AST Validation (Defense in Depth)
        # This is a fast-path layer 1 guardrail.
        ast_error = validate_ast(code)
        if ast_error:
            import hashlib
            logger.warning(
                "Sandbox security violation: %s (hash=%s)",
                ast_error,
                hashlib.sha256(code.encode()).hexdigest()[:16],
            )
            return SandboxResult(
                stdout="", stderr=f"Security Error: {ast_error}", result=None
            )

        queue = _MP_CTX.Queue()

        # The _sandbox_worker function is designed to run in a separate process.
        # It handles the connection setup and then calls _sandbox_worker_inner.
        p = _MP_CTX.Process(
            target=_sandbox_worker,
            args=(
                self._get_os_type(),
                code,
                queue,
                self.config_path,
                self.workspace_root,
            ),
        )

        timeout = self._get_timeout(timeout_secs)

        try:
            p.start()
            
            # The parent MUST read from the queue before calling process.join() to
            # prevent deadlock if the child writes a large result payload to the pipe.
            def _get_result_from_queue():
                import queue as q
                try:
                    return queue.get(timeout=timeout)
                except q.Empty:
                    return None

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self._executor, _get_result_from_queue)

            if p.is_alive():
                # Give the process a brief moment to exit cleanly after putting its result
                p.join(timeout=0.1)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=5.0)  # Ensure the process is fully terminated but don't block forever
                    if p.is_alive():
                        logger.critical(f"Failed to terminate sandbox process {p.pid}")
            
            # [Robustness] Check queue after process death to catch race conditions.
            # We drain multiple times with a small timeout to ensure we capture the full payload.
            if result is None:
                import queue as q
                for _ in range(3):
                    try:
                        result = queue.get(timeout=0.2)
                        if result:
                            break
                    except q.Empty:
                        continue

            if result is not None:
                if isinstance(result, dict):
                    return SandboxResult.from_dict(result)
                if isinstance(result, SandboxResult):
                    return result
                return SandboxResult(stdout="", stderr=str(result), result=None)

            # If process exited but queue is empty
            if p.exitcode is not None and p.exitcode != 0:
                logger.error(f"Sandbox worker exited with code {p.exitcode}")
                return SandboxResult(
                    stdout="",
                    stderr=f"Runtime Error: Sandbox process exited with code {p.exitcode}",
                    result=None,
                )

            return SandboxResult(
                stdout="",
                stderr="Execution Error: Sandbox process finished without returning output.",
                result=None,
            )
        except Exception as e:
            logger.error(f"Error retrieving result from sandbox queue: {e}")
            return SandboxResult(
                stdout="",
                stderr=SandboxErrorMessages.INTERNAL_FAILURE if "Internal" in str(e) else f"Execution Error: Failed to retrieve sandbox output: {str(e)}",
                result=None,
            )
        finally:
            try:
                queue.close()
            except Exception as e:
                logger.debug(f"Queue close error (may be harmless): {e}")
            try:
                queue.join_thread()
            except Exception as e:
                logger.debug(f"Queue join error: {e}")


class LinuxSandboxManager(NativeOSSandboxManager):
    """
    Linux Native Sandbox using Namespace Isolation (unshare CLONE_NEWNET),
    Syscall Filtering (Seccomp-BPF), Filesystem Isolation (Landlock LSM),
    and Resource Governance (cgroups, setrlimit).
    """

    def _get_sandbox_name(self) -> str:
        return "Linux Sandbox (Landlock + Seccomp + Namespaces)"

    def _get_os_type(self) -> str:
        return "linux"


class MacOSSandboxManager(NativeOSSandboxManager):
    """
    macOS Native Sandbox using Seatbelt (sandbox-exec) for filesystem and
    network isolation, plus rlimits for resource governance.

    Unlike the Linux path (which applies restrictions in-process via syscalls),
    macOS Seatbelt must wrap the entire child process. This manager overrides
    run() to spawn the sandbox worker through /usr/bin/sandbox-exec with a
    dynamically generated SBPL profile.
    """

    _SEATBELT_EXECUTABLE = "/usr/bin/sandbox-exec"

    def _get_sandbox_name(self) -> str:
        return "macOS Sandbox (Seatbelt + SIP)"

    def _get_os_type(self) -> str:
        return "macos"

    async def run(
        self, code: str, timeout_secs: Optional[float] = None
    ) -> "SandboxResult":
        """
        Execute code inside a macOS Seatbelt sandbox.
        """
        logger.info(f"Initializing {self._get_sandbox_name()}...")

        # 1. AST Validation (Defense in Depth)
        ast_error = validate_ast(code)
        if ast_error:
            import hashlib
            logger.warning(
                "Sandbox security violation: %s (hash=%s)",
                ast_error,
                hashlib.sha256(code.encode()).hexdigest()[:16],
            )
            return SandboxResult(
                stdout="", stderr=f"Security Error: {ast_error}", result=None
            )

        # 2. Generate Seatbelt profile
        from strake.policy import SandboxPolicy

        policy = SandboxPolicy(workspace_root=self.workspace_root)
        applied = policy.to_macos()

        if not policy._seatbelt_profile_path:
            # Seatbelt profile generation failed — fall back to base class (rlimits only)
            logger.warning(
                "Seatbelt profile not generated. Falling back to rlimit-only sandbox."
            )
            return await super().run(code, timeout_secs)

        profile_path = policy._seatbelt_profile_path

        # 3. Build the sandbox-exec command
        cmd = [
            self._SEATBELT_EXECUTABLE,
            "-f",
            profile_path,
            sys.executable,
            "-v",  # Standardize for some environments
            "-m",
            "strake.sandbox.macos_worker",
        ]

        # Pass limited environment variables to prevent leaking host secrets
        # Token is now passed via stdin/pipe, not ENV.
        ALLOWED_ENV_VARS = {
            "PATH",
            "HOME",
            "USER",
            "LANG",
            "LC_ALL",
            "STRAKE_CONFIG",
            "STRAKE_URL",
            "SANDBOX_TIMEOUT_SECS",
            "SANDBOX_FAIL_CLOSED",
        }
        env = {k: v for k, v in os.environ.items() if k in ALLOWED_ENV_VARS}

        token = os.environ.get("STRAKE_TOKEN")

        if self.config_path:
            env["STRAKE_CONFIG"] = self.config_path
        env["STRAKE_SANDBOX"] = "seatbelt"

        timeout = self._get_timeout(timeout_secs)

        # 4. Apply Resource Limits to parent subprocess creation
        # We set soft limits on the parent so the child inherits them.
        import resource

        old_cpu = resource.getrlimit(resource.RLIMIT_CPU)
        old_mem = resource.getrlimit(resource.RLIMIT_AS)
        config = SandboxConfig.from_env()

        # Set parent soft limits to the requested sandbox bounds
        resource.setrlimit(resource.RLIMIT_CPU, (int(timeout) + 2, old_cpu[1]))
        resource.setrlimit(resource.RLIMIT_AS, (config.memory_limit_bytes, old_mem[1]))

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=self.workspace_root,
            )
        finally:
            # Restore parent limits immediately
            resource.setrlimit(resource.RLIMIT_CPU, old_cpu)
            resource.setrlimit(resource.RLIMIT_AS, old_mem)

        try:
            try:
                # [Hardening] Pass token and code via JSON payload over pipe
                payload = json.dumps({"token": token, "code": code}).encode("utf-8")
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(input=payload), timeout=timeout
                )
            except asyncio.TimeoutError:
                if proc and proc.returncode is None:
                    try:
                        proc.kill()
                        await proc.wait()
                    except ProcessLookupError:
                        pass
                return SandboxResult(
                    stdout="",
                    stderr=SandboxErrorMessages.TIMEOUT,
                    result=None,
                )

            # [Security] Handle result serialization (worker uses JSON for SandboxResult)
            import json
            import re

            try:
                # The macOS worker writes JSON SandboxResult to stdout wrapped in markers
                decoded_stdout = stdout_bytes.decode("utf-8") if stdout_bytes else ""
                
                # Robust extraction using markers
                pattern = f"{re.escape(config.marker_start)}(.*?){re.escape(config.marker_end)}"
                match = re.search(pattern, decoded_stdout, re.DOTALL)
                
                if match:
                    result_dict = json.loads(match.group(1).strip())
                    result = SandboxResult.from_dict(result_dict)
                else:
                    # Fallback for older workers or direct output
                    last_line = decoded_stdout.strip().split("\n")[-1]
                    result_dict = json.loads(last_line)
                    result = SandboxResult.from_dict(result_dict)
                
                # Combine stderr captured by subprocess with worker's internal stderr
                result.stderr = (result.stderr or "") + (
                    stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
                )
                return result
            except Exception:
                # Fallback to plain string decoding if JSON fails
                stdout = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
                stderr = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
                return SandboxResult(stdout=stdout, stderr=stderr, result=None)

        except FileNotFoundError:
            logger.error(
                f"sandbox-exec not found at {self._SEATBELT_EXECUTABLE}. "
                "Falling back to rlimit-only sandbox."
            )
            return await super().run(code, timeout_secs)
        except Exception as e:
            logger.error(f"Seatbelt sandbox execution failed: {e}")
            return SandboxResult(
                stdout="",
                stderr=f"Execution Error: {str(e)}",
                result=None,
            )
        finally:
            # Clean up the temporary profile file
            try:
                os.unlink(profile_path)
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning(f"Failed to cleanup seatbelt profile {profile_path}: {e}")


class WindowsSandboxManager(NativeOSSandboxManager):
    """
    Windows Native Sandbox using Job Objects for resource limits and
    AppContainer/Low Integrity Level for capability restriction.
    """

    async def run(self, code: str, timeout_secs: Optional[float] = None) -> "SandboxResult":
        raise NotImplementedError("WindowsSandboxManager is not yet implemented.")

    def _get_sandbox_name(self) -> str:
        return "Windows Sandbox (AppContainer + Job Objects)"

    def _get_os_type(self) -> str:
        return "windows"
