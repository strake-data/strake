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

from .base import SandboxManager, SandboxResult
from .core import validate_ast, _sandbox_worker_inner

logger = logging.getLogger("strake.sandbox.native")

# Single pre-spawned context to avoid module reinitialization overhead repeatedly.
_MP_CTX = mp.get_context("spawn")


def _sandbox_worker(
    os_type: str,
    code: str,
    queue: mp.Queue,
    config_path: str,
    workspace_root: Optional[str] = None,
):
    sandbox_id = str(uuid.uuid4())

    try:
        from strake.policy import SandboxPolicy, SandboxAttestation

        policy = SandboxPolicy(workspace_root=workspace_root)
        applied = policy.apply_for_os(os_type)
        if not applied and os_type != "none":
            logger.warning(f"Failed to apply any sandbox constraints for {os_type}!")

        attestation = SandboxAttestation(
            sandbox_id,
            applied,
            time.time(),
            landlock_abi_version=policy._landlock_abi_version,
        )
        # In an enterprise product, we might pass the attestation signature back the queue
    except Exception as e:
        logger.warning(f"Could not apply sandbox constraints via policy: {e}")

    try:
        from strake import StrakeConnection

        if config_path:
            connection = StrakeConnection(config_path)
            # Remove configuration from env if it was present
            os.environ.pop("STRAKE_CONFIG", None)
        else:
            token = os.environ.pop("STRAKE_TOKEN", None)
            url = os.environ.pop("STRAKE_URL", "grpc://127.0.0.1:50051")
            connection = StrakeConnection(url, api_key=token)

            # Explicitly delete the token variable so it cannot be accidentally captured
            # in a closure deeper down.
            del token

        _sandbox_worker_inner(code, queue, connection)
    except Exception as e:
        import traceback

        logger.error(
            f"Sandbox initialization failed: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        )
        queue.put(
            "Runtime Error: Sandbox could not connect to the data engine. Check server logs."
        )


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

    def _get_sandbox_name(self) -> str:
        return "Native OS Sandbox"

    def _get_os_type(self) -> str:
        return "none"

    async def run(
        self, code: str, timeout_secs: Optional[float] = None
    ) -> "SandboxResult":
        logger.info(f"Initializing {self._get_sandbox_name()}...")

        # 1. AST Validation (Defense in Depth)
        # Check AST first as it's fast and doesn't need a process
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
            args=(self._get_os_type(), code, queue, self.config_path, self.workspace_root),
        )

        SANDBOX_TIMEOUT_SECS = int(os.environ.get("SANDBOX_TIMEOUT_SECS", 30))
        timeout = timeout_secs if timeout_secs is not None else SANDBOX_TIMEOUT_SECS

        try:
            p.start()
            # Use to_thread to wait for the process without blocking the event loop
            await asyncio.to_thread(p.join, timeout=timeout)

            if p.is_alive():
                p.kill()
                p.join()  # Ensure the process is fully terminated
                return SandboxResult(
                    stdout="",
                    stderr="Resource Error: Execution timed out.",
                    result=None,
                )

            # Check queue only if process exited cleanly or was killed
            if not queue.empty():
                result = queue.get()
                # The _sandbox_worker puts a SandboxResult object directly
                if isinstance(result, SandboxResult):
                    return result
                # If it's a string (e.g., error message), wrap it
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
                stderr=f"Execution Error: Failed to retrieve sandbox output: {str(e)}",
                result=None,
            )


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

        Instead of multiprocessing.Process (which applies restrictions
        after fork), this spawns the child through sandbox-exec so the
        Seatbelt profile is active before the Python interpreter starts.
        """
        logger.info(f"Initializing {self._get_sandbox_name()}...")

        # 1. AST Validation
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
        # We pass the code via stdin to avoid shell escaping issues
        cmd = [
            self._SEATBELT_EXECUTABLE,
            "-f", profile_path,
            sys.executable, "-c", code,
        ]

        # Pass environment variables for Strake connection
        env = os.environ.copy()
        if self.config_path:
            env["STRAKE_CONFIG"] = self.config_path
        env["STRAKE_SANDBOX"] = "seatbelt"

        SANDBOX_TIMEOUT_SECS = int(os.environ.get("SANDBOX_TIMEOUT_SECS", 30))
        timeout = timeout_secs if timeout_secs is not None else SANDBOX_TIMEOUT_SECS

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=self.workspace_root,
            )

            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return SandboxResult(
                    stdout="",
                    stderr="Resource Error: Execution timed out.",
                    result=None,
                )

            stdout = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
            stderr = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""

            if proc.returncode != 0:
                logger.error(
                    f"Seatbelt sandbox exited with code {proc.returncode}: {stderr[:500]}"
                )
                return SandboxResult(
                    stdout=stdout,
                    stderr=stderr or f"Runtime Error: Sandbox process exited with code {proc.returncode}",
                    result=None,
                )

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
                if os.path.exists(profile_path):
                    os.unlink(profile_path)
            except OSError:
                pass


class WindowsSandboxManager(NativeOSSandboxManager):
    """
    Windows Native Sandbox using Job Objects for resource limits and
    AppContainer/Low Integrity Level for capability restriction.
    """

    def _get_sandbox_name(self) -> str:
        return "Windows Sandbox (AppContainer + Job Objects)"

    def _get_os_type(self) -> str:
        return "windows"
