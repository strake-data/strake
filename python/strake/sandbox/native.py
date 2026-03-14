from __future__ import annotations

# Standard Library
import asyncio
import hashlib
import json
import logging
import multiprocessing as mp
import os
import sys
from dataclasses import dataclass
import signal
import threading
import concurrent.futures
import weakref
import re
from typing import Any, ClassVar, Dict, List, Optional, Tuple

# Local Application Imports
from strake.sandbox.base import SandboxManager, SandboxResult, SandboxErrorMessages
from strake.sandbox.core import validate_ast, _sandbox_worker_inner

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
    MARKER_START: str = "===RESULT_START==="
    MARKER_END: str = "===RESULT_END==="
    READY_TIMEOUT: float = 10.0
    DEFAULT_POOL_SIZE: int = 2

    @property
    def marker_start(self) -> str:
        return self.MARKER_START

    @property
    def marker_end(self) -> str:
        return self.MARKER_END

    @classmethod
    def from_env(cls) -> "SandboxConfig":
        """Loads configuration from environment variables with safe fallbacks."""
        try:
            timeout = float(os.environ.get("SANDBOX_TIMEOUT_SECS", "30.0"))
        except ValueError:
            logger.warning("Invalid SANDBOX_TIMEOUT_SECS, using default")
            timeout = 30.0

        try:
            memory = int(os.environ.get("SANDBOX_MEMORY_LIMIT", str(512 * 1024 * 1024)))
        except ValueError:
            logger.warning("Invalid SANDBOX_MEMORY_LIMIT, using default")
            memory = 512 * 1024 * 1024

        return cls(timeout_secs=timeout, memory_limit_bytes=memory)


@dataclass(frozen=True)
class PipeWrapper:
    """Compatibility wrapper adapting multiprocessing.Pipe to a Queue-like interface."""

    conn: mp.connection.Connection

    def put(self, item: Any):
        """Send item over the pipe."""
        self.conn.send(item)


def _initialize_connection(config_path: str | None) -> Any:
    """Initializes the StrakeConnection based on environment or config path."""
    if os.environ.get("STRAKE_SANDBOX_SKIP_CONN", "false").lower() == "true":
        logger.info("Skipping StrakeConnection initialization as requested.")
        return None

    try:
        from strake import StrakeConnection

        if config_path:
            connection = StrakeConnection(config_path)
            os.environ.pop("STRAKE_CONFIG", None)
            return connection

        token = os.environ.pop("STRAKE_TOKEN", None)
        try:
            url = os.environ.get("STRAKE_URL", "grpc://127.0.0.1:50051")
            return StrakeConnection(url, api_key=token)
        finally:
            token = None  # Ensure token is cleared even if connection fails
    except Exception as e:
        import traceback

        logger.error(
            f"Sandbox connection initialization failed: {e}\n{traceback.format_exc()}"
        )
        raise RuntimeError(SandboxErrorMessages.CONNECTION_FAILED) from e


def _apply_policy(os_type: str, workspace_root: str | None) -> None:
    """Applies sandbox security policy for the given OS type."""
    try:
        from strake.policy import SandboxPolicy

        policy = SandboxPolicy(workspace_root=workspace_root)
        applied = policy.apply_for_os(os_type)
        if not applied and os_type != "none":
            logger.warning(f"Failed to apply any sandbox constraints for {os_type}!")
            if os.environ.get("SANDBOX_FAIL_CLOSED", "false").lower() == "true":
                raise RuntimeError(
                    f"Security Error: Sandbox constraints failed to apply for {os_type}"
                )

    except Exception as e:
        logger.warning(f"Could not apply sandbox constraints via policy: {e}")
        if os.environ.get("SANDBOX_FAIL_CLOSED", "false").lower() == "true":
            raise RuntimeError(f"Security Error: Sandbox constraints failed: {e}")


def _sandbox_worker(
    os_type: str,
    conn_pipe: mp.connection.Connection,
    config_path: str | None,
    workspace_root: str | None = None,
) -> None:
    """
    A persistent worker process that initializes its environment once and then
    waits for code payloads via a pipe.
    """
    # Pre-load data science libraries
    try:
        import numpy
        import pandas
        import pyarrow
    except ImportError:
        pass

    try:
        connection = _initialize_connection(config_path)
    except RuntimeError as e:
        conn_pipe.send(SandboxResult(stdout="", stderr=str(e), result=None).to_dict())
        return

    try:
        _apply_policy(os_type, workspace_root)
    except RuntimeError as e:
        conn_pipe.send(SandboxResult(stdout="", stderr=str(e), result=None).to_dict())
        return

    # Signal ready
    conn_pipe.send({"status": "ready"})

    # Wait for the code payload
    try:
        payload = conn_pipe.recv()
        if not isinstance(payload, dict) or "code" not in payload:
            conn_pipe.send(
                SandboxResult(
                    stdout="",
                    stderr="Invalid payload: expected dict with 'code'",
                    result=None,
                ).to_dict()
            )
            return

        ctx = payload.get("execution_context")
        if isinstance(ctx, dict):
            kind = ctx.get("kind")
            guard_mode = ctx.get("guard_mode")
            session_id = ctx.get("session_id")
            if kind:
                os.environ["STRAKE_EXECUTION_CONTEXT"] = str(kind)
            if guard_mode:
                os.environ["STRAKE_AGENT_GUARD_MODE"] = str(guard_mode)
            if session_id:
                os.environ["STRAKE_AGENT_SESSION_ID"] = str(session_id)

        _sandbox_worker_inner(payload["code"], PipeWrapper(conn_pipe), connection)
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass
    except EOFError:
        pass  # Parent closed the connection
    except Exception as e:
        conn_pipe.send(
            SandboxResult(stdout="", stderr=f"Worker Error: {e}", result=None).to_dict()
        )


class NativeOSSandboxManager(SandboxManager):
    """
    Base class for sandboxes that rely on native OS capabilities
    like namespaces, cgroups, or job objects.
    Maintains a pool of pre-warmed processes to reduce boot latency.
    """

    # Class-level pool configuration: per-subclass dict keyed by os_type.
    # [Security/Robustness] This dict is shared across all subclasses.
    # Keys are differentiated by self._get_os_type() to ensure isolation.
    _pools: ClassVar[Dict[str, List[Tuple[mp.Process, mp.connection.Connection]]]] = {}
    _pools_lock = threading.Lock()

    def __init__(
        self,
        connection,
        config_path: Optional[str] = None,
        workspace_root: Optional[str] = None,
    ):
        super().__init__(connection, config_path)
        self.workspace_root = workspace_root or os.getcwd()

        # Dedicated executor for blocking queue operations
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._finalizer = weakref.finalize(self, self._cleanup_executor, self._executor)

        # Pool warm-up can block (Pipe.poll). Defer to first run() to avoid
        # stalling event loops when constructed inside async contexts.
        # Use thread-safe primitives so this remains safe even if a manager instance
        # is (incorrectly) shared across event loops/threads.
        self._pool_init_done = threading.Event()
        self._pool_init_lock = threading.Lock()

    @classmethod
    def _reset_pools(cls) -> None:
        """Test-only module cleanup to drain all pooled processes.
        Prevents test bleed since _pools is a mutable class variable.
        """
        with cls._pools_lock:
            for _os_type, pool in cls._pools.items():
                for process, conn in pool:
                    try:
                        process.terminate()
                        conn.close()
                    except Exception:
                        pass
            cls._pools.clear()

    def _init_pool(self):
        self._replenish_pool(self.config_path, self.workspace_root, self._get_os_type())

    async def _ensure_pool_initialized(self) -> None:
        if self._pool_init_done.is_set():
            return

        def _init_once() -> None:
            if self._pool_init_done.is_set():
                return
            with self._pool_init_lock:
                if self._pool_init_done.is_set():
                    return
                self._init_pool()
                self._pool_init_done.set()

        # Intentionally run pool init in a thread: readiness checks block on Pipe.poll().
        await asyncio.to_thread(_init_once)

    @staticmethod
    def _pool_key(os_type: str, config_path: str | None) -> str:
        # Pool keys must include config identity to prevent cross-instance contamination.
        # Do not include secrets here. A stable hash is sufficient.
        cfg = config_path or ""
        cfg_hash = hashlib.sha256(cfg.encode("utf-8")).hexdigest()[:16]
        return f"{os_type}::{cfg_hash}"

    @classmethod
    def _spawn_worker(
        cls,
        os_type: str,
        config_path: str | None,
        workspace_root: str | None,
    ) -> tuple[mp.Process, mp.connection.Connection]:
        """Spawns a new worker process and returns the process and parent connection."""
        parent_conn, child_conn = mp.Pipe()
        p = _MP_CTX.Process(
            target=_sandbox_worker,
            args=(os_type, child_conn, config_path, workspace_root),
        )
        p.start()
        # Close the child end in the parent process immediately to avoid FD leaks
        child_conn.close()
        return p, parent_conn

    @classmethod
    def _verify_worker_ready(
        cls,
        p: mp.Process,
        parent_conn: mp.connection.Connection,
        os_type: str,
    ) -> bool:
        """Verifies if a worker is ready to accept code."""
        try:
            config = SandboxConfig.from_env()
            if parent_conn.poll(config.READY_TIMEOUT):
                msg = parent_conn.recv()
                if isinstance(msg, dict) and msg.get("status") == "ready":
                    return True

            logger.error(
                f"Worker process ({os_type}) failed to initialize or signal ready"
            )
        except Exception as e:
            logger.error(f"Failed to verify worker readiness: {e}")
        return False

    @classmethod
    def _replenish_pool(
        cls,
        config_path: str | None = None,
        workspace_root: str | None = None,
        os_type: str = "none",
    ) -> None:
        """Ensures the pool has the target number of processes ready."""
        config = SandboxConfig.from_env()
        pool_key = cls._pool_key(os_type, config_path)
        try:
            pool_size = int(
                os.environ.get(
                    "STRAKE_SANDBOX_POOL_SIZE", str(config.DEFAULT_POOL_SIZE)
                )
            )
        except ValueError:
            pool_size = config.DEFAULT_POOL_SIZE

        with cls._pools_lock:
            if pool_key not in cls._pools:
                cls._pools[pool_key] = []

            # Filter out dead processes from the pool
            cls._pools[pool_key] = [
                (p, c) for (p, c) in cls._pools[pool_key] if p.is_alive()
            ]
            num_needed = pool_size - len(cls._pools[pool_key])

        if num_needed <= 0:
            return

        new_workers = []
        for _ in range(num_needed):
            p, parent_conn = cls._spawn_worker(os_type, config_path, workspace_root)
            if cls._verify_worker_ready(p, parent_conn, os_type):
                new_workers.append((p, parent_conn))
            else:
                # If a worker fails to become ready, ensure its resources are cleaned up
                try:
                    p.terminate()
                    p.join(timeout=1.0)
                    parent_conn.close()
                except Exception:
                    pass

        if new_workers:
            with cls._pools_lock:
                cls._pools[pool_key].extend(new_workers)

    @classmethod
    def _get_process_from_pool(
        cls,
        os_type: str,
        config_path: str | None,
    ) -> Optional[Tuple[mp.Process, mp.connection.Connection]]:
        pool_key = cls._pool_key(os_type, config_path)
        with cls._pools_lock:
            pool = cls._pools.get(pool_key)
            if not pool:
                return None

            # Quick check and pop
            while pool:
                p, conn = pool.pop()
                if p.is_alive():
                    return p, conn
                else:
                    # Clean up dead process's connection
                    try:
                        conn.close()
                    except Exception:
                        pass
            return None

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
        self,
        code: str,
        timeout_secs: Optional[float] = None,
        *,
        execution_context: Optional[dict[str, str]] = None,
    ) -> "SandboxResult":
        logger.info(f"Initializing {self._get_sandbox_name()}...")

        # 1. AST Validation (Defense in Depth)
        # This is a fast-path layer 1 guardrail.
        ast_error = validate_ast(code)
        if ast_error:

            logger.warning(
                "Sandbox security violation: %s (hash=%s)",
                ast_error,
                hashlib.sha256(code.encode()).hexdigest()[:16],
            )
            return SandboxResult(
                stdout="", stderr=f"Security Error: {ast_error}", result=None
            )

        await self._ensure_pool_initialized()
        os_type = self._get_os_type()
        p_conn_tuple = self._get_process_from_pool(os_type, self.config_path)
        if p_conn_tuple is None:
            # Fallback if pool is empty
            parent_conn, child_conn = mp.Pipe()
            p = _MP_CTX.Process(
                target=_sandbox_worker,
                args=(
                    os_type,
                    child_conn,
                    self.config_path,
                    self.workspace_root,
                ),
            )
            p.start()
            child_conn.close()  # FD leak fix

            loop = asyncio.get_running_loop()
            is_ready = False
            poll_result = await loop.run_in_executor(None, lambda: parent_conn.poll(10.0))
            if poll_result:
                msg = parent_conn.recv()
                if isinstance(msg, dict) and msg.get("status") == "ready":
                    is_ready = True
                else:
                    logger.error(f"Fallback process failed to init: {msg}")

            if not is_ready:
                p.terminate()
                parent_conn.close()
                return SandboxResult(
                    stdout="",
                    stderr="Execution Error: Failed to initialize sandbox worker (timeout or init error).",
                    result=None,
                )
            conn = parent_conn
        else:
            p, conn = p_conn_tuple

        timeout = self._get_timeout(timeout_secs)

        try:
            # Send the code
            payload: dict[str, Any] = {"code": code}
            if execution_context:
                payload["execution_context"] = execution_context
            conn.send(payload)

            def _get_result_from_pipe():
                try:
                    if conn.poll(timeout):
                        return conn.recv()
                    return None
                except EOFError:
                    return None

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self._executor, _get_result_from_pipe)

            if p.is_alive():
                # End of life for this process
                p.terminate()
                p.join(timeout=1.0)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=5.0)

            # Replenish pool in background thread
            def _background_replenish():
                self._replenish_pool(
                    self.config_path, self.workspace_root, self._get_os_type()
                )

            self._executor.submit(_background_replenish)

            if result is not None:
                if isinstance(result, dict):
                    return SandboxResult.from_dict(result)
                if isinstance(result, SandboxResult):
                    return result
                return SandboxResult(stdout="", stderr=str(result), result=None)

            # If process exited but pipe gave nothing
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
            logger.error(f"Error retrieving result from sandbox pipe: {e}")
            return SandboxResult(
                stdout="",
                stderr=SandboxErrorMessages.INTERNAL_FAILURE
                if "Internal" in str(e)
                else f"Execution Error: Failed to retrieve sandbox output: {str(e)}",
                result=None,
            )
        finally:
            try:
                conn.close()
            except Exception as e:
                logger.debug(f"Pipe close error (may be harmless): {e}")


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
    # Python 3.10+: asyncio.Lock() does not require a running loop at construction.
    # Initialize eagerly to avoid TOCTOU races under concurrent load.
    _rlimit_lock: ClassVar[asyncio.Lock] = asyncio.Lock()

    def _get_sandbox_name(self) -> str:
        return "macOS Sandbox (Seatbelt + SIP)"

    def _get_os_type(self) -> str:
        return "macos"

    async def run(
        self,
        code: str,
        timeout_secs: Optional[float] = None,
        *,
        execution_context: Optional[dict[str, str]] = None,
    ) -> "SandboxResult":
        """
        Execute code inside a macOS Seatbelt sandbox.
        """
        logger.info(f"Initializing {self._get_sandbox_name()}...")

        # 1. AST Validation (Defense in Depth)
        ast_error = validate_ast(code)
        if ast_error:

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
            return await super().run(
                code, timeout_secs, execution_context=execution_context
            )

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

        async with MacOSSandboxManager._rlimit_lock:
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
                body: dict[str, Any] = {"token": token, "code": code}
                if execution_context:
                    body["execution_context"] = execution_context
                payload = json.dumps(body).encode("utf-8")
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
                    stderr_bytes.decode("utf-8", errors="replace")
                    if stderr_bytes
                    else ""
                )
                return result
            except Exception:
                # Fallback to plain string decoding if JSON fails
                stdout = (
                    stdout_bytes.decode("utf-8", errors="replace")
                    if stdout_bytes
                    else ""
                )
                stderr = (
                    stderr_bytes.decode("utf-8", errors="replace")
                    if stderr_bytes
                    else ""
                )
                return SandboxResult(stdout=stdout, stderr=stderr, result=None)

        except FileNotFoundError:
            logger.error(
                f"sandbox-exec not found at {self._SEATBELT_EXECUTABLE}. "
                "Falling back to rlimit-only sandbox."
            )
            return await super().run(code, timeout_secs, execution_context=execution_context)
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
                logger.warning(
                    f"Failed to cleanup seatbelt profile {profile_path}: {e}"
                )


class WindowsSandboxManager(NativeOSSandboxManager):
    """
    Windows Native Sandbox using Job Objects for resource limits and
    AppContainer/Low Integrity Level for capability restriction.
    """

    def _init_pool(self):
        # Prevent spawning unused worker processes on Windows, since run() is not implemented.
        pass

    async def _ensure_pool_initialized(self) -> None:
        # Prevent spinning up any background threads for pool init on Windows.
        return

    async def run(
        self,
        code: str,
        timeout_secs: Optional[float] = None,
        *,
        execution_context: Optional[dict[str, str]] = None,
    ) -> "SandboxResult":
        raise NotImplementedError("WindowsSandboxManager is not yet implemented.")

    def _get_sandbox_name(self) -> str:
        return "Windows Sandbox (AppContainer + Job Objects)"

    def _get_os_type(self) -> str:
        return "windows"
