"""Firecracker sandbox manager orchestration module.

Provides the primary orchestrator that coordinates VM processes, UDS communication,
telemetry gathering, and guest VSOCK executions.
"""

# Standard Library
import asyncio
import logging
import os
import tempfile
import uuid
from typing import Any

# Local Application Imports
from strake.sandbox.base import (
    SandboxManager,
    SandboxResult,
    SandboxErrorMessages,
    ExecutionContext,
)
from strake.sandbox.native import SandboxConfig
from strake.sandbox.firecracker.client import UnixSocketHttpClient
from strake.sandbox.firecracker.telemetry import (
    SandboxMetering,
    FirecrackerTelemetryCollector,
)
from strake.sandbox.firecracker.lifecycle import MicroVMLifecycle
from strake.tracing import get_emitter

logger = logging.getLogger("strake.sandbox.firecracker.manager")

METERING_INTERVAL_ENV: str = "STRAKE_METERING_INTERVAL"
_DEFAULT_METERING_INTERVAL: float = 2.0


class FirecrackerSandboxManager(SandboxManager):
    """Executes Python code safely within an ephemeral Firecracker microVM.

    Phase 1 implementation for Code Mode. Must be utilized via the async context
    manager protocol (`async with`).
    """

    def __init__(
        self,
        connection: Any,
        config_path: str | None = None,
        fc_bin: str | None = None,
        kernel_path: str | None = None,
        rootfs_path: str | None = None,
        enable_background_tasks: bool = True,
    ) -> None:
        """Initialize the Firecracker sandbox manager.

        Args:
            connection: Core Strake DB/Federation connection instance.
            config_path: Optional filepath to custom sandbox configuration.
            fc_bin: Optional path to the firecracker binary (env override).
            kernel_path: Optional path to the uncompressed vmlinux kernel image.
            rootfs_path: Optional path to the ext4 root file system image.
            enable_background_tasks: If True, schedules template snapshot generation.

        Raises:
            RuntimeError: If Firecracker binary, kernel, or rootfs images are missing.
        """
        super().__init__(connection, config_path)
        self.fc_bin = fc_bin or os.environ.get("FIRECRACKER_BIN", "firecracker")
        self.kernel_path = kernel_path or os.environ.get(
            "FIRECRACKER_KERNEL", "/var/lib/firecracker/vmlinux"
        )
        self.rootfs_path = rootfs_path or os.environ.get(
            "FIRECRACKER_ROOTFS", "/var/lib/firecracker/rootfs.ext4"
        )

        # Parse config once and cache it
        self._config = SandboxConfig.from_env()

        # Cache HTTP clients and telemetry collectors to avoid object-per-request waste
        self._clients: dict[str, UnixSocketHttpClient] = {}
        self._collectors: dict[str, FirecrackerTelemetryCollector] = {}

        # Check prerequisites
        if not MicroVMLifecycle.is_available(
            self.fc_bin, self.kernel_path, self.rootfs_path
        ):
            logger.error(
                "Firecracker prerequisites not met. Refusing to fall back to INSECURE sandbox in production."
            )
            raise RuntimeError(
                "Firecracker sandbox is not available and insecure fallback is disabled. "
                "Check FIRECRACKER_BIN, KERNEL, and ROOTFS environment variables."
            )

        # Base snapshot configuration
        self._snapshot_dir: tempfile.TemporaryDirectory[str] | None = (
            tempfile.TemporaryDirectory(prefix="strake-vm-")
        )
        self.snapshot_path = os.path.join(self._snapshot_dir.name, "snapshot.ext4")
        self.mem_path = os.path.join(self._snapshot_dir.name, "mem.ext4")

        # Snapshot task variables: deferred to lazy initialization in run()
        self._snapshot_task: asyncio.Task[None] | None = None
        self._snapshot_created = False
        self._snapshot_lock = asyncio.Lock()

    async def __aenter__(self) -> "FirecrackerSandboxManager":
        """Async context manager entry to setup the sandbox environment.

        Returns:
            The FirecrackerSandboxManager instance.
        """
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit guaranteeing resource and snapshot cleanup.

        Args:
            exc_type: Exception class.
            exc_val: Exception instance.
            exc_tb: Exception traceback.
        """
        await self.shutdown()

    def __del__(self) -> None:
        """Synchronous garbage collection fallback cleanup for filesystem temporary directory."""
        snapshot_dir = getattr(self, "_snapshot_dir", None)
        if snapshot_dir:
            try:
                snapshot_dir.cleanup()
            except OSError:
                pass

    async def shutdown(self) -> None:
        """Clean up active VM resources, snapshots, and background tasks.

        Cancels any scheduled background template creation tasks and cleans up
        all temporary filesystem snapshot storage directories.
        """
        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
            except (OSError, RuntimeError) as e:
                logger.debug(f"Error cancelling snapshot task: {e}")
            self._snapshot_task = None

        if self._snapshot_dir:
            try:
                self._snapshot_dir.cleanup()
            except (OSError, PermissionError):
                pass
            self._snapshot_dir = None

    @property
    def lifecycle(self) -> MicroVMLifecycle:
        """Get or lazily initialize the MicroVMLifecycle helper."""
        if not hasattr(self, "_lifecycle_inst") or self._lifecycle_inst is None:
            self._lifecycle_inst = MicroVMLifecycle(
                self.fc_bin,
                self.kernel_path,
                self.rootfs_path,
                self._config,
                api_request_fn=lambda *args, **kwargs: self._api_request(
                    *args, **kwargs
                ),
            )
        return self._lifecycle_inst

    @property
    def _lifecycle(self) -> MicroVMLifecycle:
        """Private backward-compatible alias for the public lifecycle property."""
        return self.lifecycle

    @staticmethod
    def is_firecracker_available(
        fc_bin: str, kernel_path: str, rootfs_path: str
    ) -> bool:
        """Perform host check to determine if Firecracker is runnable.

        Args:
            fc_bin: Target path to the Firecracker binary.
            kernel_path: Target path to the guest OS kernel.
            rootfs_path: Target path to the ext4 rootfs disk.

        Returns:
            True if all prerequisites are satisfied; False otherwise.
        """
        return MicroVMLifecycle.is_available(fc_bin, kernel_path, rootfs_path)

    # =========================================================================
    # BACKWARD-COMPATIBLE DELEGATION METHODS FOR TESTING & INHERITANCE
    # =========================================================================

    async def _api_request(
        self,
        socket_path: str,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: float = 5.0,
    ) -> str:
        """Perform an HTTP request over a Unix Domain Socket and return the response body."""
        if not hasattr(self, "_clients") or self._clients is None:
            self._clients = {}
        if socket_path not in self._clients:
            self._clients[socket_path] = UnixSocketHttpClient(socket_path)
        client = self._clients[socket_path]
        return await client.request(method, path, body, timeout=timeout)

    async def _uds_http_get(self, socket_path: str, path: str, timeout: float) -> str:
        """Execute a lightweight manual HTTP GET request over a Unix Domain Socket."""
        if not hasattr(self, "_clients") or self._clients is None:
            self._clients = {}
        if socket_path not in self._clients:
            self._clients[socket_path] = UnixSocketHttpClient(socket_path)
        client = self._clients[socket_path]
        return await client.get(path, timeout=timeout)

    def _parse_metrics(self, text: str) -> dict[str, float]:
        """Parse Prometheus or JSON metrics output from Firecracker."""
        return FirecrackerTelemetryCollector.parse_metrics(text)

    async def _fetch_cpu_metrics(self, metrics_path: str, api_timeout: float) -> float:
        """Fetch total CPU time (user + system) from the metrics Unix socket."""
        if not hasattr(self, "_collectors") or self._collectors is None:
            self._collectors = {}
        if metrics_path not in self._collectors:
            self._collectors[metrics_path] = FirecrackerTelemetryCollector(
                metrics_path, api_timeout
            )
        collector = self._collectors[metrics_path]
        return await collector.fetch_cpu_seconds()

    async def _fetch_memory_mb(self, metrics_path: str, api_timeout: float) -> float:
        """Fetch current memory usage in MiB from the metrics Unix socket."""
        if not hasattr(self, "_collectors") or self._collectors is None:
            self._collectors = {}
        if metrics_path not in self._collectors:
            self._collectors[metrics_path] = FirecrackerTelemetryCollector(
                metrics_path, api_timeout
            )
        collector = self._collectors[metrics_path]
        return await collector.fetch_memory_mb()

    async def _sample_memory(
        self,
        metering: SandboxMetering,
        metrics_path: str,
        api_timeout: float,
        interval: float = 2.0,
    ) -> None:
        """Background loop to periodically sample memory and track peak RSS."""
        if not hasattr(self, "_collectors") or self._collectors is None:
            self._collectors = {}
        if metrics_path not in self._collectors:
            self._collectors[metrics_path] = FirecrackerTelemetryCollector(
                metrics_path, api_timeout
            )
        collector = self._collectors[metrics_path]
        await collector.sample_memory_loop(metering, interval)

    async def _ensure_snapshot(self) -> None:
        """Thread-safe serialized wrapper ensuring snapshot initialization runs once."""
        async with self._snapshot_lock:
            if self._snapshot_created:
                return
            await self._create_snapshot()

    async def _create_snapshot(self) -> None:
        """Generate a guest template snapshot."""
        if self._snapshot_created:
            return
        await self._lifecycle.create_snapshot(self.snapshot_path, self.mem_path)
        self._snapshot_created = True

    async def _configure_microvm(
        self, socket_path: str, *, api_timeout: float = 5.0
    ) -> None:
        """Configure the baseline hardware environment for a fresh microVM instance."""
        await self._lifecycle.configure_microvm(socket_path, api_timeout=api_timeout)

    async def _wait_for_metrics_socket(
        self,
        metrics_path: str,
        process: asyncio.subprocess.Process,
        config: SandboxConfig,
        api_timeout: float,
    ) -> None:
        """Active connection-probing loop to ensure metrics server is ready."""
        await self._lifecycle.wait_for_metrics_socket(
            metrics_path, process, api_timeout
        )

    async def _boot_vm(
        self,
        socket_path: str,
        vsock_path: str,
        api_timeout: float,
    ) -> None:
        """Restore the microVM state from snapshot or fallback to cold boot."""
        await self._lifecycle.boot_vm(
            socket_path,
            vsock_path,
            self.snapshot_path,
            self.mem_path,
            self._snapshot_created,
            api_timeout,
        )

    async def _set_vsock(
        self, socket_path: str, vsock_path: str, *, api_timeout: float
    ) -> None:
        """Register the host-side UDS VSOCK path for guest agent communications."""
        await self._lifecycle._set_vsock(
            socket_path, vsock_path, api_timeout=api_timeout
        )

    async def _deliver_code(
        self,
        writer: asyncio.StreamWriter,
        code: str,
        execution_context: ExecutionContext | None,
    ) -> None:
        """Deliver the Python source code and execution context to the guest over VSOCK."""
        await self._lifecycle.deliver_code(writer, code, execution_context)

    async def _collect_result(
        self,
        reader: asyncio.StreamReader,
        config: SandboxConfig,
        timeout: float,
    ) -> SandboxResult:
        """Read and decode the execution results from the guest agent via VSOCK."""
        return await self._lifecycle.collect_result(reader, timeout)

    # =========================================================================
    # CORE RUNNER AND COORDINATION
    # =========================================================================

    async def _execute_in_vm(
        self,
        tmpdir: str,
        code: str,
        timeout: float,
        api_timeout: float,
        execution_context: ExecutionContext | None,
    ) -> SandboxResult:
        """Spawn the microVM, execute guest VSOCK communication, and sample resource usage.

        Args:
            tmpdir: Ephemeral workspace root directory path.
            code: Python source code string.
            timeout: Maximum execution timeout in seconds.
            api_timeout: HTTP request timeout duration.
            execution_context: Optional typed execution metadata context.

        Returns:
            A SandboxResult containing guest stdout, stderr, and result.

        Raises:
            RuntimeError: If process initialization or UDS wait loops fail.
            ConnectionError: If stream communication or connection drops.
            asyncio.TimeoutError: If execution exceeds timeout bounds.
        """
        socket_path = os.path.join(tmpdir, "api.socket")
        vsock_path = os.path.join(tmpdir, "vsock.socket")
        metrics_path = os.path.join(tmpdir, "metrics.socket")

        # 1. Start Firecracker process
        process = await asyncio.create_subprocess_exec(
            self.fc_bin,
            "--api-sock",
            socket_path,
            "--metrics-path",
            metrics_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # 2. Wait for main API socket
        retries = self._config.fc_socket_retries
        while not os.path.exists(socket_path):
            if process.returncode is not None:
                err_b = b""
                if process.stderr is not None:
                    try:
                        err_b = await process.stderr.read()
                    except (OSError, ValueError):
                        err_b = b""
                err = err_b.decode("utf-8", errors="replace")
                raise RuntimeError(SandboxErrorMessages.FC_START_FAILED.format(err))
            await asyncio.sleep(self._config.fc_retry_delay_secs)
            retries -= 1
            if retries <= 0:
                try:
                    process.kill()
                except OSError:
                    pass
                raise RuntimeError(SandboxErrorMessages.FC_SOCKET_TIMEOUT)

        # 3. Wait for metrics socket to be fully ready and accepting connections
        await self._wait_for_metrics_socket(
            metrics_path, process, self._config, api_timeout
        )

        writer = None
        try:
            # 4. Boot VM
            await self._boot_vm(socket_path, vsock_path, api_timeout)

            # 5. Connect to VSOCK
            reader: asyncio.StreamReader | None = None
            writer: asyncio.StreamWriter | None = None
            retries = self._config.fc_socket_retries
            while True:
                try:
                    reader, writer = await asyncio.open_unix_connection(vsock_path)
                    break
                except (FileNotFoundError, ConnectionRefusedError) as e:
                    if process.returncode is not None:
                        raise RuntimeError(SandboxErrorMessages.GUEST_DIED) from e
                    await asyncio.sleep(0.1)
                    retries -= 1
                    if retries <= 0:
                        raise RuntimeError(SandboxErrorMessages.GUEST_TIMEOUT) from e

            # 6. Initialize Resource Metering
            session_id = (
                execution_context.session_id
                if execution_context and execution_context.session_id
                else uuid.uuid4().hex
            )
            metering = SandboxMetering(session_id=session_id)
            try:
                metering.start_cpu = await self._fetch_cpu_metrics(
                    metrics_path, api_timeout
                )
            except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
                logger.warning(f"Failed to fetch initial CPU metrics: {e}")
            metering.start_wall = asyncio.get_running_loop().time()

            # Start background memory polling
            sampling_interval = float(
                os.environ.get(METERING_INTERVAL_ENV, str(_DEFAULT_METERING_INTERVAL))
            )
            metering.sampling_task = asyncio.create_task(
                self._sample_memory(
                    metering, metrics_path, api_timeout, interval=sampling_interval
                )
            )

            try:
                # 7. Deliver Code
                await self._deliver_code(writer, code, execution_context)

                # 8. Collect Result
                return await self._collect_result(reader, self._config, timeout)
            finally:
                # Cancel peak memory sampling
                if metering.sampling_task:
                    metering.sampling_task.cancel()
                    try:
                        await metering.sampling_task
                    except asyncio.CancelledError:
                        pass
                    except (
                        ConnectionError,
                        OSError,
                        asyncio.TimeoutError,
                        RuntimeError,
                    ):
                        pass

                # Fetch final metrics and calculate delta
                try:
                    final_cpu = await self._fetch_cpu_metrics(metrics_path, api_timeout)
                    cpu_delta = final_cpu - metering.start_cpu
                except (
                    ConnectionError,
                    OSError,
                    asyncio.TimeoutError,
                    RuntimeError,
                ) as e:
                    logger.warning(f"Failed to fetch final CPU metrics: {e}")
                    cpu_delta = 0.0

                wall_delta = asyncio.get_running_loop().time() - metering.start_wall

                resource_usage = {
                    "session_id": metering.session_id,
                    "cpu_seconds": round(max(0.0, cpu_delta), 3),
                    "max_memory_mb": round(metering.max_memory_mb, 2),
                    "wall_duration_seconds": round(max(0.0, wall_delta), 3),
                }

                # Emit sandbox_metering event
                try:
                    get_emitter().emit(
                        {
                            "event": "sandbox_metering",
                            "session_id": metering.session_id,
                            **resource_usage,
                        }
                    )
                except (AttributeError, RuntimeError) as e:
                    logger.warning(f"Failed to emit metering span: {e}")

                logger.info(
                    "sandbox_metering", extra={"resource_usage": resource_usage}
                )

        finally:
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except (OSError, ConnectionError):
                    pass
            # Teardown microVM process
            if process.returncode is None:
                try:
                    process.terminate()
                    await asyncio.sleep(0.1)
                    if process.returncode is None:
                        process.kill()
                    await asyncio.wait_for(process.wait(), timeout=5)
                except (ProcessLookupError, asyncio.TimeoutError, OSError):
                    pass

    async def run(
        self,
        code: str,
        timeout_secs: float | None = None,
        *,
        execution_context: ExecutionContext | None = None,
    ) -> SandboxResult:
        """Execute Python code inside a Firecracker microVM and return stdout/stderr.

        This method coordinates template snapshot loading, environment validation,
        temporary folder lifecycle, and executes code via the virtual machine runner.
        Unexpected catastrophic failures bubble up, while expected networking/timeout
        errors are safely caught and returned as standard SandboxResult.

        Args:
            code: Python source code to execute inside the microVM.
            timeout_secs: Maximum execution timeout in seconds. Defaults to SandboxConfig.timeout_secs.
            execution_context: Optional ExecutionContext dataclass for tracking metadata.

        Returns:
            A SandboxResult object containing stdout, stderr, and result structure.

        Raises:
            RuntimeError: If prerequisites are missing or VM failed to start.
        """
        # 1. Lazy-initialize snapshot template if not already created (thread-safe lock)
        if not self._snapshot_created:
            await self._ensure_snapshot()

        logger.info("Preparing Firecracker microVM execution environment...")
        timeout = timeout_secs or self._config.timeout_secs
        api_timeout = min(timeout, 5.0)

        with tempfile.TemporaryDirectory(prefix="fc-") as tmpdir:
            try:
                # 2. Delegate execution logic to SOLID VM runner helper
                return await self._execute_in_vm(
                    tmpdir, code, timeout, api_timeout, execution_context
                )
            except (asyncio.TimeoutError, ConnectionError, OSError, RuntimeError) as e:
                # Only catch expected infrastructure/startup errors
                return SandboxResult(
                    stdout="",
                    stderr=f"Execution Error: {str(e)}",
                    result=None,
                )
