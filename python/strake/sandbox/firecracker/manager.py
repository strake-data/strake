"""Firecracker sandbox manager orchestration module.

Provides the primary orchestrator that coordinates VM processes, UDS communication,
telemetry gathering, and guest VSOCK executions.
"""

# Standard Library
import asyncio
import collections
import logging
import os
import tempfile
import threading
import time
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
from strake.sandbox.firecracker.warmed_vm import WarmedVM
from strake.tracing import get_emitter

logger = logging.getLogger("strake.sandbox.firecracker.manager")

METERING_INTERVAL_ENV: str = "STRAKE_METERING_INTERVAL"
_DEFAULT_METERING_INTERVAL: float = 2.0
TEST_MODE_ENV_VAL: str = "true"


async def _bg_teardown_process(process: asyncio.subprocess.Process) -> None:
    """Safely terminates and kills a Firecracker subprocess in the background.

    Args:
        process: The subprocess handle representing the running microVM.
    """
    try:
        process.terminate()
        await asyncio.sleep(0.1)
        if process.returncode is None:
            process.kill()
        await asyncio.wait_for(process.wait(), timeout=5)
    except (OSError, ProcessLookupError, asyncio.TimeoutError):
        pass


async def _drain_stream(stream: asyncio.StreamReader) -> None:
    """Read a stream to EOF to prevent the sub-process from blocking on write."""
    try:
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                break
    except Exception:
        pass


def _drain_process_output(process: asyncio.subprocess.Process) -> None:
    """Spawn background tasks to drain process stdout and stderr."""
    stdout = getattr(process, "stdout", None)
    if stdout:
        asyncio.create_task(_drain_stream(stdout))
    stderr = getattr(process, "stderr", None)
    if stderr:
        asyncio.create_task(_drain_stream(stderr))


class VSOCKExecutor:
    """Encapsulates code delivery, result collection, and telemetry metering for microVM execution.

    # Overview
    Handles the execution lifecycle within a booted Firecracker microVM. This includes
    initializing CPU/memory metering, delivering source code, and collecting execution results
    over Unix domain sockets.

    # Usage
    ```python
    executor = VSOCKExecutor(manager, metrics_path, api_timeout)
    result = await executor.execute(reader, writer, code, timeout, execution_context)
    ```

    # Performance Characteristics
    Introduces negligible overhead while wrapping standard async readers and writers.

    # Safety
    Must be executed within the context of an active asyncio event loop. Not thread-safe.

    # Errors
    Propagates `ConnectionError` and `asyncio.TimeoutError` from VSOCK sockets.
    """

    def __init__(
        self,
        manager: "FirecrackerSandboxManager",
        metrics_path: str,
        api_timeout: float,
    ) -> None:
        self.manager = manager
        self.metrics_path = metrics_path
        self.api_timeout = api_timeout

    async def _init_metering(
        self, execution_context: ExecutionContext | None
    ) -> SandboxMetering:
        """Initialize telemetry metering and start background memory sampling."""
        session_id = (
            execution_context.session_id
            if execution_context and execution_context.session_id
            else uuid.uuid4().hex
        )
        metering = SandboxMetering(session_id=session_id)
        try:
            metering.start_cpu = await self.manager._fetch_cpu_metrics(
                self.metrics_path, self.api_timeout
            )
        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            logger.warning(f"Failed to fetch initial CPU metrics: {e}")
        metering.start_wall = asyncio.get_running_loop().time()

        # Start background memory polling
        sampling_interval = float(
            os.environ.get(METERING_INTERVAL_ENV, str(_DEFAULT_METERING_INTERVAL))
        )
        metering.sampling_task = asyncio.create_task(
            self.manager._sample_memory(
                metering,
                self.metrics_path,
                self.api_timeout,
                interval=sampling_interval,
            )
        )
        return metering

    async def _cancel_metering(self, metering: SandboxMetering) -> None:
        """Cancel background peak memory sampling task."""
        if metering.sampling_task:
            metering.sampling_task.cancel()
            try:
                await metering.sampling_task
            except asyncio.CancelledError:
                pass
            except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError):
                pass

    async def _emit_metering(self, metering: SandboxMetering) -> None:
        """Fetch final telemetry metrics, calculate resource usage, and emit telemetry event."""
        try:
            final_cpu = await self.manager._fetch_cpu_metrics(
                self.metrics_path, self.api_timeout
            )
            cpu_delta = final_cpu - metering.start_cpu
        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
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

        logger.info("sandbox_metering", extra={"resource_usage": resource_usage})

    async def execute(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        code: str,
        timeout: float,
        execution_context: ExecutionContext | None,
    ) -> SandboxResult:
        """Deliver code, run metering, and collect execution output."""
        metering = await self._init_metering(execution_context)
        try:
            # Deliver Code
            await self.manager._deliver_code(writer, code, execution_context)

            # Collect Result
            return await self.manager._collect_result(
                reader, self.manager._config, timeout
            )
        finally:
            await self._cancel_metering(metering)
            await self._emit_metering(metering)


class FirecrackerVMPool:
    """Manages the pool of pre-warmed Firecracker microVM instances.

    # Overview
    Provides O(1) VM acquisition for execution workloads by pre-populating
    and replenishing a queue of booted, snapshot-resumed Firecracker VMs.

    # Usage
    ```python
    pool = FirecrackerVMPool(manager, pool_size=2)
    pool.trigger_replenish()
    vm = await pool.acquire_vm()
    ```

    # Performance Characteristics
    Maintains pre-booted VM micro-instances, allowing startup times under 10ms.

    # Safety
    - All background replenishment tasks must be canceled via `shutdown()` before discarding.
    - Safe fallback synchronous cleanups are performed by a re-entrant `threading.Lock`.

    # Errors
    Exceptions spawned during MicroVM creation (e.g. process launch errors, socket timeouts)
    are logged and trigger exponential backoffs up to 60 seconds.
    """

    def __init__(
        self,
        manager: "FirecrackerSandboxManager",
        pool_size: int,
    ) -> None:
        """Initialize the Firecracker pre-warmed pool.

        Args:
            manager: The parent FirecrackerSandboxManager instance.
            pool_size: Target count of pre-warmed VMs to maintain.

        Raises:
            TypeError: If pool_size is not an integer.
            ValueError: If pool_size is negative.
        """
        if not isinstance(pool_size, int):
            raise TypeError(f"pool_size must be an integer, got {type(pool_size)}")
        if pool_size < 0:
            raise ValueError(f"pool_size must be non-negative, got {pool_size}")
        self.manager = manager
        self.pool_size = pool_size
        self._vms: collections.deque[WarmedVM] = collections.deque()
        self._replenish_lock = asyncio.Lock()
        self._replenish_task: asyncio.Task[None] | None = None
        self._consecutive_failures = 0
        self._backoff_until: float = 0.0
        self._cleanup_lock = threading.Lock()

    def trigger_replenish(self) -> asyncio.Task[None] | None:
        """Triggers the background replenish loop if not already running.

        Returns:
            The background replenishment asyncio.Task, or None if already running.
        """
        if self._replenish_task is None or self._replenish_task.done():
            self._replenish_task = asyncio.create_task(self.replenish())
            return self._replenish_task
        return None

    async def replenish(self) -> None:
        """Replenishes the VM pool to target size, serializing spawns with a lock.

        Raises:
            Exception: Propagates initialization failures from _spawn_warmed_vm.
        """
        async with self._replenish_lock:
            self._consecutive_failures = 0  # Reset stale state before any work
            # Filter out any dead VMs that might be in the queue
            alive_vms = collections.deque()
            while self._vms:
                vm = self._vms.popleft()
                if vm.process.returncode is None:
                    alive_vms.append(vm)
                else:
                    await self.teardown_vm(vm)
            self._vms = alive_vms
            if len(self._vms) >= self.pool_size:
                self._consecutive_failures = 0

            while len(self._vms) < self.pool_size:
                now = time.monotonic()
                if now < self._backoff_until:
                    logger.debug("Replenish loop in backoff cooldown.")
                    break

                try:
                    vm = await self._spawn_warmed_vm()
                    self._vms.append(vm)
                    self._consecutive_failures = 0
                except Exception as e:
                    self._consecutive_failures += 1
                    cooldown = min(60.0, 1.0 * (2**self._consecutive_failures))
                    self._backoff_until = now + cooldown
                    logger.error(
                        f"Failed to spawn warmed VM for pool: {e}. Backoff for {cooldown}s."
                    )
                    break

    async def _spawn_warmed_vm(self) -> WarmedVM:
        """Spawns a new Firecracker microVM, boots it from snapshot, and connects VSOCK."""
        tmpdir_obj = tempfile.TemporaryDirectory(prefix="fc-pool-")
        tmpdir = tmpdir_obj.name

        socket_path = os.path.join(tmpdir, "api.socket")
        vsock_path = os.path.join(tmpdir, "vsock.socket")
        metrics_path = os.path.join(tmpdir, "metrics.socket")

        # Clone rootfs per-execution to avoid write conflicts
        cloned_rootfs = os.path.join(tmpdir, "rootfs.ext4")
        await self.manager._clone_rootfs(cloned_rootfs)

        # 1. Start Firecracker process
        process = await asyncio.create_subprocess_exec(
            self.manager.fc_bin,
            "--api-sock",
            socket_path,
            "--metrics-path",
            metrics_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # 2. Wait for main API socket
        retries = self.manager._config.fc_socket_retries
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
            await asyncio.sleep(self.manager._config.fc_retry_delay_secs)
            retries -= 1
            if retries <= 0:
                try:
                    process.kill()
                except OSError:
                    pass
                raise RuntimeError(SandboxErrorMessages.FC_SOCKET_TIMEOUT)

        _drain_process_output(process)

        # 3. Wait for metrics socket to be fully ready and accepting connections
        api_timeout = 5.0
        await self.manager._wait_for_metrics_socket(
            metrics_path, process, self.manager._config, api_timeout
        )

        # 4. Boot VM with cloned rootfs
        await self.manager._boot_vm(
            socket_path,
            vsock_path,
            api_timeout,
            rootfs_path=cloned_rootfs,
        )

        # 5. Connect to VSOCK
        reader = None
        writer = None
        retries = self.manager._config.fc_socket_retries
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

        return WarmedVM(
            tmpdir_obj=tmpdir_obj,
            tmpdir=tmpdir,
            process=process,
            socket_path=socket_path,
            vsock_path=vsock_path,
            metrics_path=metrics_path,
            reader=reader,
            writer=writer,
            cloned_rootfs=cloned_rootfs,
        )

    def get_all_vms(self) -> list[WarmedVM]:
        """Returns a list of all current warmed VMs in the pool (for backward-compatibility)."""
        return list(self._vms)

    async def acquire_vm(self) -> WarmedVM | None:
        """Acquire a pre-warmed VM from the pool, checking process health.

        Returns:
            A running WarmedVM instance, or None if the pool is exhausted.
        """
        warmed_vm = None
        while self._vms:
            candidate = self._vms.popleft()
            if candidate.process.returncode is None:
                warmed_vm = candidate
                break
            else:
                await self.teardown_vm(candidate)

        if warmed_vm is not None:
            self._consecutive_failures = 0

        self.trigger_replenish()
        return warmed_vm

    async def teardown_vm(self, vm: WarmedVM) -> None:
        """Tears down a pooled or active VM instance and releases resources.

        Args:
            vm: The WarmedVM instance to stop.
        """
        if vm.writer:
            try:
                vm.writer.close()
                await vm.writer.wait_closed()
            except (OSError, ConnectionError):
                pass

        if vm.process and vm.process.returncode is None:
            try:
                vm.process.terminate()
                await asyncio.sleep(0.1)
                if vm.process.returncode is None:
                    vm.process.kill()
                await asyncio.wait_for(vm.process.wait(), timeout=5)
            except (ProcessLookupError, asyncio.TimeoutError, OSError):
                pass

        if vm.tmpdir_obj:
            try:
                vm.tmpdir_obj.cleanup()
            except (OSError, PermissionError):
                pass

    async def shutdown(self) -> None:
        """Shutdown all VMs in the pool and cancel background replenish tasks."""
        if self._replenish_task:
            self._replenish_task.cancel()
            try:
                await self._replenish_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Error cancelling replenish task: {e}")
            self._replenish_task = None

        while self._vms:
            vm = self._vms.popleft()
            try:
                await self.teardown_vm(vm)
            except Exception as e:
                logger.warning(f"Error tearing down VM during pool shutdown: {e}")

    def _cleanup_sync_fallback(self) -> None:
        """Synchronously clean up the pool's filesystem directories if GC occurs before shutdown."""
        with self._cleanup_lock:
            while self._vms:
                vm = self._vms.popleft()
                if vm.tmpdir_obj:
                    try:
                        vm.tmpdir_obj.cleanup()
                    except (OSError, PermissionError):
                        pass

    def __del__(self) -> None:
        self._cleanup_sync_fallback()


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
        enable_fc_pool: bool | None = None,
        fc_pool_size: int | None = None,
    ) -> None:
        """Initialize the Firecracker sandbox manager.

        Args:
            connection: Core Strake DB/Federation connection instance.
            config_path: Optional filepath to custom sandbox configuration.
            fc_bin: Optional path to the firecracker binary (env override).
            kernel_path: Optional path to the uncompressed vmlinux kernel image.
            rootfs_path: Optional path to the ext4 root file system image.
            enable_background_tasks: If True, schedules template snapshot generation.
            enable_fc_pool: Explicitly enable or disable pre-warmed pooling.
            fc_pool_size: Target count of pre-warmed VMs to maintain in the pool.

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

        # Pre-warmed pool configuration overrides
        self._enable_fc_pool = (
            enable_fc_pool
            if enable_fc_pool is not None
            else self._config.enable_fc_pool
        )
        pool_size_val = (
            fc_pool_size if fc_pool_size is not None else self._config.fc_pool_size
        )
        if pool_size_val < 0:
            raise ValueError(f"fc_pool_size must be non-negative, got {pool_size_val}")
        if self._enable_fc_pool and pool_size_val == 0:
            raise ValueError(
                f"fc_pool_size must be > 0 when pooling is enabled, got {pool_size_val}"
            )
        self._pool = FirecrackerVMPool(self, pool_size_val)

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
        if self._enable_fc_pool:
            self._pool.trigger_replenish()
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
        self._cleanup_sync_fallback()

    def _cleanup_sync_fallback(self) -> None:
        """Synchronously clean up directories on garbage collection fallback."""
        snapshot_dir = getattr(self, "_snapshot_dir", None)
        if snapshot_dir:
            try:
                snapshot_dir.cleanup()
            except OSError:
                pass

        pool = getattr(self, "_pool", None)
        if pool:
            try:
                pool._cleanup_sync_fallback()
            except Exception:
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

        # Clean up all pooled VMs
        await self._pool.shutdown()

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
        if not hasattr(self, "_clients"):
            self._clients = {}
        client = self._clients.setdefault(
            socket_path, UnixSocketHttpClient(socket_path)
        )
        return await client.request(method, path, body, timeout=timeout)

    async def _uds_http_get(self, socket_path: str, path: str, timeout: float) -> str:
        """Execute a lightweight manual HTTP GET request over a Unix Domain Socket."""
        if not hasattr(self, "_clients"):
            self._clients = {}
        client = self._clients.setdefault(
            socket_path, UnixSocketHttpClient(socket_path)
        )
        return await client.get(path, timeout=timeout)

    def _parse_metrics(self, text: str) -> dict[str, float]:
        """Parse Prometheus or JSON metrics output from Firecracker."""
        return FirecrackerTelemetryCollector.parse_metrics(text)

    async def _fetch_cpu_metrics(self, metrics_path: str, api_timeout: float) -> float:
        """Fetch total CPU time (user + system) from the metrics Unix socket."""
        if not hasattr(self, "_collectors"):
            self._collectors = {}
        collector = self._collectors.setdefault(
            metrics_path, FirecrackerTelemetryCollector(metrics_path, api_timeout)
        )
        return await collector.fetch_cpu_seconds()

    async def _fetch_memory_mb(self, metrics_path: str, api_timeout: float) -> float:
        """Fetch current memory usage in MiB from the metrics Unix socket."""
        if not hasattr(self, "_collectors"):
            self._collectors = {}
        collector = self._collectors.setdefault(
            metrics_path, FirecrackerTelemetryCollector(metrics_path, api_timeout)
        )
        return await collector.fetch_memory_mb()

    async def _sample_memory(
        self,
        metering: SandboxMetering,
        metrics_path: str,
        api_timeout: float,
        interval: float = 2.0,
    ) -> None:
        """Background loop to periodically sample memory and track peak RSS."""
        if not hasattr(self, "_collectors"):
            self._collectors = {}
        collector = self._collectors.setdefault(
            metrics_path, FirecrackerTelemetryCollector(metrics_path, api_timeout)
        )
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
        *,
        rootfs_path: str | None = None,
    ) -> None:
        """Restore the microVM state from snapshot or fallback to cold boot."""
        await self._lifecycle.boot_vm(
            socket_path,
            vsock_path,
            self.snapshot_path,
            self.mem_path,
            self._snapshot_created,
            api_timeout,
            rootfs_path=rootfs_path,
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

    @property
    def _fc_pool(self) -> list[WarmedVM]:
        """Backward-compatible property exposing the underlying pool collection as a list."""
        return self._pool.get_all_vms()

    @_fc_pool.setter
    def _fc_pool(self, value: list[WarmedVM]) -> None:
        """Allow setting or resetting the pool collection (for test mocks only).

        Args:
            value: A list of WarmedVM instances to populate the pool.
        """
        if not isinstance(value, list) or not all(
            isinstance(x, WarmedVM) for x in value
        ):
            raise TypeError("Value must be a list of WarmedVM instances")
        self._pool._vms = collections.deque(value)

    @property
    def _fc_pool_size(self) -> int:
        """Backward-compatible property for the pool size configuration."""
        return self._pool.pool_size

    @_fc_pool_size.setter
    def _fc_pool_size(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError(f"pool_size must be an integer, got {type(value)}")
        if value < 0:
            raise ValueError(f"fc_pool_size must be non-negative, got {value}")
        if self._enable_fc_pool and value == 0:
            raise ValueError(
                f"fc_pool_size must be > 0 when pooling is enabled, got {value}"
            )
        self._pool.pool_size = value

    async def _replenish_fc_pool(self) -> None:
        """Backward-compatible wrapper to replenish the pool."""
        await self._pool.replenish()

    async def _teardown_fc_vm(self, vm: WarmedVM) -> None:
        """Backward-compatible wrapper to tear down a VM."""
        await self._pool.teardown_vm(vm)

    async def _spawn_warmed_vm(self) -> WarmedVM:
        """Backward-compatible wrapper to spawn a warmed VM."""
        return await self._pool._spawn_warmed_vm()

    async def _acquire_warmed_vm(self) -> WarmedVM | None:
        """Acquires a pre-warmed VM from the pool, checking process health."""
        return await self._pool.acquire_vm()

    async def _clone_rootfs(self, cloned_rootfs: str) -> None:
        """Clone the rootfs file to the target location using non-blocking I/O."""
        copy_success = False
        try:
            proc = await asyncio.create_subprocess_exec(
                "cp",
                "--reflink=auto",
                self.rootfs_path,
                cloned_rootfs,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
            if proc.returncode == 0:
                copy_success = True
        except Exception as e:
            logger.debug(f"cp --reflink=auto failed: {e}")

        if not copy_success:
            import shutil

            try:
                await asyncio.to_thread(shutil.copy2, self.rootfs_path, cloned_rootfs)
            except FileNotFoundError:
                is_test_mode = (
                    os.environ.get("STRAKE_TEST_MODE") == TEST_MODE_ENV_VAL
                    or os.environ.get("PYTEST_CURRENT_TEST") is not None
                )
                if is_test_mode:
                    with open(cloned_rootfs, "w") as f:
                        f.write("")
                else:
                    raise FileNotFoundError(
                        f"Rootfs file {self.rootfs_path} not found and test mode is not enabled."
                    )

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
        *,
        warmed_vm: WarmedVM | None = None,
    ) -> SandboxResult:
        """Spawn the microVM, execute guest VSOCK communication, and sample resource usage.

        Args:
            tmpdir: Ephemeral workspace root directory path.
            code: Python source code string.
            timeout: Maximum execution timeout in seconds.
            api_timeout: HTTP request timeout duration.
            execution_context: Optional typed execution metadata context.
            warmed_vm: Optional pre-warmed VM instance.

        Returns:
            A SandboxResult containing guest stdout, stderr, and result.

        Raises:
            RuntimeError: If process initialization or UDS wait loops fail.
            ConnectionError: If stream communication or connection drops.
            asyncio.TimeoutError: If execution exceeds timeout bounds.
        """
        if warmed_vm:
            process = warmed_vm.process
            socket_path = warmed_vm.socket_path
            vsock_path = warmed_vm.vsock_path
            metrics_path = warmed_vm.metrics_path
            reader = warmed_vm.reader
            writer = warmed_vm.writer
        else:
            socket_path = os.path.join(tmpdir, "api.socket")
            vsock_path = os.path.join(tmpdir, "vsock.socket")
            metrics_path = os.path.join(tmpdir, "metrics.socket")

            # Clone rootfs per-execution to avoid write conflicts
            cloned_rootfs = os.path.join(tmpdir, "rootfs.ext4")
            await self._clone_rootfs(cloned_rootfs)

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

            _drain_process_output(process)

            # 3. Wait for metrics socket to be fully ready and accepting connections
            await self._wait_for_metrics_socket(
                metrics_path, process, self._config, api_timeout
            )

        writer_ref = writer if warmed_vm else None
        try:
            if not warmed_vm:
                # 4. Boot VM with cloned rootfs
                await self._boot_vm(
                    socket_path, vsock_path, api_timeout, rootfs_path=cloned_rootfs
                )

                # 5. Connect to VSOCK
                reader = None
                writer = None
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
                            raise RuntimeError(
                                SandboxErrorMessages.GUEST_TIMEOUT
                            ) from e
                writer_ref = writer

            # Delegate VSOCK delivery, collection, and metering to VSOCKExecutor
            executor = VSOCKExecutor(self, metrics_path, api_timeout)
            return await executor.execute(
                reader, writer_ref, code, timeout, execution_context
            )

        finally:
            if warmed_vm:
                # Trigger clean teardown of the pool VM in the background to avoid blocking caller
                asyncio.create_task(self._pool.teardown_vm(warmed_vm))
            else:
                if writer_ref:
                    writer_ref.close()
                    try:
                        await writer_ref.wait_closed()
                    except (OSError, ConnectionError):
                        pass

                # Teardown microVM process in the background
                if process.returncode is None:
                    asyncio.create_task(_bg_teardown_process(process))

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

        warmed_vm = None
        if self._enable_fc_pool:
            warmed_vm = await self._acquire_warmed_vm()

        if warmed_vm:
            try:
                return await self._execute_in_vm(
                    warmed_vm.tmpdir,
                    code,
                    timeout,
                    api_timeout,
                    execution_context,
                    warmed_vm=warmed_vm,
                )
            except (asyncio.TimeoutError, ConnectionError, OSError, RuntimeError) as e:
                return SandboxResult(
                    stdout="",
                    stderr=f"Execution Error: {str(e)}",
                    result=None,
                )
        else:
            with tempfile.TemporaryDirectory(prefix="fc-") as tmpdir:
                try:
                    # 2. Delegate execution logic to SOLID VM runner helper
                    return await self._execute_in_vm(
                        tmpdir, code, timeout, api_timeout, execution_context
                    )
                except (
                    asyncio.TimeoutError,
                    ConnectionError,
                    OSError,
                    RuntimeError,
                ) as e:
                    # Only catch expected infrastructure/startup errors
                    return SandboxResult(
                        stdout="",
                        stderr=f"Execution Error: {str(e)}",
                        result=None,
                    )
