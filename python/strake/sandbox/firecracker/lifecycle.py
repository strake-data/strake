"""MicroVM subprocess execution and snapshot lifecycle orchestrator.

Handles the raw subprocess spawning of Firecracker, KVM accessibility checks,
configuration setup, template snapshotting, and VM booting/resuming.
"""

# Standard Library
import asyncio
import logging
import os
import tempfile
from typing import Any, Optional, Protocol

# Local Application Imports
from strake.sandbox.base import (
    SandboxResult,
    SandboxErrorMessages,
    ExecutionContext,
)
from strake.sandbox.native import SandboxConfig
from strake.sandbox.firecracker.client import UnixSocketHttpClient

logger = logging.getLogger("strake.sandbox.firecracker.lifecycle")


class ApiRequestCallable(Protocol):
    """Protocol signature for the Firecracker HTTP socket request callback."""

    async def __call__(
        self,
        socket_path: str,
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        *,
        timeout: float = 5.0,
    ) -> str:
        """Execute request over control UDS."""
        ...


class MicroVMLifecycle:
    """Manages the lifecycle of a Firecracker microVM subprocess."""

    def __init__(
        self,
        fc_bin: str,
        kernel_path: str,
        rootfs_path: str,
        config: SandboxConfig,
        api_request_fn: Optional[ApiRequestCallable] = None,
    ) -> None:
        """Initialize the microVM lifecycle manager.

        Args:
            fc_bin: Path to the Firecracker binary.
            kernel_path: Path to the uncompressed vmlinux kernel image.
            rootfs_path: Path to the ext4 rootfs disk image.
            config: Sandbox configuration context.
            api_request_fn: Optional custom UDS HTTP request callable.
        """
        self.fc_bin = fc_bin
        self.kernel_path = kernel_path
        self.rootfs_path = rootfs_path
        self.config = config
        self.api_request_fn = api_request_fn or self._default_api_request

    async def _default_api_request(
        self,
        socket_path: str,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: float = 5.0,
    ) -> str:
        """Default HTTP over UDS request implementation.

        Args:
            socket_path: Unix socket file path.
            method: HTTP verb method string.
            path: Target URL request path.
            body: Optional JSON request payload dict.
            timeout: Socket timeout duration in seconds.

        Returns:
            The HTTP decoded body response string.
        """
        client = UnixSocketHttpClient(socket_path)
        return await client.request(method, path, body, timeout=timeout)

    @staticmethod
    def is_available(fc_bin: str, kernel_path: str, rootfs_path: str) -> bool:
        """Verify that Firecracker and its binary assets are accessible on the host.

        Args:
            fc_bin: Target path to the Firecracker binary.
            kernel_path: Target path to the guest kernel image.
            rootfs_path: Target path to the guest ext4 root file system.

        Returns:
            True if all assets are available and KVM permissions are satisfied.
        """
        if (
            not os.path.exists(fc_bin)
            or not os.path.exists(kernel_path)
            or not os.path.exists(rootfs_path)
        ):
            return False

        if not os.access("/dev/kvm", os.R_OK | os.W_OK):
            logger.warning(
                "/dev/kvm is not accessible. Firecracker requires KVM permissions."
            )
            return False

        return True

    async def wait_for_metrics_socket(
        self,
        metrics_path: str,
        process: asyncio.subprocess.Process,
        api_timeout: float,
    ) -> None:
        """Active connection-probing loop to ensure the Firecracker metrics HTTP server is ready.

        Args:
            metrics_path: Path to the metrics Unix socket.
            process: The Firecracker subprocess instance.
            api_timeout: Timeout to use for each socket API request probe.

        Raises:
            RuntimeError: If the Firecracker process terminated or failed to initialize.
        """
        metrics_client = UnixSocketHttpClient(metrics_path)
        metrics_retries = self.config.fc_socket_retries
        while True:
            if process.returncode is not None:
                err_b = b""
                if process.stderr is not None:
                    try:
                        err_b = await process.stderr.read()
                    except (OSError, ValueError):
                        err_b = b""
                err = err_b.decode("utf-8", errors="replace")
                raise RuntimeError(SandboxErrorMessages.FC_START_FAILED.format(err))

            try:
                await metrics_client.get("/metrics", timeout=api_timeout)
                logger.info(
                    "Firecracker metrics socket is fully initialized and listening."
                )
                break
            except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError):
                metrics_retries -= 1
                if metrics_retries <= 0:
                    logger.warning(
                        "Firecracker metrics socket failed to become ready. Telemetry may be zero baseline."
                    )
                    break
                await asyncio.sleep(self.config.fc_retry_delay_secs)

    async def configure_microvm(
        self, socket_path: str, *, api_timeout: float = 5.0
    ) -> None:
        """Configure the baseline hardware environment for a fresh microVM instance.

        Args:
            socket_path: Path to the microVM's control socket.
            api_timeout: API call request timeout limit.
        """
        await self.api_request_fn(
            socket_path,
            "PUT",
            "/boot-source",
            {
                "kernel_image_path": self.kernel_path,
                "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/bin/agent_init",
            },
            timeout=api_timeout,
        )
        await self.api_request_fn(
            socket_path,
            "PUT",
            "/drives/rootfs",
            {
                "drive_id": "rootfs",
                "path_on_host": self.rootfs_path,
                "is_root_device": True,
                "is_read_only": False,
            },
            timeout=api_timeout,
        )
        await self.api_request_fn(
            socket_path,
            "PUT",
            "/machine-config",
            {"vcpu_count": 1, "mem_size_mib": 128, "smt": False},
            timeout=api_timeout,
        )

    async def create_snapshot(self, snapshot_path: str, mem_path: str) -> None:
        """Generate a baseline guest template snapshot for fast restoration.

        Args:
            snapshot_path: Path where the guest state snapshot file will be saved.
            mem_path: Path where the guest memory dump file will be saved.
        """
        logger.info("Creating Firecracker template snapshot...")

        with tempfile.TemporaryDirectory(prefix="fc-template-") as tmpdir:
            socket_path = os.path.join(tmpdir, "api.socket")

            process = await asyncio.create_subprocess_exec(
                self.fc_bin,
                "--api-sock",
                socket_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            retries = self.config.fc_socket_retries
            while not os.path.exists(socket_path):
                if process.returncode is not None:
                    logger.error("Template VM failed to start")
                    return
                await asyncio.sleep(self.config.fc_retry_delay_secs)
                retries -= 1
                if retries <= 0:
                    logger.error("Template VM socket timeout")
                    try:
                        process.kill()
                    except OSError:
                        pass
                    return

            try:
                await self.configure_microvm(socket_path, api_timeout=5.0)

                await self.api_request_fn(
                    socket_path,
                    "PUT",
                    "/actions",
                    {"action_type": "InstanceStart"},
                    timeout=5.0,
                )

                await asyncio.sleep(0.5)

                await self.api_request_fn(
                    socket_path, "PATCH", "/vm", {"state": "Paused"}, timeout=5.0
                )
                await self.api_request_fn(
                    socket_path,
                    "PUT",
                    "/snapshot/create",
                    {
                        "snapshot_type": "Full",
                        "snapshot_path": snapshot_path,
                        "mem_file_path": mem_path,
                    },
                    timeout=5.0,
                )
                logger.info("Firecracker template snapshot created successfully.")
            except (RuntimeError, ConnectionError, OSError, asyncio.TimeoutError) as e:
                logger.error(f"Failed to create template snapshot: {e}")
            finally:
                if process.returncode is None:
                    try:
                        process.terminate()
                        await asyncio.sleep(0.1)
                        if process.returncode is None:
                            process.kill()

                        try:
                            stdout_b, stderr_b = await asyncio.wait_for(
                                process.communicate(), timeout=2
                            )
                        except asyncio.TimeoutError:
                            stdout_b, stderr_b = b"", b""

                        if process.returncode not in (None, 0):
                            stdout = stdout_b.decode("utf-8", errors="replace")
                            stderr = stderr_b.decode("utf-8", errors="replace")
                            logger.error(
                                "Firecracker template VM crashed! stdout=%s stderr=%s",
                                stdout,
                                stderr,
                            )
                    except (OSError, ProcessLookupError, asyncio.TimeoutError):
                        pass

    async def boot_vm(
        self,
        socket_path: str,
        vsock_path: str,
        snapshot_path: str,
        mem_path: str,
        snapshot_created: bool,
        api_timeout: float,
        *,
        rootfs_path: Optional[str] = None,
    ) -> None:
        """Restore the microVM state from an existing snapshot, or fallback to cold boot.

        Args:
            socket_path: Path to the main Firecracker API Unix socket.
            vsock_path: Host-side VSOCK path to register.
            snapshot_path: Path to the saved VM snapshot file.
            mem_path: Path to the saved VM memory file.
            snapshot_created: True if the snapshot files have been successfully created.
            api_timeout: Timeout duration for each API call.
            rootfs_path: Optional path to a cloned rootfs disk image for copy-on-write isolation.

        Raises:
            RuntimeError: If any of the setup API requests fail.
        """
        r_path = rootfs_path or self.rootfs_path
        if (
            snapshot_created
            and os.path.exists(snapshot_path)
            and (os.path.exists(mem_path) or self.config.fc_uffd_socket)
        ):
            logger.info("Resuming Firecracker microVM from template snapshot...")
            snapshot_load_body = {
                "snapshot_path": snapshot_path,
                "enable_diff_snapshots": False,
                "resume_vm": False,
            }
            if self.config.fc_uffd_socket:
                snapshot_load_body["mem_backend"] = {
                    "backend_path": self.config.fc_uffd_socket,
                    "backend_type": "Uffd",
                }
            else:
                snapshot_load_body["mem_file_path"] = mem_path

            await self.api_request_fn(
                socket_path,
                "PUT",
                "/snapshot/load",
                snapshot_load_body,
                timeout=api_timeout,
            )
            await self.api_request_fn(
                socket_path,
                "PATCH",
                "/drives/rootfs",
                {
                    "drive_id": "rootfs",
                    "path_on_host": r_path,
                },
                timeout=api_timeout,
            )
            await self._set_vsock(socket_path, vsock_path, api_timeout=api_timeout)
            await self.api_request_fn(
                socket_path,
                "PATCH",
                "/vm",
                {"state": "Resumed"},
                timeout=api_timeout,
            )
        else:
            logger.info("Performing cold boot for Firecracker microVM...")
            await self.api_request_fn(
                socket_path,
                "PUT",
                "/boot-source",
                {
                    "kernel_image_path": self.kernel_path,
                    "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/bin/agent_init",
                },
                timeout=api_timeout,
            )
            await self.api_request_fn(
                socket_path,
                "PUT",
                "/drives/rootfs",
                {
                    "drive_id": "rootfs",
                    "path_on_host": r_path,
                    "is_root_device": True,
                    "is_read_only": False,
                },
                timeout=api_timeout,
            )
            await self.api_request_fn(
                socket_path,
                "PUT",
                "/machine-config",
                {"vcpu_count": 1, "mem_size_mib": 128, "smt": False},
                timeout=api_timeout,
            )
            await self._set_vsock(socket_path, vsock_path, api_timeout=api_timeout)
            await self.api_request_fn(
                socket_path,
                "PUT",
                "/actions",
                {"action_type": "InstanceStart"},
                timeout=api_timeout,
            )

    async def _set_vsock(
        self, socket_path: str, vsock_path: str, *, api_timeout: float
    ) -> None:
        """Register the host-side UDS VSOCK path for guest agent communications.

        Args:
            socket_path: MicroVM control socket path.
            vsock_path: Host-side VSOCK communication socket path.
            api_timeout: Timeout limit for the VM API request.
        """
        await self.api_request_fn(
            socket_path,
            "PUT",
            "/vsock",
            {"vsock_id": "1", "guest_cid": 3, "uds_path": vsock_path},
            timeout=api_timeout,
        )

    async def deliver_code(
        self,
        writer: asyncio.StreamWriter,
        code: str,
        execution_context: ExecutionContext | None,
    ) -> None:
        """Deliver the Python source code and execution context to the guest over VSOCK.

        Delegates to the guest_protocol module.
        """
        from strake.sandbox.firecracker import guest_protocol

        await guest_protocol.deliver_code(writer, code, execution_context)

    async def collect_result(
        self,
        reader: asyncio.StreamReader,
        timeout: float,
    ) -> SandboxResult:
        """Read and decode the execution results from the guest agent via VSOCK.

        Delegates to the guest_protocol module.
        """
        from strake.sandbox.firecracker import guest_protocol

        return await guest_protocol.collect_result(
            reader, self.config.max_output_size, timeout
        )
