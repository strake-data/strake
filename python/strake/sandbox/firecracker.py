import os
import time
import json
import logging
import socket
import subprocess
import uuid
import http.client
import asyncio
import tempfile
import signal
from typing import Optional, Any
from .base import SandboxManager, SandboxResult, SandboxErrorMessages
from .native import SandboxConfig

logger = logging.getLogger("strake.sandbox.firecracker")


# UnixHTTPConnection is removed in favor of asyncio.open_unix_connection


class FirecrackerSandboxManager(SandboxManager):
    """
    Executes Python code safely within an ephemeral Firecracker microVM.
    Phase 1 implementation for Code Mode.
    """

    def __init__(
        self,
        connection,
        config_path: Optional[str] = None,
        fc_bin: Optional[str] = None,
        kernel_path: Optional[str] = None,
        rootfs_path: Optional[str] = None,
    ):
        super().__init__(connection, config_path)
        self.fc_bin = fc_bin or os.environ.get("FIRECRACKER_BIN", "firecracker")
        self.kernel_path = kernel_path or os.environ.get(
            "FIRECRACKER_KERNEL", "/var/lib/firecracker/vmlinux"
        )
        self.rootfs_path = rootfs_path or os.environ.get(
            "FIRECRACKER_ROOTFS", "/var/lib/firecracker/rootfs.ext4"
        )

        # Check prerequisites
        if not self.is_firecracker_available(
            self.fc_bin, self.kernel_path, self.rootfs_path
        ):
            logger.error(
                "Firecracker prerequisites not met. Refusing to fall back to INSECURE sandbox in production."
            )
            raise RuntimeError(
                "Firecracker sandbox is not available and insecure fallback is disabled. "
                "Check FIRECRACKER_BIN, KERNEL, and ROOTFS environment variables."
            )

    async def _api_request(
        self, socket_path: str, method: str, path: str, body: Optional[dict] = None
    ) -> None:
        """Perform an async HTTP request over a Unix Domain Socket."""
        reader, writer = await asyncio.open_unix_connection(socket_path)
        try:
            payload = json.dumps(body) if body else ""
            request = (
                f"{method} {path} HTTP/1.1\r\n"
                f"Host: localhost\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f"Accept: application/json\r\n"
                f"Connection: close\r\n\r\n"
                f"{payload}"
            )
            writer.write(request.encode("utf-8"))
            await writer.drain()

            response_data = await reader.read()
            # Defensive parsing of the HTTP response status and body
            if response_data:
                try:
                    decoded = response_data.decode("utf-8", errors="replace")
                    first_line = decoded.split("\r\n")[0]
                    parts = first_line.split(" ", 2)
                    status_code = int(parts[1]) if len(parts) >= 2 else 500
                except (ValueError, IndexError):
                    raise RuntimeError("Firecracker API returned unparseable response")

                if status_code >= 400:
                    raise RuntimeError(f"Firecracker API error {status_code}: {decoded}")
        finally:
            writer.close()
            await writer.wait_closed()

    def is_firecracker_available(
        self, fc_bin: str, kernel_path: str, rootfs_path: str
    ) -> bool:
        """Check if all Firecracker prerequisites are met."""
        # 1. Check binaries and images
        if (
            not os.path.exists(fc_bin)
            or not os.path.exists(kernel_path)
            or not os.path.exists(rootfs_path)
        ):
            return False

        # 2. Check /dev/kvm permissions
        if not os.access("/dev/kvm", os.R_OK | os.W_OK):
            logger.warning(
                "/dev/kvm is not accessible. Firecracker requires KVM permissions."
            )
            return False

        return True

    async def run(self, code: str, timeout_secs: Optional[float] = None) -> "SandboxResult":
        logger.info("Initializing Firecracker microVM for code execution...")
        config = SandboxConfig.from_env()

        with tempfile.TemporaryDirectory(prefix="fc-") as tmpdir:
            socket_path = os.path.join(tmpdir, "api.socket")
            vsock_path = os.path.join(tmpdir, "vsock.socket")

            # 1. Start Firecracker process
            process = subprocess.Popen(
                [self.fc_bin, "--api-sock", socket_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Wait for socket
            retries = config.fc_socket_retries
            while not os.path.exists(socket_path):
                if process.poll() is not None:
                    _, err = process.communicate()
                    return SandboxResult(
                        stdout="",
                        stderr=SandboxErrorMessages.FC_START_FAILED.format(err.decode("utf-8")),
                        result=None,
                    )
                await asyncio.sleep(config.fc_retry_delay_secs)
                retries -= 1
                if retries <= 0:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    return SandboxResult(
                        stdout="",
                        stderr=SandboxErrorMessages.FC_SOCKET_TIMEOUT,
                        result=None,
                    )

            writer = None
            try:
                # 2. Configure MicroVM
                # Note: guest_cid=3 is the default for Firecracker microVMs
                await self._api_request(
                    socket_path,
                    "PUT",
                    "/boot-source",
                    {
                        "kernel_image_path": self.kernel_path,
                        "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/bin/agent_init",
                    },
                )
                await self._api_request(
                    socket_path,
                    "PUT",
                    "/drives/rootfs",
                    {
                        "drive_id": "rootfs",
                        "path_on_host": self.rootfs_path,
                        "is_root_device": True,
                        "is_read_only": False,
                    },
                )
                await self._api_request(
                    socket_path,
                    "PUT",
                    "/machine-config",
                    {"vcpu_count": 1, "mem_size_mib": 128, "smt": False},
                )
                await self._api_request(
                    socket_path,
                    "PUT",
                    "/vsock",
                    {"vsock_id": "1", "guest_cid": 3, "uds_path": vsock_path},
                )
                await self._api_request(
                    socket_path, "PUT", "/actions", {"action_type": "InstanceStart"}
                )

                # 3. Connect to VSOCK
                reader, writer = (None, None)
                retries = config.fc_socket_retries
                while True:
                    try:
                        reader, writer = await asyncio.open_unix_connection(vsock_path)
                        break
                    except (FileNotFoundError, ConnectionRefusedError):
                        if process.poll() is not None:
                            return SandboxResult(
                                stdout="",
                                stderr=SandboxErrorMessages.GUEST_DIED,
                                result=None,
                            )
                        await asyncio.sleep(0.1)
                        retries -= 1
                        if retries <= 0:
                            return SandboxResult(
                                stdout="",
                                stderr=SandboxErrorMessages.GUEST_TIMEOUT,
                                result=None,
                            )

                # 4. Send Payload
                payload = json.dumps({"code": code}).encode("utf-8")
                # Protocol: 4 bytes length + payload
                writer.write(len(payload).to_bytes(4, byteorder="big"))
                writer.write(payload)
                await writer.drain()

                # 5. Receive Output (with size guard to prevent host memory exhaustion)
                try:
                    size_bytes = await reader.readexactly(4)
                    size = int.from_bytes(size_bytes, byteorder="big")

                    if size > config.max_output_size:
                        return SandboxResult(
                            stdout="",
                            stderr=SandboxErrorMessages.GUEST_RESPONSE_OVERFLOW.format(size),
                            result=None,
                        )

                    resp_bytes = (await reader.readexactly(size)).decode("utf-8")
                    response = json.loads(resp_bytes)
                except (ConnectionError, asyncio.IncompleteReadError) as e:
                    return SandboxResult(
                        stdout="",
                        stderr=f"Failed to receive full response from guest: {e}",
                        result=None,
                    )

                # Schema validation for guest response
                if not isinstance(response, dict):
                    return SandboxResult(
                        stdout="",
                        stderr="Runtime Error: Invalid guest response format",
                        result=None,
                    )

                if "error" in response:
                    return SandboxResult(
                        stdout="", stderr=f"Runtime Error: {response['error']}", result=None
                    )

                output = response.get("output", "")
                if not isinstance(output, str):
                    output = str(output)

                return SandboxResult(
                    stdout=output if output else "(No output)", stderr="", result=None
                )

            except Exception as e:
                return SandboxResult(
                    stdout="", stderr=f"Execution Error: {str(e)}", result=None
                )
            finally:
                if writer:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                # Cleanup process with race protection
                if process.poll() is None:
                    try:
                        process.terminate()
                        await asyncio.sleep(0.1)  # Give it a moment to exit
                        if process.poll() is None:
                            process.kill()
                        process.wait(timeout=5)
                    except (ProcessLookupError, subprocess.TimeoutExpired):
                        pass

