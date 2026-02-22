import os
import time
import json
import logging
import socket
import subprocess
import uuid
import http.client
from typing import Optional
from .base import SandboxManager, _recv_exact, SandboxResult

logger = logging.getLogger("strake.sandbox.firecracker")


class UnixHTTPConnection(http.client.HTTPConnection):
    """Custom HTTPConnection for talking to Firecracker's Unix Domain Socket."""

    def __init__(self, path, timeout=None):
        if timeout is None:
            timeout = int(os.environ.get("FIRECRACKER_TIMEOUT_SECS", 10))
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.path)
        self.sock = sock


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

    def _api_request(
        self, socket_path: str, method: str, path: str, body: Optional[dict] = None
    ) -> None:
        conn = UnixHTTPConnection(socket_path)
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        payload = json.dumps(body) if body else None
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        conn.close()
        if response.status >= 400:
            raise RuntimeError(f"Firecracker API error {response.status}: {data}")

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

    async def run(self, code: str) -> "SandboxResult":
        logger.info("Initializing Firecracker microVM for code execution...")

        # Phase 3: Hardware isolation via VSOCK delivery
        sandbox_id = str(uuid.uuid4())
        import re

        if not re.match(r"^[a-f0-9-]{36}$", sandbox_id):
            raise ValueError(f"Invalid sandbox ID format: {sandbox_id}")

        socket_path = f"/tmp/firecracker-{sandbox_id}.socket"
        vsock_path = f"/tmp/firecracker-{sandbox_id}.vsock"

        # Defense in depth: normalize and verify no path traversal
        if (
            os.path.normpath(socket_path) != socket_path
            or os.path.normpath(vsock_path) != vsock_path
        ):
            raise ValueError("Potential path traversal detected in sandbox paths")

        # 1. Start Firecracker process
        process = subprocess.Popen(
            [self.fc_bin, "--api-sock", socket_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for socket
        retries = 50
        while not os.path.exists(socket_path):
            if process.poll() is not None:
                _, err = process.communicate()
                return SandboxResult(
                    stdout="",
                    stderr=f"Firecracker failed to start: {err.decode('utf-8')}",
                    result=None,
                )
            time.sleep(0.01)
            retries -= 1
            if retries <= 0:
                process.kill()
                return SandboxResult(
                    stdout="",
                    stderr="Timeout waiting for Firecracker API socket",
                    result=None,
                )

        vsock_conn = None
        try:
            # 2. Configure MicroVM
            # Note: guest_cid=3 is the default for Firecracker microVMs
            self._api_request(
                socket_path,
                "PUT",
                "/boot-source",
                {
                    "kernel_image_path": self.kernel_path,
                    "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/bin/agent_init",
                },
            )
            self._api_request(
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
            self._api_request(
                socket_path,
                "PUT",
                "/machine-config",
                {"vcpu_count": 1, "mem_size_mib": 128, "smt": False},
            )
            self._api_request(
                socket_path,
                "PUT",
                "/vsock",
                {"vsock_id": "1", "guest_cid": 3, "uds_path": vsock_path},
            )
            self._api_request(
                socket_path, "PUT", "/actions", {"action_type": "InstanceStart"}
            )

            # 3. Connect to VSOCK
            vsock_conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            retries = 50
            while True:
                try:
                    vsock_conn.connect(vsock_path)
                    break
                except (FileNotFoundError, ConnectionRefusedError):
                    if process.poll() is not None:
                        return SandboxResult(
                            stdout="",
                            stderr="Firecracker died during boot",
                            result=None,
                        )
                    time.sleep(0.1)
                    retries -= 1
                    if retries <= 0:
                        return SandboxResult(
                            stdout="",
                            stderr="Timeout waiting for Guest VSOCK agent",
                            result=None,
                        )

            # 4. Send Payload
            payload = json.dumps({"code": code}).encode("utf-8")
            # Protocol: 4 bytes length + payload
            vsock_conn.sendall(len(payload).to_bytes(4, byteorder="big"))
            vsock_conn.sendall(payload)

            # 5. Receive Output
            try:
                size_bytes = _recv_exact(vsock_conn, 4)
                size = int.from_bytes(size_bytes, byteorder="big")
                resp_bytes = _recv_exact(vsock_conn, size).decode("utf-8")
                response = json.loads(resp_bytes)
            except ConnectionError as e:
                return SandboxResult(
                    stdout="",
                    stderr=f"Failed to receive full response from guest: {e}",
                    result=None,
                )

            if "error" in response:
                return SandboxResult(
                    stdout="", stderr=f"Runtime Error: {response['error']}", result=None
                )

            output = response.get("output", "")
            return SandboxResult(
                stdout=output if output else "(No output)", stderr="", result=None
            )

        except Exception as e:
            return SandboxResult(
                stdout="", stderr=f"Execution Error: {str(e)}", result=None
            )
        finally:
            if vsock_conn:
                vsock_conn.close()
            # Cleanup
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

            for path in [socket_path, vsock_path]:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    pass
