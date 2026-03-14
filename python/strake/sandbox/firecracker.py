from __future__ import annotations

# Standard Library
import asyncio
import http.client
import json
import logging
import os
import signal
import socket
import tempfile
import time
import uuid
from typing import Any, Optional

# Local Application Imports
from strake.sandbox.base import SandboxManager, SandboxResult, SandboxErrorMessages
from strake.sandbox.native import SandboxConfig

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

        # Base snapshot configuration
        # A unique temporary directory for this VM run.
        # This will securely hold the socket and any volume mounts.
        # CRITICAL: shutdown() MUST be called to ensure cleanup.
        self._snapshot_dir = tempfile.TemporaryDirectory(prefix="strake-vm-")
        self.snapshot_path = os.path.join(self._snapshot_dir.name, "snapshot.ext4")
        self.mem_path = os.path.join(self._snapshot_dir.name, "mem.ext4")
        # Snapshot initialization task to be awaited in run()
        self._snapshot_task = None
        self._snapshot_created = False
        # Safe in Python 3.10+: asyncio.Lock() does not require a running event loop at construction.
        self._snapshot_lock = asyncio.Lock()

        try:
            # Kick off snapshot creation when constructed inside an active event loop.
            # If no loop is running (sync construction), defer snapshot creation to run().
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None:
                self._snapshot_task = loop.create_task(self._ensure_snapshot())
        except Exception as e:
            logger.error(f"Failed to create Firecracker snapshot: {e}")
            # Fallback to cold boot if snapshot fails

    async def shutdown(self) -> None:
        """Explicitly cleans up resources including snapshots and background tasks."""
        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Error cancelling snapshot task: {e}")
            self._snapshot_task = None

        if self._snapshot_dir:
            self._snapshot_dir.cleanup()
            self._snapshot_dir = None

    async def _configure_microvm(self, socket_path: str, *, api_timeout: float = 5.0) -> None:
        """Configures the microVM kernel, drives, and machine settings."""
        await self._api_request(
            socket_path,
            "PUT",
            "/boot-source",
            {
                "kernel_image_path": self.kernel_path,
                "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/bin/agent_init",
            },
            timeout=api_timeout,
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
            timeout=api_timeout,
        )
        await self._api_request(
            socket_path,
            "PUT",
            "/machine-config",
            {"vcpu_count": 1, "mem_size_mib": 128, "smt": False},
            timeout=api_timeout,
        )

    async def _create_snapshot(self) -> None:
        """Creates a template microVM snapshot to resume from for faster boot times."""
        if self._snapshot_created:
            return

        logger.info("Creating Firecracker template snapshot...")
        config = SandboxConfig.from_env()

        with tempfile.TemporaryDirectory(prefix="fc-template-") as tmpdir:
            socket_path = os.path.join(tmpdir, "api.socket")

            process = await asyncio.create_subprocess_exec(
                self.fc_bin,
                "--api-sock",
                socket_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait for socket
            retries = config.fc_socket_retries
            while not os.path.exists(socket_path):
                if process.returncode is not None:
                    logger.error("Template VM failed to start")
                    return
                await asyncio.sleep(config.fc_retry_delay_secs)
                retries -= 1
                if retries <= 0:
                    logger.error("Template VM socket timeout")
                    try:
                        process.kill()
                    except Exception:
                        pass
                    return

            try:
                # Configure and Start Template MicroVM
                await self._configure_microvm(socket_path, api_timeout=5.0)

                await self._api_request(
                    socket_path,
                    "PUT",
                    "/actions",
                    {"action_type": "InstanceStart"},
                    timeout=5.0,
                )

                # Wait for guest OS to boot up slightly
                await asyncio.sleep(0.5)

                # Pause and Snapshot
                await self._api_request(
                    socket_path, "PATCH", "/vm", {"state": "Paused"}, timeout=5.0
                )
                await self._api_request(
                    socket_path,
                    "PUT",
                    "/snapshot/create",
                    {
                        "snapshot_type": "Full",
                        "snapshot_path": self.snapshot_path,
                        "mem_file_path": self.mem_path,
                    },
                    timeout=5.0,
                )

                self._snapshot_created = True
                logger.info("Firecracker template snapshot created successfully.")
            except Exception as e:
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
                    except Exception:
                        pass

    async def _api_request(
        self,
        socket_path: str,
        method: str,
        path: str,
        body: Optional[dict] = None,
        *,
        timeout: float = 5.0,
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

            try:
                response_data = await asyncio.wait_for(reader.read(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"Firecracker API timeout on {method} {path} after {timeout}s"
                ) from exc
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
                    raise RuntimeError(
                        f"Firecracker API error {status_code}: {decoded}"
                    )
        finally:
            writer.close()
            await writer.wait_closed()

    async def _ensure_snapshot(self) -> None:
        async with self._snapshot_lock:
            if self._snapshot_created:
                return
            await self._create_snapshot()

    async def _set_vsock(self, socket_path: str, vsock_path: str, *, api_timeout: float) -> None:
        """Register the host-side VSOCK UDS path for this VM instance."""
        await self._api_request(
            socket_path,
            "PUT",
            "/vsock",
            {"vsock_id": "1", "guest_cid": 3, "uds_path": vsock_path},
            timeout=api_timeout,
        )

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

    async def run(
        self,
        code: str,
        timeout_secs: Optional[float] = None,
        *,
        execution_context: Optional[dict[str, str]] = None,
    ) -> "SandboxResult":
        # Await snapshot creation (if scheduled) or create lazily (if constructed sync).
        if self._snapshot_task is not None:
            try:
                await self._snapshot_task
            except Exception as e:
                logger.error(f"Background snapshot creation failed: {e}")
            finally:
                self._snapshot_task = None
        elif not self._snapshot_created:
            try:
                await self._ensure_snapshot()
            except Exception as e:
                logger.error(f"Snapshot creation failed (cold boot fallback): {e}")

        logger.info("Initializing Firecracker microVM for code execution...")
        config = SandboxConfig.from_env()
        timeout = timeout_secs or config.timeout_secs
        api_timeout = min(timeout, 5.0)

        with tempfile.TemporaryDirectory(prefix="fc-") as tmpdir:
            socket_path = os.path.join(tmpdir, "api.socket")
            vsock_path = os.path.join(tmpdir, "vsock.socket")

            # 1. Start Firecracker process
            process = await asyncio.create_subprocess_exec(
                self.fc_bin,
                "--api-sock",
                socket_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Wait for socket
            retries = config.fc_socket_retries
            while not os.path.exists(socket_path):
                if process.returncode is not None:
                    err_b = b""
                    if process.stderr is not None:
                        try:
                            err_b = await process.stderr.read()
                        except Exception:
                            err_b = b""
                    err = err_b.decode("utf-8", errors="replace")
                    return SandboxResult(
                        stdout="",
                        stderr=SandboxErrorMessages.FC_START_FAILED.format(
                            err
                        ),
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
                if (
                    self._snapshot_created
                    and os.path.exists(self.snapshot_path)
                    and os.path.exists(self.mem_path)
                ):
                    # Fast Path: Resume from Snapshot
                    # Update rootfs and vsock for the resumed VM
                    await self._api_request(
                        socket_path,
                        "PUT",
                        "/snapshot/load",
                        {
                            "snapshot_path": self.snapshot_path,
                            "mem_file_path": self.mem_path,
                            "enable_diff_snapshots": False,
                            "resume_vm": False,
                        },
                        timeout=api_timeout,
                    )
                    # Use PATCH to replace the rootfs path. `is_read_only` remains the same.
                    # This is necessary because Firecracker's snapshots restore the exact same path.
                    await self._api_request(
                        socket_path,
                        "PATCH",
                        "/drives/rootfs",
                        {
                            "drive_id": "rootfs",
                            "path_on_host": self.rootfs_path,
                        },
                        timeout=api_timeout,
                    )
                    # Snapshot restores the guest CID, but the host-side UDS path is per-run.
                    await self._set_vsock(
                        socket_path, vsock_path, api_timeout=api_timeout
                    )
                    # Resume
                    await self._api_request(
                        socket_path,
                        "PATCH",
                        "/vm",
                        {"state": "Resumed"},
                        timeout=api_timeout,
                    )
                else:
                    # Slow path (Cold Boot)
                    await self._api_request(
                        socket_path,
                        "PUT",
                        "/boot-source",
                        {
                            "kernel_image_path": self.kernel_path,
                            "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/bin/agent_init",
                        },
                        timeout=api_timeout,
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
                        timeout=api_timeout,
                    )
                    await self._api_request(
                        socket_path,
                        "PUT",
                        "/machine-config",
                        {"vcpu_count": 1, "mem_size_mib": 128, "smt": False},
                        timeout=api_timeout,
                    )
                    await self._set_vsock(socket_path, vsock_path, api_timeout=api_timeout)
                    await self._api_request(
                        socket_path,
                        "PUT",
                        "/actions",
                        {"action_type": "InstanceStart"},
                        timeout=api_timeout,
                    )

                # 3. Connect to VSOCK
                reader: asyncio.StreamReader | None = None
                writer: asyncio.StreamWriter | None = None
                retries = config.fc_socket_retries
                while True:
                    try:
                        reader, writer = await asyncio.open_unix_connection(vsock_path)
                        break
                    except (FileNotFoundError, ConnectionRefusedError):
                        if process.returncode is not None:
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
                body: dict[str, Any] = {"code": code}
                if execution_context:
                    body["execution_context"] = execution_context
                payload = json.dumps(body).encode("utf-8")
                # Protocol: 4 bytes length + payload
                writer.write(len(payload).to_bytes(4, byteorder="big"))
                writer.write(payload)
                await writer.drain()

                # 5. Receive Output (with size guard to prevent host memory exhaustion)
                try:
                    size_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
                    size = int.from_bytes(size_bytes, byteorder="big")

                    if size > config.max_output_size:
                        return SandboxResult(
                            stdout="",
                            stderr=SandboxErrorMessages.GUEST_RESPONSE_OVERFLOW.format(
                                size
                            ),
                            result=None,
                        )

                    resp_bytes = (
                        await asyncio.wait_for(reader.readexactly(size), timeout=timeout)
                    ).decode("utf-8")
                    response = json.loads(resp_bytes)
                except asyncio.TimeoutError:
                    return SandboxResult(
                        stdout="",
                        stderr=SandboxErrorMessages.TIMEOUT,
                        result=None,
                    )
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
                        stdout="",
                        stderr=f"Runtime Error: {response['error']}",
                        result=None,
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
                if process.returncode is None:
                    try:
                        process.terminate()
                        await asyncio.sleep(0.1)  # Give it a moment to exit
                        if process.returncode is None:
                            process.kill()
                        await asyncio.wait_for(process.wait(), timeout=5)
                    except (ProcessLookupError, asyncio.TimeoutError):
                        pass
