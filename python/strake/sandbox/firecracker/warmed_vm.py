"""Type-safe data transfer object for pre-warmed Firecracker microVM handles.

# Overview
Provides an immutable ``WarmedVM`` dataclass representing a fully booted,
VSOCK-connected Firecracker microVM ready for code execution.

# Usage
```python
vm = await pool.acquire_vm()
result = await manager._execute_in_vm(..., warmed_vm=vm)
```

# Performance Characteristics
Frozen dataclass provides O(1) attribute access. Note that hashability is unavailable due to unhashable stream handles.

# Safety
Contains ``asyncio.StreamWriter`` references. Must not be manipulated from
threads other than the owning event loop.

# Errors
No custom exceptions raised under normal usage.
"""

from dataclasses import dataclass
import asyncio
import tempfile


@dataclass(frozen=True)
class WarmedVM:
    """Type-safe wrapper representing a pre-warmed/running Firecracker microVM.

    Attributes:
        tmpdir_obj: TemporaryDirectory context object holding socket files.
        tmpdir: Path to the ephemeral workspace directory.
        process: Subprocess handler of the running Firecracker microVM.
        socket_path: Unix domain socket path for controlling Firecracker API.
        vsock_path: Unix domain socket path for communication with guest agent.
        metrics_path: Unix domain socket path for Firecracker Prometheus metrics.
        reader: StreamReader linked to the guest's VSOCK connection.
        writer: StreamWriter linked to the guest's VSOCK connection.
        cloned_rootfs: Path to the copy-on-write cloned rootfs disk image.
    """

    tmpdir_obj: tempfile.TemporaryDirectory
    tmpdir: str
    process: asyncio.subprocess.Process
    socket_path: str
    vsock_path: str
    metrics_path: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    cloned_rootfs: str
