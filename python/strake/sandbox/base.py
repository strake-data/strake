import socket
from abc import ABC, abstractmethod
from typing import Optional, Any
from dataclasses import dataclass


@dataclass
class SandboxResult:
    """Standardized result from a sandbox execution."""

    stdout: str
    stderr: str
    result: Any = None

    def to_str(self) -> str:
        """Backward compatibility: returns the stdout or stderr if failed."""
        if self.stderr:
            return self.stderr
        return self.stdout or "(No output)"


class SandboxManager(ABC):
    """
    Abstract interface for executing Agent-generated Python code safely.

    Note: The connection is stored directly. Ensure StrakeConnection uses
    Arc<> internally for thread-safe sharing (Python-side bindings usually
    handle this via the Rust-to-Python boundary), or concurrent operations
    may fail or result in deadlocks if the underlying Rust objects are moved.
    """

    def __init__(self, connection, config_path=None):
        self.connection = connection
        self.config_path = config_path

    @abstractmethod
    async def run(self, code: str) -> "SandboxResult":
        """Executes Python code in the sandbox and returns the result."""
        pass


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Accurately receive exactly n bytes from a stream socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed mid-read")
        buf.extend(chunk)
    return bytes(buf)
