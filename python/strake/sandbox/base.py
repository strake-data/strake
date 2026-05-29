from abc import ABC, abstractmethod
import enum
from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionContext:
    """Frozen dataclass container for execution metadata context.

    Attributes:
        session_id: Unique identifier for tracking this execution session.
    """

    session_id: str | None = None


class SandboxErrorMessages(str, enum.Enum):
    """Unified error message constants for the sandbox."""

    CONNECTION_FAILED = "Runtime Error: Sandbox could not connect to the data engine. Check server logs."
    TIMEOUT = "Resource Error: Execution timed out."
    INTERNAL_FAILURE = "Runtime Error: Internal sandbox failure"
    CODE_SIZE_EXCEEDED = "Security Error: Code size exceeds limit."
    IMPORT_NOT_PERMITTED = (
        "Security Error: Import of '{}' is not permitted in the sandbox."
    )
    DANGEROUS_GETATTR = "Security Error: Dangerous getattr target '{}'."
    PATTERN_MATCHING_NOT_ALLOWED = "Security Error: Pattern matching syntax not allowed"
    MEMORY_EXHAUSTED = "Resource Error: Output exceeded maximum size of {} bytes"
    GUEST_RESPONSE_OVERFLOW = "Runtime Error: Guest response size {} exceeds limit."
    GUEST_DIED = "Firecracker died during boot"
    GUEST_TIMEOUT = "Timeout waiting for Guest VSOCK agent"
    FC_START_FAILED = "Firecracker failed to start: {}"
    FC_SOCKET_TIMEOUT = "Timeout waiting for Firecracker API socket"


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

    def to_dict(self) -> dict:
        """Serialize the result for JSON IPC."""
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "result": self.result,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SandboxResult":
        """Reconstruct from JSON-compatible dictionary with basic validation."""
        if not isinstance(d, dict):
            return cls(
                stdout="",
                stderr="Runtime Error: Invalid IPC response format",
                result=None,
            )

        return cls(
            stdout=d.get("stdout") or "",
            stderr=d.get("stderr") or "",
            result=d.get("result"),
        )


class SandboxManager(ABC):
    """Abstract interface for executing Agent-generated Python code safely.

    Note: The connection is stored directly. Ensure StrakeConnection uses
    Arc<> internally for thread-safe sharing (Python-side bindings usually
    handle this via the Rust-to-Python boundary), or concurrent operations
    may fail or result in deadlocks if the underlying Rust objects are moved.
    """

    def __init__(self, connection: Any, config_path: str | None = None) -> None:
        """Initialize the sandbox execution manager.

        Args:
            connection: Database/Federation connection instance.
            config_path: Path to optional configuration files.
        """
        self.connection = connection
        self.config_path = config_path

    @abstractmethod
    async def run(
        self,
        code: str,
        timeout_secs: float | None = None,
        *,
        execution_context: ExecutionContext | None = None,
    ) -> SandboxResult:
        """Executes Python code in the sandbox and returns the result.

        execution_context is an optional, per-call metadata envelope used to
        propagate agent-only execution signals into the sandbox runtime.
        """
        pass
