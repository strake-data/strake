"""VSOCK-based guest agent communication protocol helper."""

# Standard Library
import asyncio
import json
from typing import Any

# Local Application Imports
from strake.sandbox.base import (
    SandboxResult,
    SandboxErrorMessages,
    ExecutionContext,
)


async def deliver_code(
    writer: asyncio.StreamWriter,
    code: str,
    execution_context: ExecutionContext | None,
) -> None:
    """Deliver the Python source code and execution context to the guest over VSOCK.

    Args:
        writer: VSOCK connection stream writer.
        code: Python source code string.
        execution_context: Optional ExecutionContext structure for context parameters.

    Raises:
        ConnectionError: If writing to the stream fails.
    """
    body: dict[str, Any] = {"code": code}
    if execution_context and execution_context.session_id:
        body["execution_context"] = {"session_id": execution_context.session_id}
    payload = json.dumps(body).encode("utf-8")
    writer.write(len(payload).to_bytes(4, byteorder="big"))
    writer.write(payload)
    await writer.drain()


async def collect_result(
    reader: asyncio.StreamReader,
    max_output_size: int,
    timeout: float,
) -> SandboxResult:
    """Read and decode the execution results from the guest agent via VSOCK.

    Args:
        reader: VSOCK connection stream reader.
        max_output_size: Maximum permitted response payload size in bytes.
        timeout: Timeout in seconds for response reading.

    Returns:
        A SandboxResult containing stdout, stderr, and result structure.

    Raises:
        asyncio.TimeoutError: If reading guest response exceeds timeout limit.
        ConnectionError: If connection is closed or incomplete read occurs.
        RuntimeError: If UDS HTTP parsing fails.
    """
    try:
        size_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
        size = int.from_bytes(size_bytes, byteorder="big")

        if size > max_output_size:
            return SandboxResult(
                stdout="",
                stderr=SandboxErrorMessages.GUEST_RESPONSE_OVERFLOW.format(size),
                result=None,
            )

        resp_bytes = (
            await asyncio.wait_for(reader.readexactly(size), timeout=timeout)
        ).decode("utf-8")
        response = json.loads(resp_bytes)
    except asyncio.TimeoutError as e:
        raise asyncio.TimeoutError(SandboxErrorMessages.TIMEOUT) from e
    except (ConnectionError, asyncio.IncompleteReadError) as e:
        raise ConnectionError(f"Failed to receive full response from guest: {e}") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("Runtime Error: Guest response was invalid JSON") from e

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
