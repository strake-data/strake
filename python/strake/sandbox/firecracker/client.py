# Standard Library
import asyncio
import json
from typing import Any


class UnixSocketHttpClient:
    """Manual HTTP/1.1 client performing generic requests over Unix Domain Sockets."""

    def __init__(self, socket_path: str) -> None:
        """Initialize the Unix Domain Socket HTTP client.

        Args:
            socket_path: Absolute filepath to the Unix socket.
        """
        self.socket_path = socket_path

    async def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: float = 5.0,
    ) -> str:
        """Execute manual HTTP request over UDS and return the decoded response body.

        Args:
            method: HTTP method verb (GET, PUT, PATCH).
            path: Target Request URI path.
            body: Optional JSON request payload dictionary.
            timeout: Maximum connection and read timeout limit.

        Returns:
            The HTTP response body string if status is success (< 400).

        Raises:
            asyncio.TimeoutError: If the socket timeout is exceeded.
            RuntimeError: If status is >= 400 or response is unparseable.
            ConnectionError: If UDS connection fails.
        """
        try:
            reader, writer = await asyncio.open_unix_connection(self.socket_path)
        except OSError as exc:
            raise ConnectionError(
                f"Failed to connect to UDS at {self.socket_path}: {exc}"
            ) from exc

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
                # Read headers first (until double CRLF)
                header_bytes = b""

                async def read_headers() -> None:
                    nonlocal header_bytes
                    while b"\r\n\r\n" not in header_bytes:
                        chunk = await reader.read(4096)
                        if not chunk:
                            break
                        header_bytes += chunk

                await asyncio.wait_for(read_headers(), timeout=timeout)

                if b"\r\n\r\n" in header_bytes:
                    headers_part, body_part = header_bytes.split(b"\r\n\r\n", 1)

                    from email.parser import BytesParser

                    # Strip the HTTP status line before parsing headers
                    header_lines = headers_part.split(b"\r\n", 1)
                    headers_only = header_lines[1] if len(header_lines) > 1 else b""

                    # Use standard library BytesParser to handle HTTP headers in a typed, case-insensitive way
                    headers = BytesParser().parsebytes(headers_only)
                    content_length_val = headers.get("Content-Length")
                    content_length = None
                    if content_length_val is not None:
                        try:
                            content_length = int(str(content_length_val).strip())
                        except ValueError:
                            pass

                    if content_length is not None:
                        remaining = content_length - len(body_part)
                        if remaining > 0:

                            async def read_body() -> None:
                                nonlocal body_part
                                body_part += await reader.readexactly(remaining)

                            await asyncio.wait_for(read_body(), timeout=timeout)
                        body_part = body_part[:content_length]
                        response_data = headers_part + b"\r\n\r\n" + body_part
                    else:
                        # Fallback to EOF
                        async def read_eof() -> None:
                            nonlocal header_bytes
                            while True:
                                chunk = await reader.read(4096)
                                if not chunk:
                                    break
                                header_bytes += chunk

                        await asyncio.wait_for(read_eof(), timeout=timeout)
                        response_data = header_bytes
                else:
                    response_data = header_bytes
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"Firecracker API timeout on {method} {path} after {timeout}s"
                ) from exc

            # Defensive parsing of the HTTP response status and body
            if response_data:
                try:
                    decoded = response_data.decode("utf-8", errors="replace")
                    if "\r\n\r\n" not in decoded:
                        raise RuntimeError(
                            "Firecracker API returned unparseable response"
                        )
                    parts = decoded.split("\r\n\r\n", 1)
                    headers_part_str = parts[0]
                    body_part_str = parts[1] if len(parts) > 1 else ""

                    first_line = headers_part_str.split("\r\n")[0]
                    line_parts = first_line.split(" ", 2)
                    status_code = int(line_parts[1]) if len(line_parts) >= 2 else 500
                except (ValueError, IndexError) as exc:
                    raise RuntimeError(
                        "Firecracker API returned unparseable response"
                    ) from exc

                if status_code >= 400:
                    raise RuntimeError(
                        f"Firecracker API error {status_code}: {decoded}"
                    )
                return body_part_str
            return ""
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass

    async def get(self, path: str, *, timeout: float = 5.0) -> str:
        """Perform a standard HTTP GET request over UDS.

        Args:
            path: Target Request URI path.
            timeout: Maximum read timeout.

        Returns:
            The HTTP response body string.
        """
        return await self.request("GET", path, timeout=timeout)
