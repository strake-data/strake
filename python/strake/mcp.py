from mcp.server.fastmcp import FastMCP
import os
import logging
import argparse
import threading
from .sandbox import (
    SandboxManager,
    LinuxSandboxManager,
    MacOSSandboxManager,
    WindowsSandboxManager,
)
from .sandbox.core import StrakeShim
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("strake.mcp")

# Initialize FastMCP
mcp = FastMCP("strake")

import asyncio


class MCPServer:
    def __init__(self):
        self._connection = None
        self._sandbox = None
        self._config_path = None
        self._lock: Optional[asyncio.Lock] = None
        self._thread_lock = threading.Lock()
        self._initialized = False

    def _get_lock(self) -> asyncio.Lock:
        """
        Lazily initialize the asyncio Lock.
        Note: Requires Python 3.10+ where asyncio.Lock() does not capture the running loop at construction.
        """
        with self._thread_lock:
            if self._lock is None:
                self._lock = asyncio.Lock()
            return self._lock

    def set_config_path(self, path: str):
        if self._initialized:
            raise RuntimeError("Engine already initialized. Cannot set config path.")
        self._config_path = path

    async def get_sandbox(self) -> SandboxManager:
        async with self._get_lock():
            if self._sandbox is None:
                self._initialized = True
                try:
                    from strake import StrakeConnection

                    if self._config_path:
                        logger.info(
                            f"Starting Embedded Strake Engine with config: {self._config_path}"
                        )
                        self._connection = StrakeConnection(self._config_path)
                    else:
                        url = os.environ.get("STRAKE_URL", "grpc://127.0.0.1:50051")
                        token = os.environ.get("STRAKE_TOKEN")
                        logger.info(f"Connecting to Strake at {url}")
                        self._connection = StrakeConnection(url, api_key=token)

                    env = os.environ.get("STRAKE_ENV", "development").lower()
                    if env == "production":
                        logger.info(
                            "Production environment detected. Initializing Firecracker sandbox."
                        )
                        from .sandbox.firecracker import FirecrackerSandboxManager

                        self._sandbox = FirecrackerSandboxManager(
                            self._connection, self._config_path
                        )
                    else:
                        import sys

                        if sys.platform == "linux" or sys.platform == "linux2":
                            logger.info(
                                "Linux environment detected. Using Native Linux Sandbox (Landlock/Seccomp/Namespaces)."
                            )
                            self._sandbox = LinuxSandboxManager(
                                self._connection, self._config_path
                            )
                        elif sys.platform == "darwin":
                            logger.info(
                                "macOS environment detected. Using Native macOS Sandbox (Seatbelt/SIP)."
                            )
                            self._sandbox = MacOSSandboxManager(
                                self._connection, self._config_path
                            )
                        elif sys.platform == "win32":
                            logger.info(
                                "Windows environment detected. Using Native Windows Sandbox (Job Objects/AppContainer)."
                            )
                            self._sandbox = WindowsSandboxManager(
                                self._connection, self._config_path
                            )
                        else:
                            raise RuntimeError(
                                f"Unsupported OS for sandboxing: {sys.platform}"
                            )
                except ImportError as e:
                    logger.error(f"Failed to import Strake bindings: {e}")
                    raise RuntimeError(
                        "Strake Python bindings not found. Ensure 'strake' is installed."
                    )
            return self._sandbox


# Global server instance
_SERVER = MCPServer()


async def get_sandbox() -> SandboxManager:
    """Module-level access to the sandbox for backward compatibility."""
    return await _SERVER.get_sandbox()


@mcp.tool()
async def search_schemas(query: str) -> List[Dict[str, Any]]:
    """
    Search semantic index of available database schemas (tables and columns).
    Use this to find which tables contain the data you need.
    """
    sandbox = await _SERVER.get_sandbox()
    shim = StrakeShim(_SERVER._connection, sandbox)

    def _search():
        indexer = shim.get_indexer()
        return indexer.search_tables(query)

    return await asyncio.to_thread(_search)


@mcp.tool()
async def run_python(script: str) -> str:
    """
    Execute a Python script in the Strake Safe Runtime.

    The script has access to a `strake` module with the following methods:
    - `strake.sql(query: str) -> List[Dict]`: Execute a SQL query and return results.
    - `strake.search(query: str) -> List[Dict]`: Search for tables relevant to your query.

    Standard output (print) is captured and returned.

    Example:
    ```python
    tables = strake.search("revenue")
    print(f"Found tables: {tables}")
    df = strake.sql("SELECT * FROM sales LIMIT 5")
    print(df)
    ```
    """
    logger.info("Received run_python request")
    sandbox = await _SERVER.get_sandbox()
    # Execute the sandbox asynchronously
    result = await sandbox.run(script)
    # Return string representation for backward compatibility with tests/clients
    return result.to_str()


def main():
    """Entry point for python -m strake.mcp"""
    parser = argparse.ArgumentParser(description="Strake MCP Server")
    parser.add_argument(
        "--transport",
        type=str,
        default="stdio",
        choices=["stdio", "sse"],
        help="Transport mode",
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host to bind for SSE"
    )
    parser.add_argument("--port", type=int, default=8000, help="Port to bind for SSE")
    parser.add_argument(
        "--config", type=str, help="Path to strake.yaml for embedded execution"
    )

    args = parser.parse_args()

    if args.config:
        _SERVER.set_config_path(args.config)

    if args.transport == "sse":
        logger.info(f"Starting MCP SSE server on {args.host}:{args.port}")
        import uvicorn

        app = mcp.sse_app()
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        mcp.run()


if __name__ == "__main__":
    main()
