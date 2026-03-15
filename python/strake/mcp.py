# Standard Library
import argparse
import asyncio
import logging
import os
import sys
import threading
import time
from typing import Any
import contextlib

# Third-party Libraries
from mcp.server.fastmcp import FastMCP
import mcp.types as types

# Local Application Imports
from strake.sandbox.base import SandboxManager
from strake.sandbox.core import StrakeShim
from strake.sandbox.native import (
    LinuxSandboxManager,
    MacOSSandboxManager,
    WindowsSandboxManager,
)
from strake.tracing import get_emitter, AgentSession
from strake.tracing.session import code_field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("strake.mcp")

# Initialize FastMCP
mcp = FastMCP("strake")


@contextlib.asynccontextmanager
async def _tool_span(name: str, **extra_fields):
    """Context manager for emitting standardized tool call telemetry spans."""
    start = time.monotonic_ns()
    status = "ok"
    try:
        # yield a mutable dict to allow setting properties inside the block
        span_meta = {}
        yield span_meta
    except Exception as exc:
        status = f"error:{type(exc).__name__}"
        raise
    finally:
        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        get_emitter().emit(
            {
                "event": "span",
                "span_type": "tool_call",
                "name": name,
                "latency_ms": round(elapsed_ms, 2),
                "status": status,
                **extra_fields,
                **span_meta,
            }
        )


class MCPServer:
    def __init__(self):
        self._connection = None
        self._sandbox = None
        self._config_path = None
        self._lock: asyncio.Lock | None = None
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

    def _init_connection(self) -> Any:
        """Initializes the StrakeConnection based on configuration."""
        from strake import StrakeConnection

        if self._config_path:
            logger.info(
                f"Starting Embedded Strake Engine with config: {self._config_path}"
            )
            return StrakeConnection(self._config_path)

        url = os.environ.get("STRAKE_URL", "grpc://127.0.0.1:50051")
        token = os.environ.get("STRAKE_TOKEN")
        logger.info(f"Connecting to Strake at {url}")
        return StrakeConnection(url, api_key=token)

    def _init_sandbox(self, connection: Any) -> tuple[SandboxManager, str]:
        """Creates the appropriate SandboxManager based on environment."""
        env = os.environ.get("STRAKE_ENV", "development").lower()
        if env == "production":
            logger.info(
                "Production environment detected. Initializing Firecracker sandbox."
            )
            from strake.sandbox.firecracker import FirecrackerSandboxManager

            return FirecrackerSandboxManager(
                connection, self._config_path
            ), "firecracker"

        if sys.platform.startswith("linux"):
            logger.info("Linux environment detected. Using Native Linux Sandbox.")
            return LinuxSandboxManager(connection, self._config_path), "linux_native"

        if sys.platform == "darwin":
            logger.info("macOS environment detected. Using Native macOS Sandbox.")
            return MacOSSandboxManager(connection, self._config_path), "macos_seatbelt"

        if sys.platform == "win32":
            logger.info("Windows environment detected. Using Native Windows Sandbox.")
            return WindowsSandboxManager(
                connection, self._config_path
            ), "windows_appcontainer"

        raise RuntimeError(f"Unsupported OS for sandboxing: {sys.platform}")

    async def get_sandbox(self) -> SandboxManager:
        """Retrieves or initializes the sandbox manager."""
        async with self._get_lock():
            if self._sandbox is not None:
                return self._sandbox

            self._initialized = True
            try:
                self._connection = self._init_connection()
                sandbox, sandbox_type = self._init_sandbox(self._connection)
                self._sandbox = sandbox

                # Emit sandbox init trace event
                get_emitter().emit(
                    {
                        "event": "sandbox_init",
                        "sandbox_type": sandbox_type,
                        "platform": sys.platform,
                        "config_path": self._config_path,
                        "environment": os.environ.get(
                            "STRAKE_ENV", "development"
                        ).lower(),
                    }
                )

            except ImportError as e:
                logger.error(f"Failed to import Strake bindings: {e}")
                raise RuntimeError(
                    "Strake Python bindings not found. Ensure 'strake' is installed."
                ) from e

            return self._sandbox


# Global server instance
_SERVER = MCPServer()


async def get_sandbox() -> SandboxManager:
    """Module-level access to the sandbox for backward compatibility."""
    return await _SERVER.get_sandbox()


@mcp.tool()
async def search_schemas(
    query: str,
    include_descriptions: bool = True,
    description_scope: Any = "tables_only",
    max_description_length: int = 100,
) -> Any:
    """
    Search semantic index of available database schemas (tables and columns).
    Use this to find which tables contain the data you need.
    """
    try:
        sandbox = await _SERVER.get_sandbox()
        shim = StrakeShim(_SERVER._connection, sandbox)

        def _search():
            indexer = shim.get_indexer()
            return indexer.search_tables(
                query,
                include_descriptions=include_descriptions,
                description_scope=description_scope,
                max_description_length=max_description_length,
            )

        async with _tool_span("search_schemas", query=query) as span_meta:
            results = await asyncio.to_thread(_search)
            span_meta["result_count"] = len(results)
            return results
    except Exception as exc:
        return types.CallToolResult(
            isError=True, content=[types.TextContent(type="text", text=f"Error: {exc}")]
        )


@mcp.tool()
async def get_schema_details(fqn: str) -> Any:
    """
    Get the full schema metadata and descriptions for a specific table.
    Use this to inspect a table after discovering it via search_schemas.

    Args:
        fqn: Fully qualified table name (e.g. `schema.table` or `catalog.schema.table`)
    
    Returns:
        List of column metadata dictionaries for the table.
    """
    try:
        sandbox = await _SERVER.get_sandbox()
        shim = StrakeShim(_SERVER._connection, sandbox)

        def _get_details():
            indexer = shim.get_indexer()
            if indexer.table_name not in indexer.db.list_tables():
                indexer.build_index()
            table = indexer.db.open_table(indexer.table_name)

            # Use SQL string with escaped single quotes to prevent injection
            # since some lancedb versions expect a string for .where().
            fqn_escaped = fqn.replace("'", "''")
            results = (
                table.search()
                .where(f"table_id = '{fqn_escaped}'")
                .limit(1000)
                .to_list()
            )

            for r in results:
                if "vector" in r:
                    del r["vector"]
                if "text" in r:
                    del r["text"]
                if "description_hash" in r:
                    del r["description_hash"]
            return results

        async with _tool_span("get_schema_details", fqn=fqn) as span_meta:
            results = await asyncio.to_thread(_get_details)
            span_meta["result_count"] = len(results)
            return results
    except Exception as exc:
        return types.CallToolResult(
            isError=True, content=[types.TextContent(type="text", text=f"Error: {exc}")]
        )


@mcp.tool()
async def run_python(script: str) -> Any:
    """
    Execute a Python script in the Strake Safe Runtime.

    CRITICAL INSTRUCTIONS FOR THE AI AGENT:
    1. The script has access to a `strake` module. Use `strake.sql('SELECT * FROM xxx')` to execute SQL. The result is a Table proxy.
    2. Accessible methods on the Table proxy: `df.to_pylist()`, `df.to_pydict()`, `df.column_values('col')`, `print(df)`.
    3. You can also use `strake.search(query: str) -> List[Dict]` to search for relevant tables.
    4. You MUST use `print()` to output the data you want to see. The tool captures and returns standard output.
    5. Stop executing code and stop calling this tool once you have retrieved the final result needed to answer the user's query.

    Example:
    ```python
    results = strake.sql("SELECT * FROM sales LIMIT 5")
    print(results.to_pylist())
    ```
    """
    logger.info("Received run_python request")
    try:
        sandbox = await _SERVER.get_sandbox()
        async with _tool_span(
            "run_python", **code_field(script)
        ) as span_meta:
            guard_mode = os.environ.get("STRAKE_AGENT_GUARD_MODE", "dry_run")
            result = await sandbox.run(
                script,
                execution_context={"kind": "agent_mcp", "guard_mode": guard_mode},
            )
            result_str = result.to_str()
            span_meta["result_size_bytes"] = len(result_str.encode("utf-8"))
            if result.stderr:
                span_meta["status"] = "error:UserScriptError"
            return result_str
    except Exception as exc:
        return types.CallToolResult(
            isError=True, content=[types.TextContent(type="text", text=f"Error: {exc}")]
        )


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
