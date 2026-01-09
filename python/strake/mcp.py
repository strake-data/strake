from mcp.server.fastmcp import FastMCP
import os
import json
import logging
try:
    # Try importing from package (when run as module)
    from ._strake import StrakeConnection
except ImportError:
    # Try importing from local build (dev fallback)
    try:
        from strake import StrakeConnection
    except ImportError:
        # Fallback for direct execution if _strake isn't built yet
        # This allows basic linting/testing without the extension
        pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("strake.mcp")

# Initialize FastMCP
mcp = FastMCP("strake")

# Global connection state
_CONNECTION = None
_CONFIG_PATH = None

def get_connection() -> "StrakeConnection":
    global _CONNECTION
    if _CONNECTION is None:
        if _CONFIG_PATH:
            logger.info(f"Starting Embedded Strake Engine with config: {_CONFIG_PATH}")
            # Embedded mode: Pass config path as first argument
            _CONNECTION = StrakeConnection(_CONFIG_PATH)
        else:
            url = os.environ.get("STRAKE_URL", "grpc://127.0.0.1:50051")
            token = os.environ.get("STRAKE_TOKEN")
            logger.info(f"Connecting to Strake at {url}")
            _CONNECTION = StrakeConnection(url, api_key=token)
    return _CONNECTION




@mcp.tool()
def query(sql: str) -> str:
    """
    Execute a SQL query against the Strake Federation Engine.
    Returns the result as a JSON string.
    """
    conn = get_connection()
    try:
        logger.info(f"Executing Query: {sql}")
        table = conn.sql(sql)
        # Convert PyArrow Table to Python Dictionary then to JSON
        # This is memory efficient enough for MCP text responses
        # For huge results, we should implement pagination or limits
        data = table.to_pydict()
        return json.dumps(data, default=str)
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return f"Error executing query: {str(e)}"

@mcp.tool()
def list_tables() -> str:
    """
    List all available tables in the federated data mesh.
    """
    conn = get_connection()
    try:
        # Try standard information_schema approach first
        # This works for DataFusion/Strake
        sql = "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'system')"
        table = conn.sql(sql)
        
        # Convert to list of dictionaries (row-based)
        rows = table.to_pylist()
        
        # Enrich with fully qualified names to help the LLM
        for row in rows:
            schema = row.get("table_schema", "")
            name = row.get("table_name", "")
            
            # Heuristic for qualification:
            # If schema is 'public' (default), just use the table name.
            # Otherwise use schema.table
            if schema == "public":
                row["fully_qualified_name"] = name
            else:
                row["fully_qualified_name"] = f"{schema}.{name}"
                
        return json.dumps(rows, default=str, indent=2)
    except Exception as e:
        logger.warning(f"information_schema.tables failed: {e}, falling back to describe()")
        # Fallback to the human-readable describe string
        return conn.describe()

@mcp.resource("strake://{schema}/{table}/schema")
def get_table_schema(schema: str, table: str) -> str:
    """
    Get the schema definition for a specific table.
    """
    conn = get_connection()
    try:
        # StrakeConnection.describe(table) returns pretty string
        # We might want structural later, but this is good for LLMs
        full_name = f"{schema}.{table}" if schema else table
        return conn.describe(full_name)
    except Exception as e:
        return f"Error fetching schema for {schema}.{table}: {str(e)}"

@mcp.prompt("debug_query")
def debug_query_prompt(query: str, error: str) -> str:
    """
    Analyze a failed SQL query and suggest fixes.
    """
    return f"""The following SQL query failed against the Strake engine:
    
SQL: {query}
Error: {error}

Please analyze the error and the query. Check for:
1. Syntax errors (e.g. trailing commas, missing keywords)
2. Schema mismatches (calling list_tables() might help)
3. Dialect issues (Strake uses Postgres/DataFusion dialect)

Propose a fixed query.
"""

import argparse

def main():
    """Entry point for python -m strake.mcp"""
    parser = argparse.ArgumentParser(description="Strake MCP Server")
    parser.add_argument("--transport", type=str, default="stdio", choices=["stdio", "sse"], help="Transport mode")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind for SSE")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind for SSE")
    parser.add_argument("--config", type=str, help="Path to strake.yaml for embedded execution")
    
    args = parser.parse_args()
    
    if args.config:
        global _CONFIG_PATH
        _CONFIG_PATH = args.config
    
    if args.transport == "sse":
        logger.info(f"Starting MCP SSE server on {args.host}:{args.port}")
        import uvicorn
        # mcp.sse_app() returns the underlying Starlette/FastAPI app for SSE
        app = mcp.sse_app()
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        mcp.run()

if __name__ == "__main__":
    main()
