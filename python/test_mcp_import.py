import sys
import os

# Ensure we can import strake
try:
    import strake

    print(f"Successfully imported strake from {strake.__file__}")
except ImportError as e:
    print(f"Failed to import strake: {e}")
    sys.exit(1)

try:
    from strake.mcp import mcp

    print("Successfully imported mcp object")

    # Introspect tools
    # FastMCP stores tools in _tool_handlers or similar, but let's not rely on internals.
    # We'll just verify imports worked and query function exists.
    from strake.mcp import run_python, search_schemas

    print("Found run_python and search_schemas functions")

except ImportError as e:
    print(f"Failed to import strake.mcp: {e}")
    sys.exit(1)
