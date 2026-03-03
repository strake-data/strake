import asyncio
from mcp.server.fastmcp import FastMCP
import mcp.types as types

mcp = FastMCP("test")
from typing import Any

@mcp.tool()
def my_tool(fail: bool) -> Any:
    if fail:
        return types.CallToolResult(isError=True, content=[types.TextContent(type="text", text="Failed")])
    return "Success"

async def main():
    res1 = await mcp.call_tool("my_tool", {"fail": False})
    print("res1:", res1)
    res2 = await mcp.call_tool("my_tool", {"fail": True})
    print("res2:", res2)

asyncio.run(main())
