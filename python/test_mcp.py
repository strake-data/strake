import asyncio
from unittest.mock import AsyncMock
import mcp.types as types
from strake.mcp import mcp, _SERVER

async def test_mcp_errors():
    # Mock get_sandbox to raise an exception to simulate failure
    _SERVER.get_sandbox = AsyncMock(side_effect=RuntimeError("Mock connection failed"))
    
    # Test run_python
    res1 = await mcp.call_tool("run_python", {"script": "print('hello')"})
    assert res1.isError is True
    assert "Mock connection failed" in res1.content[0].text
    print("✓ run_python handles errors correctly (SEP-1303)")
    
    # Test search_schemas
    res2 = await mcp.call_tool("search_schemas", {"query": "revenue"})
    assert res2.isError is True
    assert "Mock connection failed" in res2.content[0].text
    print("✓ search_schemas handles errors correctly (SEP-1303)")

if __name__ == "__main__":
    print("Running mcp error handling tests...\n")
    asyncio.run(test_mcp_errors())
    print("\nAll tests passed!")
