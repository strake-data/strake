from typing import Optional, Dict, Any
import pyarrow

class StrakeConnection:
    """
    A connection to the Strake federation engine.
    Supports both embedded execution (local engine) and remote Flight SQL execution.
    """
    def __init__(self, dsn_or_config: str, api_key: Optional[str] = None) -> None:
        """
        Initialize a new connection.
        :param dsn_or_config: Connection string (grpc://...) or path to config file (embedded).
        :param api_key: Optional API key for remote connections.
        """
        ...

    def sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> pyarrow.Table:
        """
        Execute a SQL query and return results as a PyArrow Table.
        :param query: SQL query string.
        :param params: Optional dictionnary of parameters.
        :return: pyarrow.Table
        """
        ...
    
    def trace(self, query: str) -> str:
        """
        Returns the logical plan of the query without executing it.
        :param query: SQL query to trace.
        :return: String containing the execution plan.
        """
        ...
   
class StrakeConnection:
    def __init__(self, dsn_or_config: str, api_key: str | None = None) -> None: ...
    
    def sql(self, query: str, params: dict[str, Any] | None = None) -> "pyarrow.Table": ...
    
    def trace(self, query: str) -> str: ...
    
    def describe(self, table_name: str | None = None) -> str: ...
    
    def __enter__(self) -> "StrakeConnection": ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...
