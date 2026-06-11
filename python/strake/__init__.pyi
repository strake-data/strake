import pyarrow
from typing import Any

class StrakeConnection:
    """
    A connection to the Strake federation engine.
    Supports both embedded execution (local engine) and remote Flight SQL execution.
    """
    def __init__(
        self,
        dsn_or_config: str,
        sources_config: str | None = None,
        api_key: str | None = None,
    ) -> None:
        """
        Initialize a new connection.

        :param dsn_or_config:
            - Remote mode: Connection URL starting with grpc:// or grpcs://
            - Embedded mode: Path to the main strake.yaml configuration file.
        :param sources_config:
            - Optional path to sources.yaml. If not provided in embedded mode,
              it is looked for in the same directory as dsn_or_config.
        :param api_key: Optional API key for authenticating with remote servers.
        """
        ...

    def sql(self, query: str, params: dict[str, Any] | None = None) -> pyarrow.Table:
        """
        Execute a SQL query and return results as a PyArrow Table.

        :param query: SQL query string.
        :param params: Optional dictionary of parameters (currently not implemented).
        :return: pyarrow.Table containing the query results.
        """
        ...

    def trace(self, query: str) -> str:
        """
        Returns the logical plan of the query without executing it.

        :param query: SQL query to trace.
        :return: String containing the pretty-printed execution plan.
        """
        ...

    def describe(self, table_name: str | None = None) -> str:
        """
        Introspects the engine to list available tables or describe a specific table's schema.

        :param table_name: Optional full name of the table to describe.
                           If None, returns a list of all available tables.
        :return: String containing a pretty-printed table of metadata.
        """
        ...

    def explain_tree(self, query: str) -> str:
        """
        Returns a detailed ASCII tree visualization of the execution plan.
        """
        ...

    def close(self) -> Any:
        """
        Explicitly close the connection and shut down the engine.
        Returns a Coroutine.
        """
        ...

    def __enter__(self) -> "StrakeConnection":
        """Context manager support."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any | None,
    ) -> None:
        """Context manager support."""
        ...

class StrakeException(Exception): ...
class ConnectionError(StrakeException): ...
class QueryError(StrakeException): ...
class ConfigError(StrakeException): ...
class AuthError(StrakeException): ...
class InternalError(StrakeException): ...
