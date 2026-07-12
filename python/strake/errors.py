"""Exception hierarchy and error types for Strake.

Defines the base StrakeException and subclasses for connection, query,
configuration, authorization, and internal errors.
"""

from strake import (
    StrakeException,
    ConnectionError,
    QueryError,
    ConfigError,
    AuthError,
    InternalError,
)

__all__ = [
    "StrakeException",
    "ConnectionError",
    "QueryError",
    "ConfigError",
    "AuthError",
    "InternalError",
]
