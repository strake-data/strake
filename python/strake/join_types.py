"""Type definitions, protocols, enums, and dataclasses for Strake join discovery.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from strake.utils import sanitize_identifier


class SupportsSql(Protocol):
    """Protocol for active database connections capable of SQL execution and source listing."""

    def sql(self, query: str) -> Any: ...
    def list_sources(self) -> str: ...


class JoinBasis(str, Enum):
    """Origin basis for candidate join relationships."""

    USER_DEFINED = "user_defined"
    FK_METADATA = "fk_metadata"
    NAME_TYPE = "name_type"
    MIXED = "mixed"


class Cardinality(str, Enum):
    """Cardinality classification of table relationships."""

    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"


@dataclass(frozen=True)
class FQN:
    """Structured Fully Qualified Name container with validation."""

    source: str
    schema: str
    table: str

    @classmethod
    def parse(cls, raw: str) -> FQN:
        """Parses and validates a raw dot-separated SQL table identifier.

        Args:
            raw: The raw dot-separated name string (e.g. 'table', 'source.table', 'source.schema.table').

        Returns:
            An FQN instance.

        Raises:
            ValueError: If the identifier has 0 or more than 3 dot-delimited parts.
        """
        escaped = sanitize_identifier(raw)
        parts = [p.strip() for p in escaped.split(".") if p.strip()]
        if not parts or len(parts) > 3:
            raise ValueError(f"Invalid identifier or unsupported FQN format: {raw}")

        if len(parts) == 1:
            return cls(source="", schema="", table=parts[0])
        if len(parts) == 2:
            return cls(source=parts[0], schema="", table=parts[1])
        return cls(source=parts[0], schema=parts[1], table=parts[2])

    @property
    def full_name(self) -> str:
        """Returns the canonical dot-joined name string."""
        return ".".join(p for p in (self.source, self.schema, self.table) if p)


@dataclass(frozen=True)
class ColumnMetadata:
    """Column schema information retrieved from indexer cache."""

    name: str
    type: str

    def to_dict(self) -> dict[str, str]:
        """Converts column metadata to dictionary format."""
        return {"name": self.name, "type": self.type}


@dataclass(frozen=True)
class JoinCandidate:
    """Represents a join path candidate between two tables."""

    left: str
    right: str
    confidence: float
    cardinality: Cardinality
    basis: JoinBasis

    def to_dict(self) -> dict[str, Any]:
        """Converts join candidate to dictionary representation."""
        return {
            "left": self.left,
            "right": self.right,
            "confidence": self.confidence,
            "cardinality": self.cardinality.value,
            "basis": self.basis.value,
        }


@dataclass(frozen=True)
class JoinRecord:
    """LanceDB record entry for join catalog storage."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str
    confidence: float
    cardinality: Cardinality
    source: str

    def to_dict(self) -> dict[str, Any]:
        """Converts join record to dictionary payload."""
        return {
            "left_table": self.left_table,
            "left_column": self.left_column,
            "right_table": self.right_table,
            "right_column": self.right_column,
            "confidence": self.confidence,
            "cardinality": self.cardinality.value,
            "source": self.source,
        }
