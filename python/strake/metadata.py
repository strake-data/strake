import json
import logging
import re
from typing import Any, Callable, Protocol

logger = logging.getLogger("strake.metadata")


def sanitize_identifier(name: str) -> str:
    """Validate against a simple identifier pattern before interpolating."""
    if all(c.isalnum() or c in ("_", ".") for c in name):
        return name
    raise ValueError(f"Invalid identifier: {name}")


class MetadataEnricher(Protocol):
    """Protocol for fetching metadata descriptions for schemas."""

    def enrich(self, table: str, column: str | None, source: str) -> str | None:
        """Return description string or None if unavailable.
        If column is None, return the table description.

        Args:
            table: Table fully qualified name
            column: Column name (optional)
            source: Source system name

        Returns:
            Description string if available, else None.
        """
        ...


class NullEnricher:
    """Fallback enricher that returns no metadata."""

    def enrich(self, table: str, column: str | None, source: str) -> str | None:
        """Always returns None."""
        return None


class InformationSchemaEnricher:
    """Fetches schema metadata (table/column descriptions) through information_schema or similar paths."""

    def __init__(self, connection: Any, coverage_threshold: float = 0.20):
        """Initializes the enricher with a database connection and an acceptable coverage threshold."""
        self.connection = connection
        self.coverage_threshold = coverage_threshold
        # Cache for (source, table, column) -> description
        # If column is None, it's a table description.
        self._cache: dict[tuple[str, str, str | None], str] = {}
        self._fetched_sources: set[str] = set()
        self._blacklisted_sources: set[str] = set()
        self._source_dialects: dict[str, str] = self._init_dialects()

    def _init_dialects(self) -> dict[str, str]:
        dialects: dict[str, str] = {}
        try:
            if hasattr(self.connection, "list_sources"):

                sources_json = self.connection.list_sources()
                sources = json.loads(sources_json)
                for s in sources:
                    name = s.get("name")
                    if not name:
                        continue
                    stype = s.get("type", "")
                    if stype == "sql":
                        dialects[name] = s.get("config", {}).get("dialect", "").lower()
                    else:
                        dialects[name] = stype.lower()
        except Exception as e:
            logger.debug(f"Failed to initialize source dialects: {e}")
        return dialects

    def enrich(self, table: str, column: str | None, source: str) -> str | None:
        """Enriches the given column/table with descriptions."""
        if source in self._blacklisted_sources:
            return None

        if source not in self._fetched_sources:
            self._fetch_for_source(source)
            self._fetched_sources.add(source)

        return self._cache.get((source, table, column))

    def _fetch_for_source(self, source: str) -> None:
        """Fetches metadata descriptions for all tables/columns in the source."""
        dialect = self._source_dialects.get(source)

        _DIALECT_STRATEGIES: dict[str, Callable] = {
            "postgres": self._try_postgres,
            "snowflake": self._try_snowflake,
            "mysql": self._try_mysql,
            "bigquery": self._try_bigquery,
            "sqlite": self._try_sqlite,
        }

        if dialect and dialect in _DIALECT_STRATEGIES:
            strategies = [_DIALECT_STRATEGIES[dialect]]
        else:
            strategies = list(_DIALECT_STRATEGIES.values())

        for strategy in strategies:
            try:
                descriptions = strategy(source)
                if descriptions is not None:
                    # Check coverage
                    if not self._check_coverage(source, descriptions):
                        logger.warning(
                            f"Source '{source}' returned metadata below coverage threshold. Falling back to NullEnricher for this source."
                        )
                        self._blacklisted_sources.add(source)
                    else:
                        self._cache.update(descriptions)
                    return
            except Exception as e:
                # Query failed, probably not this dialect
                logger.debug(
                    f"Strategy {strategy.__name__} failed for source {source}: {e}"
                )

        # If no strategy worked, blacklist learning
        logger.warning(
            f"No metadata strategy worked for source '{source}'. Falling back to NullEnricher."
        )
        self._blacklisted_sources.add(source)

    def _check_coverage(
        self, source: str, descriptions: dict[tuple[str, str, str | None], str]
    ) -> bool:
        # Check if we got the total_columns value back from the query
        # Because we fetch all columns (with or without description) and store them
        # (val in description tuple will be None if no desc), the total_columns count
        # can also be found just from len() if it's the exact match. Or we can just read total_columns.
        if ("_total_columns", "_total_columns", None) in descriptions:
            total_columns = int(
                descriptions.pop(("_total_columns", "_total_columns", None))
            )
        else:
            # Fallback for SQLite and generic cases
            total_columns = sum(1 for k in descriptions.keys() if k[2] is not None)

        if total_columns == 0:
            return True  # No columns, so technically 100% coverage

        # Count only column descriptions that are not None/empty
        provided_cols = sum(
            1 for k, v in descriptions.items() if k[2] is not None and v
        )
        coverage = provided_cols / total_columns
        return coverage >= self.coverage_threshold

    def _try_postgres(
        self, source: str
    ) -> dict[tuple[str, str, str | None], str] | None:
        # PostgreSQL: pg_catalog.col_description() + obj_description() join
        s_source = sanitize_identifier(source)
        query = f"""
            SELECT n.nspname as schema_name, c.relname as table_name, a.attname as column_name, 
                   pg_catalog.col_description(c.oid, a.attnum) as column_description, 
                   obj_description(c.oid) as table_description,
                   COUNT(*) OVER () as total_columns
            FROM {s_source}.pg_catalog.pg_class c
            JOIN {s_source}.pg_catalog.pg_attribute a ON c.oid = a.attrelid
            JOIN {s_source}.pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE a.attnum > 0
        """
        return self._execute_and_parse(s_source, query)

    def _try_snowflake(
        self, source: str
    ) -> dict[tuple[str, str, str | None], str] | None:
        # Snowflake: COMMENT column in INFORMATION_SCHEMA.COLUMNS and TABLES
        s_source = sanitize_identifier(source)
        query = f"""
            SELECT c.table_schema as schema_name, c.table_name, c.column_name, c.comment as column_description,
                   t.comment as table_description,
                   COUNT(*) OVER () as total_columns
            FROM {s_source}.INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN {s_source}.INFORMATION_SCHEMA.TABLES t 
              ON c.table_schema = t.table_schema AND c.table_name = t.table_name
        """
        return self._execute_and_parse(s_source, query)

    def _try_bigquery(
        self, source: str
    ) -> dict[tuple[str, str, str | None], str] | None:
        # BigQuery: description from INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        s_source = sanitize_identifier(source)
        query = f"""
            SELECT table_schema as schema_name, table_name, column_name, description as column_description,
                   NULL as table_description,
                   COUNT(*) OVER () as total_columns
            FROM {s_source}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        """
        return self._execute_and_parse(s_source, query)

    def _try_mysql(
        self, source: str
    ) -> dict[tuple[str, str, str | None], str] | None:
        # MySQL / MariaDB: COLUMN_COMMENT in information_schema.COLUMNS
        s_source = sanitize_identifier(source)
        query = f"""
            SELECT table_schema as schema_name, table_name, column_name, column_comment as column_description,
                   NULL as table_description,
                   COUNT(*) OVER () as total_columns
            FROM {s_source}.information_schema.COLUMNS
        """
        return self._execute_and_parse(s_source, query)

    def _try_sqlite(
        self, source: str
    ) -> dict[tuple[str, str, str | None], str] | None:
        # SQLite: Parse inline -- comments from CREATE TABLE DDL (best-effort)
        # We query sqlite_master for the DDL, then parse it.
        s_source = sanitize_identifier(source)
        query = f"""
            SELECT name as table_name, sql 
            FROM {s_source}.sqlite_master 
            WHERE type='table' AND sql IS NOT NULL
        """
        result = self.connection.sql(query).to_pydict()

        if not result or len(result.get("table_name", [])) == 0:
            return None  # Query worked but no tables, or failed

        descriptions = {}
        # Count total columns parsed for SQLite coverage metric
        total_cols = 0
        for table_name, sql in zip(result["table_name"], result["sql"]):
            # Parse comments from sql
            lines = sql.split("\n")
            for line in lines:
                # Basic check to count columns roughly (lines with commas) to act as denominator
                if "," in line and not line.strip().startswith("--"):
                    total_cols += 1
                # Match simple inline comments e.g. "col_name INTEGER, -- user age"
                # This is highly best-effort
                match = re.search(r"([A-Za-z0-9_]+)\s+[^,]+,\s*--\s*(.*)", line)
                if match:
                    col_name = match.group(1).strip()
                    comment = match.group(2).strip()
                    full_table_name = table_name  # SQLite might not have schemas
                    descriptions[(s_source, full_table_name, col_name)] = comment

        descriptions[("_total_columns", "_total_columns", None)] = str(
            max(total_cols, sum(1 for k in descriptions.keys() if k[2] is not None))
        )
        return descriptions

    def _execute_and_parse(
        self, source: str, query: str
    ) -> dict[tuple[str, str, str | None], str] | None:
        result = self.connection.sql(query).to_pydict()

        if not result or len(result.get("table_name", [])) == 0:
            return None

        descriptions = {}

        # Save total_columns entry if it exists
        if "total_columns" in result and len(result["total_columns"]) > 0:
            descriptions[("_total_columns", "_total_columns", None)] = str(
                result["total_columns"][0]
            )

        # We assume result has list of column_description and table_description
        n = len(result["table_name"])
        for schema_name, table_name, col_name, col_desc, tbl_desc in zip(
            result.get("schema_name", [None] * n),
            result["table_name"],
            result.get("column_name", [None] * n),
            result.get("column_description", [None] * n),
            result.get("table_description", [None] * n),
        ):
            if schema_name:
                fqn_table = f"{schema_name}.{table_name}"
            else:
                fqn_table = table_name

            # Always store the key so we have accurate count of matched columns
            # (which we need if total_columns isn't provided via query)
            if col_name:
                descriptions[(source, fqn_table, col_name)] = (
                    str(col_desc) if col_desc else ""
                )
            if tbl_desc:
                # Store table desc
                descriptions[(source, fqn_table, None)] = str(tbl_desc)

        return descriptions
