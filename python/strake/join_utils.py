"""Utility functions, SQL query templates, and matching algorithms for Strake join discovery.
"""

from __future__ import annotations

import contextlib
from difflib import SequenceMatcher
import logging
import os
import re
from typing import Any
from urllib.parse import urlparse, urlunparse

import lancedb
import pyarrow as pa

from strake.join_types import (
    Cardinality,
    ColumnMetadata,
    FQN,
    JoinBasis,
    JoinCandidate,
    JoinRecord,
    SupportsSql,
)
from strake.utils import get_strake_dir, sanitize_identifier, sanitize_lance_value

logger = logging.getLogger("strake.joins")

__all__ = [
    "compatible_types",
    "levenshtein_distance",
    "levenshtein_ratio",
    "tokenize_name",
    "jaccard_similarity",
    "name_score",
    "get_table_columns_from_cache",
    "get_top_columns",
    "_compute_fuzzy_candidates",
    "_extract_postgres_direct_fks",
    "_extract_federated_fks",
    "redact_url",
    "SUPPORTED_SQL_DIALECTS",
    "POSTGRES_DIRECT_FK_QUERY",
    "POSTGRES_DIRECT_PK_QUERY",
    "POSTGRES_FEDERATED_FK_QUERY_TEMPLATE",
    "MYSQL_FEDERATED_FK_QUERY_TEMPLATE",
    "SNOWFLAKE_FEDERATED_FK_QUERY_TEMPLATE",
]

SUPPORTED_SQL_DIALECTS: tuple[str, ...] = ("postgres", "mysql", "snowflake")
EXACT_ID_SCORE: int = 10
HEURISTIC_PATTERN_SCORE: int = 5

JOIN_KEY_HEURISTIC_PATTERNS: tuple[str, ...] = (
    "id",
    "key",
    "fk",
    "ref",
    "code",
    "num",
    "idx",
)

RE_CAMEL_CASE: re.Pattern[str] = re.compile(r"[A-Za-z][a-z]*")

POSTGRES_DIRECT_FK_QUERY = """
    SELECT 
        kcu.table_schema AS child_schema,
        kcu.table_name AS child_table,
        kcu.column_name AS child_column,
        ccu.table_schema AS parent_schema,
        ccu.table_name AS parent_table,
        ccu.column_name AS parent_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    JOIN information_schema.referential_constraints rc
      ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
    JOIN information_schema.constraint_column_usage ccu
      ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
"""

POSTGRES_DIRECT_PK_QUERY = """
    SELECT 
        kcu.table_schema AS schema_name,
        kcu.table_name,
        kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
"""

POSTGRES_FEDERATED_FK_QUERY_TEMPLATE = """
    SELECT 
        kcu.table_schema AS child_schema,
        kcu.table_name AS child_table,
        kcu.column_name AS child_column,
        ccu.table_schema AS parent_schema,
        ccu.table_name AS parent_table,
        ccu.column_name AS parent_column
    FROM {q_name}.information_schema.table_constraints tc
    JOIN {q_name}.information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    JOIN {q_name}.information_schema.referential_constraints rc
      ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
    JOIN {q_name}.information_schema.constraint_column_usage ccu
      ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
"""

POSTGRES_FEDERATED_PK_QUERY_TEMPLATE = """
    SELECT 
        kcu.table_schema AS schema_name,
        kcu.table_name,
        kcu.column_name
    FROM {q_name}.information_schema.table_constraints tc
    JOIN {q_name}.information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
"""

MYSQL_FEDERATED_FK_QUERY_TEMPLATE = """
    SELECT 
        TABLE_SCHEMA AS child_schema,
        TABLE_NAME AS child_table,
        COLUMN_NAME AS child_column,
        REFERENCED_TABLE_SCHEMA AS parent_schema,
        REFERENCED_TABLE_NAME AS parent_table,
        REFERENCED_COLUMN_NAME AS parent_column
    FROM {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE REFERENCED_TABLE_NAME IS NOT NULL
"""

MYSQL_FEDERATED_PK_QUERY_TEMPLATE = """
    SELECT 
        kcu.TABLE_SCHEMA AS schema_name,
        kcu.TABLE_NAME as table_name,
        kcu.COLUMN_NAME as column_name
    FROM {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    JOIN {q_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
"""

SNOWFLAKE_FEDERATED_FK_QUERY_TEMPLATE = """
    SELECT 
        kcu1.table_schema AS child_schema,
        kcu1.table_name AS child_table,
        kcu1.column_name AS child_column,
        kcu2.table_schema AS parent_schema,
        kcu2.table_name AS parent_table,
        kcu2.column_name AS parent_column
    FROM {q_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN {q_name}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
      ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
    JOIN {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
      ON tc.constraint_name = kcu1.constraint_name AND tc.table_schema = kcu1.table_schema
    JOIN {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
      ON rc.unique_constraint_name = kcu2.constraint_name AND rc.unique_constraint_schema = kcu2.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
"""

SNOWFLAKE_FEDERATED_PK_QUERY_TEMPLATE = """
    SELECT 
        kcu.table_schema AS schema_name,
        kcu.table_name,
        kcu.column_name
    FROM {q_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
"""

_NUMERIC_TYPES: frozenset[str] = frozenset(
    {
        "int",
        "integer",
        "int2",
        "int4",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "smallint",
        "bigint",
        "decimal",
        "numeric",
        "float",
        "float2",
        "float4",
        "float8",
        "double",
        "real",
        "number",
    }
)

_STRING_TYPES: frozenset[str] = frozenset(
    {"varchar", "char", "text", "string"}
)

_TEMPORAL_TYPES: frozenset[str] = frozenset(
    {"date", "time", "timestamp", "timestamptz"}
)

_BOOLEAN_TYPES: frozenset[str] = frozenset({"bool", "boolean"})


def redact_url(url: str) -> str:
    """Redacts passwords from connection URLs securely, handling @ in passwords.

    Args:
        url: The database connection URL string.

    Returns:
        The redacted connection URL with password replaced by ***.
    """
    try:
        parsed = urlparse(url)
        if parsed.password:
            netloc = parsed.netloc.replace(f":{parsed.password}@", ":***@", 1)
            return urlunparse(parsed._replace(netloc=netloc))
        return url
    except Exception:
        return re.sub(r":([^@/]+)@", ":***@", url, count=1)


def compatible_types(type_a: str, type_b: str) -> bool:
    """Checks if two Arrow/SQL data types are compatible for a join relationship.

    Args:
        type_a: The left column type string.
        type_b: The right column type string.

    Returns:
        True if the data types are compatible, False otherwise.
    """
    ta = type_a.lower().split("(")[0].strip()
    tb = type_b.lower().split("(")[0].strip()

    if ta in _NUMERIC_TYPES and tb in _NUMERIC_TYPES:
        return True
    if ta in _STRING_TYPES and tb in _STRING_TYPES:
        return True
    if ta in _TEMPORAL_TYPES and tb in _TEMPORAL_TYPES:
        return True
    if ta in _BOOLEAN_TYPES and tb in _BOOLEAN_TYPES:
        return True

    return False


def levenshtein_distance(s1: str, s2: str) -> int:
    """Computes the Levenshtein distance between two strings.

    Args:
        s1: The first string.
        s2: The second string.

    Returns:
        The Levenshtein distance as an integer.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def levenshtein_ratio(s1: str, s2: str) -> float:
    """Computes the Levenshtein ratio between two strings.

    Args:
        s1: The first string.
        s2: The second string.

    Returns:
        The Levenshtein ratio (1.0 - distance / max_len) as a float.
    """
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    dist = levenshtein_distance(s1, s2)
    return 1.0 - (dist / max_len)


def tokenize_name(name: str) -> set[str]:
    """Tokenizes a column name by snake_case and camelCase.

    Args:
        name: The column name string to tokenize.

    Returns:
        A set of lowercased string tokens.
    """
    parts = name.split("_")
    tokens: list[str] = []
    for part in parts:
        subparts = RE_CAMEL_CASE.findall(part)
        if subparts:
            tokens.extend([sp.lower() for sp in subparts])
        elif part:
            tokens.append(part.lower())
    return set(tokens)


def jaccard_similarity(s1: str, s2: str) -> float:
    """Computes the Jaccard similarity of camelCase and snake_case tokens.

    Args:
        s1: The first string.
        s2: The second string.

    Returns:
        The Jaccard similarity coefficient as a float.
    """
    t1 = tokenize_name(s1)
    t2 = tokenize_name(s2)
    if not t1 or not t2:
        return 0.0
    intersection = len(t1.intersection(t2))
    union = len(t1.union(t2))
    return intersection / union


def name_score(left_col: str, right_col: str) -> float:
    """Computes overall column name similarity score using Levenshtein and Jaccard.

    Args:
        left_col: The left column name string.
        right_col: The right column name string.

    Returns:
        The similarity score between 0.0 and 1.0.
    """
    n_left = left_col.lower().replace("_", "")
    n_right = right_col.lower().replace("_", "")

    ratio = levenshtein_ratio(n_left, n_right)
    jacc = jaccard_similarity(left_col, right_col)

    return max(ratio, jacc)


def get_table_columns_from_cache(table_name: str) -> list[dict[str, str]]:
    """Retrieves column names and types from schema indexer cache.

    Args:
        table_name: The fully qualified name of the table.

    Returns:
        A list of dictionaries containing column name and type.
    """
    db_path = str(get_strake_dir("lancedb_cache"))
    if not os.path.exists(db_path):
        return []
    try:
        fqn = FQN.parse(table_name)
    except ValueError as e:
        logger.warning("Invalid FQN format for cache lookup '%s': %s", table_name, e)
        return []

    try:
        db = lancedb.connect(db_path)
        table = db.open_table("table_schemas")
        escaped_name = fqn.full_name

        src = fqn.source
        tbl = fqn.table

        if not src and not fqn.schema:
            where_clause = (
                f"table_id = '{escaped_name}' or table_id LIKE '%.{escaped_name}'"
            )
        elif src and not fqn.schema:
            where_clause = (
                f"table_id = '{escaped_name}' or table_id = '{tbl}' or table_id LIKE '{src}.%.{tbl}'"
            )
        else:
            where_clause = (
                f"table_id = '{escaped_name}' or table_id = '{src}.{tbl}' or table_id = '{tbl}'"
            )

        rows = (
            table.search()
            .where(where_clause)
            .limit(1000)
            .to_list()
        )

        matching_rows = [r for r in rows if r.get("table_id") == escaped_name]
        if not matching_rows and src:
            matching_rows = [
                r
                for r in rows
                if r.get("table_id", "").startswith(f"{src}.")
                and r.get("table_id", "").endswith(f".{tbl}")
            ]
        if not matching_rows:
            matching_rows = [
                r
                for r in rows
                if r.get("table_id") == tbl
                or r.get("table_id", "").endswith(f".{tbl}")
            ]

        return [
            ColumnMetadata(name=r["column_name"], type=r["data_type"]).to_dict()
            for r in matching_rows
            if "column_name" in r and "data_type" in r
        ]
    except Exception as e:
        logger.debug("Failed to retrieve columns for %s from cache: %s", table_name, e)
        return []


def get_top_columns(columns: list[dict[str, str]]) -> list[dict[str, str]]:
    """Filters/sorts columns to keep only the top 50 most likely join keys.

    Args:
        columns: The full list of column metadata dictionaries.

    Returns:
        The filtered/sorted top 50 column metadata dictionaries.
    """
    if len(columns) <= 50:
        return columns

    ranked_cols = []
    for col in columns:
        name_lower = col["name"].lower()
        score = 0
        if name_lower == "id":
            score += EXACT_ID_SCORE
        elif any(pat in name_lower for pat in JOIN_KEY_HEURISTIC_PATTERNS):
            score += HEURISTIC_PATTERN_SCORE
        ranked_cols.append((score, col))

    ranked_cols.sort(key=lambda x: x[0], reverse=True)
    return [col for _, col in ranked_cols[:50]]


def _compute_fuzzy_candidates(
    left_fqn: str, right_fqn: str
) -> list[dict[str, Any]]:
    """Computes name-type fuzzy matching join candidates.

    Args:
        left_fqn: Left table fully qualified name.
        right_fqn: Right table fully qualified name.

    Returns:
        List of fuzzy candidate dictionaries.
    """
    fuzzy_candidates = []
    left_cols_raw = get_table_columns_from_cache(left_fqn)
    right_cols_raw = get_table_columns_from_cache(right_fqn)

    left_cols = get_top_columns(left_cols_raw)
    right_cols = get_top_columns(right_cols_raw)

    for lc in left_cols:
        for rc in right_cols:
            if compatible_types(lc["type"], rc["type"]):
                score = name_score(lc["name"], rc["name"])
                if score > 0.1:
                    confidence = min(0.5 + (score * 0.35), 0.85)
                    fuzzy_candidates.append(
                        JoinCandidate(
                            left=lc["name"],
                            right=rc["name"],
                            confidence=float(confidence),
                            cardinality=Cardinality.ONE_TO_MANY,
                            basis=JoinBasis.NAME_TYPE,
                        ).to_dict()
                    )
    return fuzzy_candidates


def _extract_postgres_direct_fks(s: dict[str, Any], name: str) -> list[dict[str, Any]]:
    """Introspects foreign keys directly from PostgreSQL using psycopg2.

    Args:
        s: Source configuration dictionary.
        name: Source registration name.

    Returns:
        List of foreign key join record dictionaries.
    """
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 module not installed; skipping direct Postgres introspection")
        return []

    db_url = s.get("url") or ""
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    logger.debug(
        "Connecting directly to Postgres at %s for FK sync", redact_url(db_url)
    )
    with contextlib.closing(psycopg2.connect(db_url)) as direct_conn:
        with direct_conn.cursor() as direct_cursor:
            direct_cursor.execute(POSTGRES_DIRECT_FK_QUERY)
            fk_cols = [desc[0] for desc in direct_cursor.description]
            fk_data = [
                dict(zip(fk_cols, row)) for row in direct_cursor.fetchall()
            ]

            direct_cursor.execute(POSTGRES_DIRECT_PK_QUERY)
            pk_cols = [desc[0] for desc in direct_cursor.description]
            pk_data = [
                dict(zip(pk_cols, row)) for row in direct_cursor.fetchall()
            ]

    unique_cols = {
        (
            r["schema_name"].lower(),
            r["table_name"].lower(),
            r["column_name"].lower(),
        )
        for r in pk_data
        if r.get("schema_name") and r.get("table_name") and r.get("column_name")
    }

    new_records = []
    for r in fk_data:
        c_schema, c_table, c_column = r.get("child_schema"), r.get("child_table"), r.get("child_column")
        p_schema, p_table, p_column = r.get("parent_schema"), r.get("parent_table"), r.get("parent_column")

        if not (c_schema and c_table and c_column and p_schema and p_table and p_column):
            continue

        left_fqn = f"{name}.{p_schema}.{p_table}"
        right_fqn = f"{name}.{c_schema}.{c_table}"

        is_child_unique = (c_schema.lower(), c_table.lower(), c_column.lower()) in unique_cols
        is_parent_unique = (p_schema.lower(), p_table.lower(), p_column.lower()) in unique_cols
        cardinality = Cardinality.ONE_TO_ONE if (is_child_unique and is_parent_unique) else Cardinality.ONE_TO_MANY

        new_records.append(
            JoinRecord(
                left_table=left_fqn,
                left_column=p_column,
                right_table=right_fqn,
                right_column=c_column,
                confidence=0.95,
                cardinality=cardinality,
                source="fk",
            ).to_dict()
        )
    return new_records


def _extract_federated_fks(
    conn: SupportsSql, name: str, dialect: str
) -> list[dict[str, Any]]:
    """Introspects foreign keys via federated SQL queries through Strake connection.

    Args:
        conn: Connection instance supporting .sql().
        name: Source registration name.
        dialect: Database SQL dialect ("postgres", "mysql", "snowflake").

    Returns:
        List of foreign key join record dictionaries.
    """
    from strake.joins import quote_identifier

    q_name = quote_identifier(name)
    if dialect == "postgres":
        fk_query = POSTGRES_FEDERATED_FK_QUERY_TEMPLATE.format(q_name=q_name)
        pk_query = POSTGRES_FEDERATED_PK_QUERY_TEMPLATE.format(q_name=q_name)
    elif dialect == "mysql":
        fk_query = MYSQL_FEDERATED_FK_QUERY_TEMPLATE.format(q_name=q_name)
        pk_query = MYSQL_FEDERATED_PK_QUERY_TEMPLATE.format(q_name=q_name)
    elif dialect == "snowflake":
        fk_query = SNOWFLAKE_FEDERATED_FK_QUERY_TEMPLATE.format(q_name=q_name)
        pk_query = SNOWFLAKE_FEDERATED_PK_QUERY_TEMPLATE.format(q_name=q_name)
    else:
        return []

    fk_data = conn.sql(fk_query).to_pylist()
    pk_data = conn.sql(pk_query).to_pylist()

    unique_cols = set()
    for r in pk_data:
        s_name = r.get("schema_name") or r.get("SCHEMA_NAME")
        t_name = r.get("table_name") or r.get("TABLE_NAME")
        c_name = r.get("column_name") or r.get("COLUMN_NAME")
        if s_name and t_name and c_name:
            unique_cols.add((s_name.lower(), t_name.lower(), c_name.lower()))

    new_records = []
    for r in fk_data:
        c_schema = r.get("child_schema") or r.get("CHILD_SCHEMA")
        c_table = r.get("child_table") or r.get("CHILD_TABLE")
        c_column = r.get("child_column") or r.get("CHILD_COLUMN")
        p_schema = r.get("parent_schema") or r.get("PARENT_SCHEMA")
        p_table = r.get("parent_table") or r.get("PARENT_TABLE")
        p_column = r.get("parent_column") or r.get("PARENT_COLUMN")

        if not (c_schema and c_table and c_column and p_schema and p_table and p_column):
            continue

        left_fqn = f"{name}.{p_schema}.{p_table}"
        right_fqn = f"{name}.{c_schema}.{c_table}"

        is_child_unique = (c_schema.lower(), c_table.lower(), c_column.lower()) in unique_cols
        is_parent_unique = (p_schema.lower(), p_table.lower(), p_column.lower()) in unique_cols
        cardinality = Cardinality.ONE_TO_ONE if (is_child_unique and is_parent_unique) else Cardinality.ONE_TO_MANY

        new_records.append(
            JoinRecord(
                left_table=left_fqn,
                left_column=p_column,
                right_table=right_fqn,
                right_column=c_column,
                confidence=0.95,
                cardinality=cardinality,
                source="fk",
            ).to_dict()
        )
    return new_records
