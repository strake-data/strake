"""Module for managing cross-source join key discovery and registration in Strake.

This module provides a LanceDB-backed registry for manual/user-defined joins,
introspects live database schemas (Postgres, MySQL, Snowflake) to extract foreign
keys, and implements a fuzzy matching engine based on column name tokenization,
Levenshtein distance, and Jaccard similarity.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any
import lancedb
import pyarrow as pa
import yaml

from strake.utils import get_strake_dir, sanitize_identifier, sanitize_lance_value

logger = logging.getLogger("strake.joins")


def quote_identifier(name: str) -> str:
    """Quotes SQL identifiers defensively to prevent SQL injection and quoting errors.

    If the identifier is dotted (multipart FQN), quotes each component individually.
    Note: Source names must not contain dots; dots are treated as delimiters.

    Args:
        name: The SQL identifier (e.g. source, schema, or table name) to quote.

    Returns:
        The double-quoted SQL identifier.
    """
    return ".".join(f'"{part.replace('"', '""')}"' for part in name.split("."))


def redact_url(url: str) -> str:
    """Redacts the password from a database connection URL.

    Args:
        url: The database connection URL string.

    Returns:
        The redacted connection URL with password replaced by ***.
    """
    try:
        return re.sub(r":([^@/]+)@", ":***@", url, count=1)
    except Exception:
        return url


def _get_join_registry_table() -> lancedb.table.Table:
    """Retrieves or initializes the join_registry LanceDB table.

    Returns:
        The join_registry table object.

    Raises:
        OSError: If database cache directory creation fails.
    """
    db_path = str(get_strake_dir("lancedb_cache"))
    os.makedirs(db_path, exist_ok=True)
    db = lancedb.connect(db_path)
    table_name = "join_registry"

    schema = pa.schema(
        [
            pa.field("left_table", pa.string()),
            pa.field("left_column", pa.string()),
            pa.field("right_table", pa.string()),
            pa.field("right_column", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("cardinality", pa.string()),
            pa.field("source", pa.string()),  # "user", "fk", "name"
        ]
    )

    try:
        return db.open_table(table_name)
    except Exception:
        return db.create_table(table_name, schema=schema, data=[], mode="create")


def register_join(
    conn: Any,
    left_table: str | None = None,
    left_column: str | None = None,
    right_table: str | None = None,
    right_column: str | None = None,
    cardinality: str = "1:N",
    config_path: str | None = None,
) -> None:
    """Registers a join candidate to the catalog database (LanceDB).

    Can register a single entry programmatically or bulk load from a YAML
    configuration file.

    Args:
        conn: The active database connection reference.
        left_table: The fully qualified name of the left table.
        left_column: The name of the left join column.
        right_table: The fully qualified name of the right table.
        right_column: The name of the right join column.
        cardinality: The cardinality of the join relationship ("1:1", "1:N", etc.).
        config_path: The filesystem path to a YAML configuration file for bulk load.

    Raises:
        FileNotFoundError: If the config file path is not found.
        ValueError: If config parsing fails or required fields are missing.
    """
    table = _get_join_registry_table()

    if config_path:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            try:
                data = yaml.safe_load(f)
            except Exception as e:
                raise ValueError(f"Failed to parse YAML file {config_path}: {e}")

            if not isinstance(data, dict) or "joins" not in data:
                raise ValueError(
                    "Invalid joins configuration file format. Root must contain 'joins' key."
                )

            joins_list = data["joins"]
            if not isinstance(joins_list, list):
                raise ValueError("'joins' key must be a list of join mappings.")

            records = []
            delete_clauses = []
            for idx, item in enumerate(joins_list):
                l_table = item.get("left_table")
                l_col = item.get("left_column")
                r_table = item.get("right_table")
                r_col = item.get("right_column")
                card = item.get("cardinality", "1:N")

                if not (l_table and l_col and r_table and r_col):
                    raise ValueError(
                        f"Missing required keys in join entry at index {idx}: {item}"
                    )

                records.append(
                    {
                        "left_table": str(l_table),
                        "left_column": str(l_col),
                        "right_table": str(r_table),
                        "right_column": str(r_col),
                        "confidence": 1.0,
                        "cardinality": str(card),
                        "source": "user",
                    }
                )

                lt = sanitize_identifier(str(l_table))
                lc = sanitize_identifier(str(l_col))
                rt = sanitize_identifier(str(r_table))
                rc = sanitize_identifier(str(r_col))
                delete_clauses.append(
                    f"((left_table = '{lt}' and left_column = '{lc}' and right_table = '{rt}' and right_column = '{rc}') or "
                    f"(left_table = '{rt}' and left_column = '{rc}' and right_table = '{lt}' and right_column = '{lc}'))"
                )

            if delete_clauses:
                try:
                    table.delete(" or ".join(delete_clauses))
                except Exception as e:
                    logger.debug(f"Failed to delete existing join records: {e}")

            if records:
                table.add(records)
            return

    # Programmatic registration
    if not (left_table and left_column and right_table and right_column):
        raise ValueError(
            "Must provide left_table, left_column, right_table, and right_column when config_path is None."
        )

    lt = sanitize_identifier(left_table)
    lc = sanitize_identifier(left_column)
    rt = sanitize_identifier(right_table)
    rc = sanitize_identifier(right_column)
    where_clause = (
        f"(left_table = '{lt}' and left_column = '{lc}' and right_table = '{rt}' and right_column = '{rc}') or "
        f"(left_table = '{rt}' and left_column = '{rc}' and right_table = '{lt}' and right_column = '{lc}')"
    )
    try:
        table.delete(where_clause)
    except Exception as e:
        logger.debug(f"Failed to delete existing join record: {e}")

    table.add(
        [
            {
                "left_table": left_table,
                "left_column": left_column,
                "right_table": right_table,
                "right_column": right_column,
                "confidence": 1.0,
                "cardinality": cardinality,
                "source": "user",
            }
        ]
    )


def compatible_types(type_a: str, type_b: str) -> bool:
    """Checks if two Arrow/SQL data types are compatible for a join relationship.

    Args:
        type_a: The left column type string.
        type_b: The right column type string.

    Returns:
        True if the data types are compatible, False otherwise.
    """
    ta = type_a.lower()
    tb = type_b.lower()

    numeric_patterns = (
        "int",
        "decimal",
        "float",
        "double",
        "real",
        "numeric",
        "number",
    )
    is_a_numeric = any(p in ta for p in numeric_patterns)
    is_b_numeric = any(p in tb for p in numeric_patterns)
    if is_a_numeric and is_b_numeric:
        return True

    string_patterns = ("varchar", "char", "text", "string")
    is_a_string = any(p in ta for p in string_patterns)
    is_b_string = any(p in tb for p in string_patterns)
    if is_a_string and is_b_string:
        return True

    temporal_patterns = ("date", "time", "timestamp")
    is_a_temp = any(p in ta for p in temporal_patterns)
    is_b_temp = any(p in tb for p in temporal_patterns)
    if is_a_temp and is_b_temp:
        return True

    is_a_bool = "bool" in ta
    is_b_bool = "bool" in tb
    if is_a_bool and is_b_bool:
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

    previous_row = range(len(s2) + 1)
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
    tokens = []
    for part in parts:
        subparts = re.findall(r"[A-Za-z][a-z]*", part)
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
        db = lancedb.connect(db_path)
        # Avoid lazy-load list_tables() bug; directly open and catch potential exceptions
        table = db.open_table("table_schemas")
        escaped_name = sanitize_identifier(table_name)
        rows = (
            table.search()
            .where(f"table_id = '{escaped_name}'")
            .select(["column_name", "data_type"])
            .limit(1000)
            .to_list()
        )
        return [{"name": r["column_name"], "type": r["data_type"]} for r in rows]
    except Exception as e:
        logger.debug(f"Failed to retrieve columns for {table_name} from cache: {e}")
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

    interesting_patterns = ("id", "key", "fk", "ref", "code", "num", "idx")
    ranked_cols = []
    for col in columns:
        name_lower = col["name"].lower()
        score = 0
        if name_lower == "id":
            score += 10
        elif any(pat in name_lower for pat in interesting_patterns):
            score += 5
        ranked_cols.append((score, col))

    ranked_cols.sort(key=lambda x: x[0], reverse=True)
    return [col for _, col in ranked_cols[:50]]


def join_hints(
    conn: Any,
    left_fqn: str,
    right_fqn: str,
    enable_fuzzy: bool | None = None,
) -> list[dict[str, Any]]:
    """Returns candidate join paths between two tables, sorted by confidence descending.

    Args:
        conn: The active database connection reference.
        left_fqn: Fully qualified name of the left table.
        right_fqn: Fully qualified name of the right table.
        enable_fuzzy: Whether to perform fuzzy name-type column comparisons.
            Falls back to env var STRAKE_ENABLE_FUZZY_JOINS if None.

    Returns:
        A list of join candidate dictionaries with left/right column mappings,
        confidence scores, cardinality, and basis info.
    """
    # 1. Fetch catalog stored candidates (User defined & FK constraints)
    db_candidates = []
    table = _get_join_registry_table()

    l_fqn = sanitize_identifier(left_fqn)
    r_fqn = sanitize_identifier(right_fqn)
    where_clause = f"(left_table = '{l_fqn}' and right_table = '{r_fqn}') or (left_table = '{r_fqn}' and right_table = '{l_fqn}')"

    try:
        rows = table.search().where(where_clause).to_list()
    except Exception as e:
        logger.warning(f"Error querying join registry: {e}")
        rows = []

    for r in rows:
        is_swapped = r["left_table"] == right_fqn
        if is_swapped:
            left_col = r["right_column"]
            right_col = r["left_column"]
            card = r["cardinality"]
            if card == "1:N":
                card = "N:1"
            elif card == "N:1":
                card = "1:N"
        else:
            left_col = r["left_column"]
            right_col = r["right_column"]
            card = r["cardinality"]

        basis_map = {"user": "user_defined", "fk": "fk_metadata"}
        basis = basis_map.get(r["source"], r["source"])

        db_candidates.append(
            {
                "left": left_col,
                "right": right_col,
                "confidence": float(r["confidence"]),
                "cardinality": card,
                "basis": basis,
            }
        )

    # 2. Check feature flag for fuzzy matching
    fuzzy_candidates = []
    if enable_fuzzy is None:
        enable_fuzzy = os.environ.get("STRAKE_ENABLE_FUZZY_JOINS", "").lower() in (
            "1",
            "true",
            "yes",
        )

    if enable_fuzzy:
        left_cols_raw = get_table_columns_from_cache(left_fqn)
        right_cols_raw = get_table_columns_from_cache(right_fqn)

        left_cols = get_top_columns(left_cols_raw)
        right_cols = get_top_columns(right_cols_raw)

        for lc in left_cols:
            for rc in right_cols:
                if compatible_types(lc["type"], rc["type"]):
                    score = name_score(lc["name"], rc["name"])
                    if score > 0.1:
                        confidence = 0.5 + (score * 0.35)
                        confidence = min(confidence, 0.85)
                        fuzzy_candidates.append(
                            {
                                "left": lc["name"],
                                "right": rc["name"],
                                "confidence": float(confidence),
                                "cardinality": "1:N",
                                "basis": "name_type",
                            }
                        )

    # 3. Deduplicate and merge candidates (Option B)
    candidate_map = {}

    for r in db_candidates:
        key = (r["left"], r["right"])
        if key not in candidate_map:
            candidate_map[key] = {
                "left": r["left"],
                "right": r["right"],
                "confidence": r["confidence"],
                "cardinality": r["cardinality"],
                "basis": r["basis"],
                "sources": {r["basis"]},
            }
        else:
            existing = candidate_map[key]
            existing["sources"].add(r["basis"])
            if r["confidence"] > existing["confidence"]:
                existing["confidence"] = r["confidence"]
                existing["cardinality"] = r["cardinality"]

    for r in fuzzy_candidates:
        key = (r["left"], r["right"])
        if key not in candidate_map:
            candidate_map[key] = {
                "left": r["left"],
                "right": r["right"],
                "confidence": r["confidence"],
                "cardinality": r["cardinality"],
                "basis": r["basis"],
                "sources": {r["basis"]},
            }
        else:
            existing = candidate_map[key]
            existing["sources"].add(r["basis"])
            if r["confidence"] > existing["confidence"]:
                existing["confidence"] = r["confidence"]
                existing["cardinality"] = r["cardinality"]

    final_candidates = []
    for existing in candidate_map.values():
        sources = existing.pop("sources")
        if len(sources) > 1:
            existing["basis"] = "mixed"
        else:
            existing["basis"] = list(sources)[0]
        final_candidates.append(existing)

    final_candidates.sort(key=lambda x: x["confidence"], reverse=True)
    return final_candidates


def sync_db_foreign_keys(conn: Any) -> None:
    """Query information_schema/pg_catalog from active SQL sources to extract foreign keys.

    Updates the registry database dynamically on schema sync.

    Args:
        conn: The active database connection reference.
    """
    if conn is None:
        return

    try:
        sources_json = conn.list_sources()
        sources = json.loads(sources_json)
    except Exception as e:
        logger.warning(f"Failed to list sources for FK sync: {e}")
        return

    table = _get_join_registry_table()

    for s in sources:
        name = s.get("name")
        if not name:
            continue

        stype = s.get("type", "").lower()
        dialect = ""
        if stype == "sql" or stype == "jdbc":
            dialect = s.get("config", {}).get("dialect", "").lower()
            if not dialect:
                url = s.get("url", "").lower()
                if "postgres" in url:
                    dialect = "postgres"
                elif "mysql" in url:
                    dialect = "mysql"
                elif "snowflake" in url:
                    dialect = "snowflake"
                elif "sqlite" in url:
                    dialect = "sqlite"
        else:
            dialect = stype

        if dialect not in ("postgres", "mysql", "snowflake"):
            continue

        logger.debug(f"Syncing foreign keys for source '{name}' (dialect: {dialect})")

        # Delete old FK records for this source before attempting to fetch new ones
        escaped_src = sanitize_identifier(name)
        where_clause = f"source = 'fk' and (left_table LIKE '{escaped_src}.%' or right_table LIKE '{escaped_src}.%')"
        try:
            table.delete(where_clause)
        except Exception as e:
            logger.debug(f"Failed to delete old FKs for {name}: {e}")

        # Fetch and sync records for this source
        new_records = []
        try:
            if dialect == "postgres":
                try:
                    import psycopg2

                    db_url = s.get("url") or ""
                    if db_url.startswith("postgres://"):
                        db_url = db_url.replace("postgres://", "postgresql://", 1)

                    logger.debug(
                        f"Connecting directly to Postgres at {redact_url(db_url)} for FK sync"
                    )
                    with psycopg2.connect(db_url) as direct_conn:
                        with direct_conn.cursor() as direct_cursor:
                            fk_query = """
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
                            pk_query = """
                                SELECT 
                                    kcu.table_schema AS schema_name,
                                    kcu.table_name,
                                    kcu.column_name
                                FROM information_schema.table_constraints tc
                                JOIN information_schema.key_column_usage kcu
                                  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                                WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                            """
                            direct_cursor.execute(fk_query)
                            fk_cols = [desc[0] for desc in direct_cursor.description]
                            fk_data = [
                                dict(zip(fk_cols, row))
                                for row in direct_cursor.fetchall()
                            ]

                            direct_cursor.execute(pk_query)
                            pk_cols = [desc[0] for desc in direct_cursor.description]
                            pk_data = [
                                dict(zip(pk_cols, row))
                                for row in direct_cursor.fetchall()
                            ]

                    unique_cols = set()
                    for r in pk_data:
                        s_name = r.get("schema_name")
                        t_name = r.get("table_name")
                        c_name = r.get("column_name")
                        if s_name and t_name and c_name:
                            unique_cols.add(
                                (s_name.lower(), t_name.lower(), c_name.lower())
                            )

                    for r in fk_data:
                        c_schema = r.get("child_schema")
                        c_table = r.get("child_table")
                        c_column = r.get("child_column")
                        p_schema = r.get("parent_schema")
                        p_table = r.get("parent_table")
                        p_column = r.get("parent_column")

                        if not (
                            c_schema
                            and c_table
                            and c_column
                            and p_schema
                            and p_table
                            and p_column
                        ):
                            continue

                        left_fqn = f"{name}.{p_schema}.{p_table}"
                        right_fqn = f"{name}.{c_schema}.{c_table}"

                        is_child_unique = (
                            c_schema.lower(),
                            c_table.lower(),
                            c_column.lower(),
                        ) in unique_cols
                        is_parent_unique = (
                            p_schema.lower(),
                            p_table.lower(),
                            p_column.lower(),
                        ) in unique_cols

                        cardinality = (
                            "1:1" if (is_child_unique and is_parent_unique) else "1:N"
                        )

                        new_records.append(
                            {
                                "left_table": left_fqn,
                                "left_column": p_column,
                                "right_table": right_fqn,
                                "right_column": c_column,
                                "confidence": 0.95,
                                "cardinality": cardinality,
                                "source": "fk",
                            }
                        )

                    if new_records:
                        table.add(new_records)
                    continue
                except Exception as direct_err:
                    logger.warning(
                        f"Direct connection to Postgres failed, falling back to federated query: {direct_err}"
                    )

            q_name = quote_identifier(name)
            if dialect == "postgres":
                fk_query = f"""
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
                pk_query = f"""
                    SELECT 
                        kcu.table_schema AS schema_name,
                        kcu.table_name,
                        kcu.column_name
                    FROM {q_name}.information_schema.table_constraints tc
                    JOIN {q_name}.information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                """
            elif dialect == "mysql":
                fk_query = f"""
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
                pk_query = f"""
                    SELECT 
                        kcu.TABLE_SCHEMA AS schema_name,
                        kcu.TABLE_NAME as table_name,
                        kcu.COLUMN_NAME as column_name
                    FROM {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    JOIN {q_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
                """
            elif dialect == "snowflake":
                fk_query = f"""
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
                pk_query = f"""
                    SELECT 
                        kcu.table_schema AS schema_name,
                        kcu.table_name,
                        kcu.column_name
                    FROM {q_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN {q_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                """
            else:
                continue

            # Run FK query
            fk_table = conn.sql(fk_query)
            fk_data = fk_table.to_pylist()

            # Run PK/Unique query
            pk_table = conn.sql(pk_query)
            pk_data = pk_table.to_pylist()

            # Build unique lookup set
            unique_cols = set()
            for r in pk_data:
                s_name = r.get("schema_name") or r.get("SCHEMA_NAME")
                t_name = r.get("table_name") or r.get("TABLE_NAME")
                c_name = r.get("column_name") or r.get("COLUMN_NAME")
                if s_name and t_name and c_name:
                    unique_cols.add((s_name.lower(), t_name.lower(), c_name.lower()))

            # Process FK entries
            for r in fk_data:
                c_schema = r.get("child_schema") or r.get("CHILD_SCHEMA")
                c_table = r.get("child_table") or r.get("CHILD_TABLE")
                c_column = r.get("child_column") or r.get("CHILD_COLUMN")
                p_schema = r.get("parent_schema") or r.get("PARENT_SCHEMA")
                p_table = r.get("parent_table") or r.get("PARENT_TABLE")
                p_column = r.get("parent_column") or r.get("PARENT_COLUMN")

                if not (
                    c_schema
                    and c_table
                    and c_column
                    and p_schema
                    and p_table
                    and p_column
                ):
                    continue

                # Format FQNs
                left_fqn = f"{name}.{p_schema}.{p_table}"
                right_fqn = f"{name}.{c_schema}.{c_table}"

                is_child_unique = (
                    c_schema.lower(),
                    c_table.lower(),
                    c_column.lower(),
                ) in unique_cols
                is_parent_unique = (
                    p_schema.lower(),
                    p_table.lower(),
                    p_column.lower(),
                ) in unique_cols

                cardinality = "1:1" if (is_child_unique and is_parent_unique) else "1:N"

                new_records.append(
                    {
                        "left_table": left_fqn,
                        "left_column": p_column,
                        "right_table": right_fqn,
                        "right_column": c_column,
                        "confidence": 0.95,
                        "cardinality": cardinality,
                        "source": "fk",
                    }
                )

            if new_records:
                table.add(new_records)
        except Exception as e:
            logger.warning(
                f"Failed to query FK metadata for source '{name}': {e}",
                exc_info=True,
            )
