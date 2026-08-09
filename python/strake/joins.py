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

from strake.join_types import (
    Cardinality,
    ColumnMetadata,
    FQN,
    JoinBasis,
    JoinCandidate,
    JoinRecord,
    SupportsSql,
)
from strake.join_utils import (
    SUPPORTED_SQL_DIALECTS,
    _compute_fuzzy_candidates,
    _extract_federated_fks,
    _extract_postgres_direct_fks,
    compatible_types,
    get_table_columns_from_cache,
    get_top_columns,
    jaccard_similarity,
    levenshtein_distance,
    levenshtein_ratio,
    name_score,
    redact_url,
    tokenize_name,
)
from strake.utils import get_strake_dir, sanitize_identifier, sanitize_lance_value

logger = logging.getLogger("strake.joins")

__all__ = [
    "register_join",
    "join_hints",
    "sync_db_foreign_keys",
    "compatible_types",
    "quote_identifier",
    "redact_url",
    "JoinBasis",
    "Cardinality",
    "JoinCandidate",
    "JoinRecord",
    "ColumnMetadata",
    "FQN",
    "SupportsSql",
    "levenshtein_distance",
    "levenshtein_ratio",
    "tokenize_name",
    "jaccard_similarity",
    "name_score",
    "get_table_columns_from_cache",
    "get_top_columns",
]


def quote_identifier(name: str) -> str:
    """Quotes SQL identifiers defensively to prevent SQL injection and quoting errors.

    If the identifier is dotted (multipart FQN), quotes each component individually.
    Note: Source names must not contain dots; dots are treated as delimiters.

    Args:
        name: The SQL identifier (e.g. source, schema, or table name) to quote.

    Returns:
        The double-quoted SQL identifier.
    """
    parts = []
    for part in name.split("."):
        escaped = part.replace('"', '""')
        parts.append(f'"{escaped}"')
    return ".".join(parts)


def _get_join_registry_table() -> lancedb.table.Table:
    """Retrieves or initializes the join_registry LanceDB table.

    Returns:
        The join_registry table object.

    Raises:
        RuntimeError: If opening or initializing the LanceDB table fails.
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
            pa.field("source", pa.string()),
        ]
    )

    try:
        return db.open_table(table_name)
    except Exception as e:
        err_msg = str(e).lower()
        if "not found" in err_msg or "does not exist" in err_msg or "was not found" in err_msg:
            return db.create_table(table_name, schema=schema, data=[], mode="create")
        logger.error("Failed to open join_registry table: %s", e)
        raise RuntimeError(f"Database error opening join_registry: {e}") from e


def register_join(
    conn: SupportsSql | None = None,
    left_table: str | None = None,
    left_column: str | None = None,
    right_table: str | None = None,
    right_column: str | None = None,
    cardinality: str | Cardinality = Cardinality.ONE_TO_MANY,
    config_path: str | None = None,
) -> None:
    """Registers a join candidate to the catalog database (LanceDB).

    Can register a single entry programmatically or bulk load from a YAML
    configuration file.

    Args:
        conn: Reserved for multi-source connection context (optional/unused).
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

        with open(config_path, "r", encoding="utf-8") as f:
            try:
                data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise ValueError(f"Failed to parse YAML file {config_path}: {e}") from e

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
                raw_card = item.get("cardinality", "1:N")
                try:
                    card_enum = Cardinality(raw_card)
                except ValueError:
                    card_enum = Cardinality.ONE_TO_MANY

                if not (l_table and l_col and r_table and r_col):
                    raise ValueError(
                        f"Missing required keys in join entry at index {idx}: {item}"
                    )

                records.append(
                    JoinRecord(
                        left_table=str(l_table),
                        left_column=str(l_col),
                        right_table=str(r_table),
                        right_column=str(r_col),
                        confidence=1.0,
                        cardinality=card_enum,
                        source="user",
                    ).to_dict()
                )

                lt = sanitize_lance_value(sanitize_identifier(str(l_table)))
                lc = sanitize_lance_value(sanitize_identifier(str(l_col)))
                rt = sanitize_lance_value(sanitize_identifier(str(r_table)))
                rc = sanitize_lance_value(sanitize_identifier(str(r_col)))
                delete_clauses.append(
                    f"((left_table = '{lt}' and left_column = '{lc}' and right_table = '{rt}' and right_column = '{rc}') or "
                    f"(left_table = '{rt}' and left_column = '{rc}' and right_table = '{lt}' and right_column = '{lc}'))"
                )

            if delete_clauses:
                try:
                    table.delete(" or ".join(delete_clauses))
                except Exception as e:
                    logger.debug("Failed to delete existing join records: %s", e)

            if records:
                table.add(records)
            return

    # Programmatic registration
    if not (left_table and left_column and right_table and right_column):
        raise ValueError(
            "Must provide left_table, left_column, right_table, and right_column when config_path is None."
        )

    if isinstance(cardinality, Cardinality):
        card_enum = cardinality
    else:
        try:
            card_enum = Cardinality(cardinality)
        except ValueError:
            card_enum = Cardinality.ONE_TO_MANY

    lt = sanitize_lance_value(sanitize_identifier(left_table))
    lc = sanitize_lance_value(sanitize_identifier(left_column))
    rt = sanitize_lance_value(sanitize_identifier(right_table))
    rc = sanitize_lance_value(sanitize_identifier(right_column))
    where_clause = (
        f"(left_table = '{lt}' and left_column = '{lc}' and right_table = '{rt}' and right_column = '{rc}') or "
        f"(left_table = '{rt}' and left_column = '{rc}' and right_table = '{lt}' and right_column = '{lc}')"
    )
    try:
        table.delete(where_clause)
    except Exception as e:
        logger.debug("Failed to delete existing join record: %s", e)

    table.add(
        [
            JoinRecord(
                left_table=left_table,
                left_column=left_column,
                right_table=right_table,
                right_column=right_column,
                confidence=1.0,
                cardinality=card_enum,
                source="user",
            ).to_dict()
        ]
    )


def _fetch_catalog_candidates(
    table: lancedb.table.Table, left_fqn: str, right_fqn: str
) -> list[dict[str, Any]]:
    """Fetches candidate join entries from the LanceDB registry.

    Args:
        table: LanceDB table instance.
        left_fqn: Left table fully qualified name.
        right_fqn: Right table fully qualified name.

    Returns:
        List of catalog candidate dictionaries.
    """
    l_fqn = sanitize_lance_value(sanitize_identifier(left_fqn))
    r_fqn = sanitize_lance_value(sanitize_identifier(right_fqn))
    where_clause = f"(left_table = '{l_fqn}' and right_table = '{r_fqn}') or (left_table = '{r_fqn}' and right_table = '{l_fqn}')"

    try:
        rows = table.search().where(where_clause).to_list()
    except Exception as e:
        logger.warning("Error querying join registry: %s", e)
        rows = []

    db_candidates = []
    for r in rows:
        is_swapped = r["left_table"] == right_fqn
        card_str = r["cardinality"]
        if is_swapped:
            left_col = r["right_column"]
            right_col = r["left_column"]
            if card_str == "1:N":
                card_str = "N:1"
            elif card_str == "N:1":
                card_str = "1:N"
        else:
            left_col = r["left_column"]
            right_col = r["right_column"]

        try:
            card_enum = Cardinality(card_str)
        except ValueError:
            card_enum = Cardinality.ONE_TO_MANY

        raw_source = r["source"]
        if raw_source == "user":
            basis_enum = JoinBasis.USER_DEFINED
        elif raw_source == "fk":
            basis_enum = JoinBasis.FK_METADATA
        else:
            try:
                basis_enum = JoinBasis(raw_source)
            except ValueError:
                basis_enum = JoinBasis.USER_DEFINED

        db_candidates.append(
            JoinCandidate(
                left=left_col,
                right=right_col,
                confidence=float(r["confidence"]),
                cardinality=card_enum,
                basis=basis_enum,
            ).to_dict()
        )
    return db_candidates


def _deduplicate_and_rank_candidates(
    db_candidates: list[dict[str, Any]],
    fuzzy_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merges and ranks candidate join relationships.

    Args:
        db_candidates: Candidates from catalog registry.
        fuzzy_candidates: Candidates from fuzzy matching engine.

    Returns:
        Deduplicated, confidence-sorted list of join candidates.
    """
    candidate_map: dict[tuple[str, str], dict[str, Any]] = {}

    for r in db_candidates + fuzzy_candidates:
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

    final_candidates: list[dict[str, Any]] = []
    for existing in candidate_map.values():
        sources = existing.pop("sources")
        if len(sources) > 1:
            existing["basis"] = JoinBasis.MIXED.value
        else:
            val = list(sources)[0]
            existing["basis"] = val.value if hasattr(val, "value") else val
        final_candidates.append(existing)

    final_candidates.sort(key=lambda x: x["confidence"], reverse=True)
    return final_candidates


def join_hints(
    conn: SupportsSql | None = None,
    left_fqn: str = "",
    right_fqn: str = "",
    enable_fuzzy: bool | None = None,
) -> list[dict[str, Any]]:
    """Returns candidate join paths between two tables, sorted by confidence descending.

    Args:
        conn: Reserved for multi-source connection routing (optional/unused).
        left_fqn: Fully qualified name of the left table.
        right_fqn: Fully qualified name of the right table.
        enable_fuzzy: Whether to perform fuzzy name-type column comparisons.
            Falls back to env var STRAKE_ENABLE_FUZZY_JOINS if None.

    Returns:
        A list of join candidate dictionaries with left/right column mappings,
        confidence scores, cardinality, and basis info.
    """
    table = _get_join_registry_table()
    db_candidates = _fetch_catalog_candidates(table, left_fqn, right_fqn)

    if enable_fuzzy is None:
        enable_fuzzy = os.environ.get("STRAKE_ENABLE_FUZZY_JOINS", "").lower() in (
            "1",
            "true",
            "yes",
        )

    fuzzy_candidates = []
    if enable_fuzzy:
        fuzzy_candidates = _compute_fuzzy_candidates(left_fqn, right_fqn)

    return _deduplicate_and_rank_candidates(db_candidates, fuzzy_candidates)


def sync_db_foreign_keys(conn: SupportsSql | None = None) -> None:
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
        logger.warning("Failed to list sources for FK sync: %s", e)
        return

    table = _get_join_registry_table()

    for s in sources:
        name = s.get("name")
        if not name:
            continue

        stype = s.get("type", "").lower()
        dialect = ""
        if stype in ("sql", "jdbc"):
            dialect = s.get("config", {}).get("dialect", "").lower()
            if not dialect:
                url = s.get("url", "").lower()
                for d in SUPPORTED_SQL_DIALECTS:
                    if d in url:
                        dialect = d
                        break
        else:
            dialect = stype

        if dialect not in SUPPORTED_SQL_DIALECTS:
            continue

        logger.debug("Syncing foreign keys for source '%s' (dialect: %s)", name, dialect)

        escaped_src = sanitize_lance_value(sanitize_identifier(name))
        where_clause = f"source = 'fk' and (left_table LIKE '{escaped_src}.%' or right_table LIKE '{escaped_src}.%')"
        try:
            table.delete(where_clause)
        except Exception as e:
            logger.debug("Failed to delete old FKs for %s: %s", name, e)

        new_records: list[dict[str, Any]] = []
        if dialect == "postgres":
            try:
                new_records = _extract_postgres_direct_fks(s, name)
                if new_records:
                    table.add(new_records)
                continue
            except Exception as direct_err:
                logger.warning(
                    "Direct connection to Postgres failed, falling back to federated query: %s",
                    direct_err,
                )

        try:
            new_records = _extract_federated_fks(conn, name, dialect)
            if new_records:
                table.add(new_records)
        except Exception as e:
            logger.warning(
                "Failed to query FK metadata for source '%s': %s",
                name,
                e,
                exc_info=True,
            )
