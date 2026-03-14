# Standard Library
import hashlib
import logging
import os
import threading
from typing import Any, Literal

# Third-party Libraries
import lancedb
import pyarrow as pa

# Local Application Imports
from strake import StrakeConnection
from strake.metadata import MetadataEnricher, NullEnricher
from strake.utils import get_strake_dir

logger = logging.getLogger("strake.search")


def truncate(text: str | None, length: int) -> str:
    """Truncates text to a specified length adding an ellipsis if needed."""
    if not text:
        return ""
    if len(text) > length:
        return text[:length] + "..."
    return text


class SchemaIndexer:
    """Manages indexing and searching of database schemas using LanceDB.

    Attributes:
        connection: An active StrakeConnection to the database.
        db_path: Path to the LanceDB cache directory.
        enricher: MetadataEnricher for retrieving table and column descriptions.
    """

    def __init__(
        self,
        connection: StrakeConnection,
        db_path: str | None = None,
        enricher: MetadataEnricher | None = None,
    ):
        """Initializes the SchemaIndexer with connection and optional enricher."""
        self.connection = connection
        self.enricher = enricher or NullEnricher()

        if db_path:
            self.db_path = os.path.expanduser(db_path)
        else:
            self.db_path = str(get_strake_dir("lancedb_cache"))

        self.table_name = "table_schemas"
        self._db = None
        self._db_lock = threading.Lock()

    @property
    def db(self):
        if self._db is None:
            with self._db_lock:
                if self._db is None:
                    os.makedirs(self.db_path, exist_ok=True)
                    self._db = lancedb.connect(self.db_path)
        return self._db

    def build_index(self):
        """Fetch schemas from strake and index them into LanceDB using vectorized operations."""
        logger.info("Building schema index in LanceDB...")

        query = """
            SELECT 
                table_catalog, 
                table_schema, 
                table_name, 
                column_name, 
                data_type 
            FROM information_schema.columns
        """

        try:
            table = self.connection.sql(query)
            schemas = []
            seen_tables: dict[str, str | None] = {}

            # Semantic drift detection logic: hash contents
            # description_hash is present to facilitate future selective re-embed logic.

            for batch in table.to_batches():
                data = batch.to_pydict()
                for catalog, schema_name, table_name, col_name, data_type in zip(
                    data["table_catalog"],
                    data["table_schema"],
                    data["table_name"],
                    data["column_name"],
                    data["data_type"],
                ):
                    key = f"{catalog}.{schema_name}.{table_name}"
                    fqn = f"{schema_name}.{table_name}" if schema_name else table_name

                    if key not in seen_tables:
                        seen_tables[key] = self.enricher.enrich(fqn, None, catalog)
                    table_desc = seen_tables[key]

                    col_desc = self.enricher.enrich(fqn, col_name, catalog)

                    raw_desc = f"{table_desc or ''} | {col_desc or ''}"
                    desc_hash = hashlib.sha256(raw_desc.encode("utf-8")).hexdigest()

                    truncated_desc = truncate(raw_desc, 100)

                    text = f"{table_name} {col_name} {data_type} {truncated_desc}"

                    schemas.append(
                        {
                            "table_id": key,
                            "column_name": col_name,
                            "data_type": data_type,
                            "text": text,
                            "table_description": truncate(table_desc, 200),
                            "column_description": truncate(col_desc, 100),
                            "description_hash": desc_hash,
                        }
                    )

            if not schemas:
                logger.warning("No schema data found to index.")
                return

            schema = pa.schema(
                [
                    pa.field("table_id", pa.string()),
                    pa.field("column_name", pa.string()),
                    pa.field("data_type", pa.string()),
                    pa.field("text", pa.string()),
                    pa.field("table_description", pa.string()),
                    pa.field("column_description", pa.string()),
                    pa.field("description_hash", pa.string()),
                ]
            )

            table = self.db.create_table(
                self.table_name, schema=schema, data=schemas, mode="overwrite"
            )

            try:
                table.create_fts_index("text", replace=True)
            except Exception as e:
                logger.warning(f"Could not create FTS index: {e}")

            logger.info("Schema index built successfully.")
        except Exception as e:
            logger.error(f"Failed to build schema index: {e}")

    def search_tables(
        self,
        query: str,
        limit: int = 5,
        include_descriptions: bool = True,
        description_scope: Literal["tables_only", "all", "none"] = "tables_only",
        max_description_length: int = 100,
    ) -> list[dict[str, Any]]:
        """Search the indexed schemas for relevance.

        Args:
            query: The search query string.
            limit: Maximum number of results to return.
            include_descriptions: Whether to include metadata descriptions.
            description_scope: Controlling if only table or both table/column descs are included.
            max_description_length: Truncation limit for descriptions.

        Returns:
            A list of dictionary results containing schema metadata.
        """
        if self.table_name not in self.db.list_tables():
            logger.info("Schema index missing, building now...")
            self.build_index()

        table = self.db.open_table(self.table_name)

        try:
            results = table.search(query, query_type="fts").limit(limit).to_list()
        except Exception as e:
            logger.warning(
                f"Schema search failed ({type(e).__name__}): {e}. Returning empty results."
            )
            return []

        for r in results:
            if "vector" in r:
                del r["vector"]
            if "text" in r:
                del r["text"]  # remove bulky text
            if "description_hash" in r:
                del r["description_hash"]

            if not include_descriptions or description_scope == "none":
                r["table_description"] = ""
                r["column_description"] = ""
            elif description_scope == "tables_only":
                r["table_description"] = truncate(
                    r.get("table_description"), max_description_length
                )
                r["column_description"] = ""
            elif description_scope == "all":
                r["table_description"] = truncate(
                    r.get("table_description"), max_description_length
                )
                r["column_description"] = truncate(
                    r.get("column_description"), max_description_length
                )

        return results
