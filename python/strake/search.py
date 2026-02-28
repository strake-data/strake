import lancedb
import pyarrow as pa
import os
import logging
import threading
from typing import Any, Dict, List, Optional
from strake import StrakeConnection
from .utils import get_strake_dir

logger = logging.getLogger("strake.search")


class SchemaIndexer:
    def __init__(self, connection: StrakeConnection, db_path: Optional[str] = None):
        self.connection = connection

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
                    # LanceDB's Rust core handles the heavy lifting and releases the GIL
                    # during search operations, allowing parallelism with other threads.
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

            # Use Arrow's group_by aggregation (available in pyarrow >= 15.0)
            # This pushes schema aggregation into Arrow's C++ kernels
            grouped = table.group_by(["table_catalog", "table_schema", "table_name"])
            aggregated = grouped.aggregate(
                [("column_name", "list"), ("data_type", "list")]
            )

            schemas = []

            # Convert aggregated result to dict of lists (one pass)
            data = aggregated.to_pydict()

            # Iterate using zip for cleaner and potentially faster processing
            for catalog, schema_name, table_name, col_list, type_list in zip(
                data["table_catalog"],
                data["table_schema"],
                data["table_name"],
                data["column_name_list"],
                data["data_type_list"],
            ):
                key = f"{catalog}.{schema_name}.{table_name}"

                # Build text document efficiently
                col_docs = [f"- {c} ({t})" for c, t in zip(col_list, type_list)]
                text = f"Table: {key}\nColumns:\n" + "\n".join(col_docs)

                schemas.append(
                    {
                        "table_id": key,
                        "text": text,
                        "columns": [
                            {"name": c, "type": t} for c, t in zip(col_list, type_list)
                        ],
                    }
                )

            if not schemas:
                logger.warning("No schema data found to index.")
                return

            # Overwrite the table directly, avoiding rename_table which is unsupported in OSS
            table = self.db.create_table(
                self.table_name, data=schemas, mode="overwrite"
            )

            # Create full-text search index as fallback if tantivy is present
            try:
                table.create_fts_index("text", replace=True)
            except Exception as e:
                logger.warning(f"Could not create FTS index: {e}")

            logger.info("Schema index built successfully.")
        except Exception as e:
            logger.error(f"Failed to build schema index: {e}")

    def search_tables(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search the indexed schemas for relevance."""
        if self.table_name not in self.db.table_names():
            logger.info("Schema index missing, building now...")
            self.build_index()

        table = self.db.open_table(self.table_name)

        # Use full-text search as the primary mechanism if embeddings are complex to set up in the REPL
        try:
            results = table.search(query, query_type="fts").limit(limit).to_list()
        except Exception as e:
            # Fallback if fts isn't built or fails
            logger.warning(
                f"Schema search failed ({type(e).__name__}): {e}. Returning empty results."
            )
            return []

        for r in results:
            if "vector" in r:
                del r["vector"]  # remove bulky vector from REPL output
            if "columns" in r:
                del r["columns"]  # remove parsed list-of-dicts to save space

        return results
