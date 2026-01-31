use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use std::sync::Arc;

use super::common::{FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams};
use super::wrappers::register_tables;
use strake_common::config::TableConfig;
use strake_common::retry::retry_async;

/// DuckDB Metadata Fetcher
pub struct DuckDBMetadataFetcher {
    #[allow(dead_code)]
    pub db_path: String,
}

#[async_trait]
impl SqlMetadataFetcher for DuckDBMetadataFetcher {
    async fn fetch_metadata(&self, _schema: &str, _table: &str) -> Result<FetchedMetadata> {
        // DuckDB metadata fetching implementation
        Ok(FetchedMetadata::default())
    }
}

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;

/// DuckDB Table Provider
#[derive(Debug)]
pub struct DuckDBTableProvider {
    connection_string: String,
    table_name: String,
    schema: SchemaRef,
}

impl DuckDBTableProvider {
    pub async fn new(connection_string: String, table_name: String) -> Result<Self> {
        let conn_str = connection_string.clone();
        let tbl_name = table_name.clone();

        let fields = tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&conn_str)
                .context("Failed to open DuckDB for schema inference")?;

            let mut stmt = conn
                .prepare(&format!("PRAGMA table_info('{}')", tbl_name))
                .context("Failed to prepare table_info query")?;

            let rows = stmt
                .query_map([], |row| {
                    let name: String = row.get("name")?;
                    let type_str: String = row.get("type")?;
                    let notnull: bool = row.get("notnull")?;
                    Ok((name, type_str, notnull))
                })
                .context("Failed to execute table_info")?;

            let mut fields = Vec::new();
            for row in rows {
                let (name, type_str, notnull) = row?;
                let dt = map_duckdb_type(&type_str);
                fields.push(Field::new(name, dt, !notnull));
            }
            Ok::<_, anyhow::Error>(fields)
        })
        .await
        .context("Join error during schema inference")??;

        Ok(Self {
            connection_string,
            table_name,
            schema: Arc::new(Schema::new(fields)),
        })
    }
    pub async fn execute_substrait_plan(&self, plan_bytes: Vec<u8>) -> Result<RecordBatch> {
        // Isolate DuckDB interaction
        let connection_string = self.connection_string.clone();
        let schema = self.schema.clone();

        let batch = tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&connection_string)?;

            // Enable Substrait extension
            conn.execute("INSTALL substrait", [])
                .context("Failed to install substrait extension")?;
            conn.execute("LOAD substrait", [])
                .context("Failed to load substrait extension")?;

            // Execute plan
            let mut stmt = conn
                .prepare("SELECT * FROM from_substrait(?)")
                .context("Failed to prepare substrait query")?;

            let mut rows = stmt
                .query([plan_bytes])
                .context("Failed to execute substrait query")?;

            // Convert to RecordBatch
            convert_duckdb_rows_to_arrow(&mut rows, schema)
        })
        .await
        .context("Join error during substrait execution")??;

        Ok(batch)
    }
}

pub fn map_duckdb_type(type_str: &str) -> DataType {
    let t = type_str.to_uppercase();
    if t == "BIGINT" || t == "INT8" || t == "LONG" {
        DataType::Int64
    } else if t == "INTEGER" || t == "INT" || t == "INT4" || t == "SIGNED" {
        DataType::Int32
    } else if t == "SMALLINT" || t == "INT2" || t == "SHORT" {
        DataType::Int16
    } else if t == "TINYINT" || t == "INT1" {
        DataType::Int8
    } else if t == "UBIGINT" {
        DataType::UInt64
    } else if t == "UINTEGER" || t == "UINT" {
        DataType::UInt32
    } else if t == "USMALLINT" || t == "USHORT" {
        DataType::UInt16
    } else if t == "UTINYINT" {
        DataType::UInt8
    } else if t == "VARCHAR" || t == "TEXT" || t == "STRING" || t == "CHAR" || t == "BPCHAR" {
        DataType::Utf8
    } else if t == "DOUBLE" || t == "FLOAT8" || t == "DECIMAL" {
        DataType::Float64 // Treat decimals as Float64 for simplicity unless precise mapping strategy
    } else if t == "FLOAT" || t == "FLOAT4" || t == "REAL" {
        DataType::Float32
    } else if t == "BOOLEAN" || t == "BOOL" {
        DataType::Boolean
    } else if t.contains("TIMESTAMP") {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    } else if t == "DATE" {
        DataType::Date32
    } else if t == "BLOB" || t == "BYTEA" || t == "BINARY" || t == "VARBINARY" {
        DataType::Binary
    } else {
        // Fallback
        DataType::Utf8
    }
}

#[async_trait]
impl TableProvider for DuckDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Isolate DuckDB interaction to ensure no non-Send types cross await points
        let connection_string = self.connection_string.clone();
        let table_name = self.table_name.clone();

        let target_schema = if let Some(proj) = projection {
            self.schema.project(proj)?
        } else {
            self.schema.as_ref().clone()
        };
        let target_schema = Arc::new(target_schema);

        // Generate SQL Query outside blocking task
        let col_names: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();

        let mut query = format!("SELECT {} FROM {}", col_names.join(", "), table_name);

        let mut where_clauses = Vec::new();
        {
            let dialect = datafusion::sql::unparser::dialect::PostgreSqlDialect {};
            let unparser = datafusion::sql::unparser::Unparser::new(&dialect);

            for filter in filters {
                if let Ok(sql) = unparser.expr_to_sql(filter) {
                    where_clauses.push(sql.to_string());
                } else {
                    tracing::warn!("Failed to unparse filter for pushdown: {:?}", filter);
                }
            }
        }

        if !where_clauses.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&where_clauses.join(" AND "));
        }

        if let Some(n) = limit {
            query.push_str(&format!(" LIMIT {}", n));
        }

        tracing::info!(query = %query, "Executing DuckDB Pushdown Query");

        let batch = tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&connection_string)
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let mut rows = stmt
                .query([])
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            convert_duckdb_rows_to_arrow(&mut rows, target_schema)
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))
        })
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Join Error: {}", e))
        })??;

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

        // Note: we pass 'None' for filters and 'None' for limit to mem_table.scan
        // because we have already applied them at the source (DuckDB).
        // However, DataFusion might still stick a Filter/Limit node on top if we don't return Exact pushdown confirmation.
        // For correctness, passing them again to MemTable is safe (limit 2 on 2 rows is 2 rows).
        // But to verify pushdown optimization, we ideally want to fetch less data.
        mem_table.scan(state, None, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Optimistically accept all filters
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

fn convert_duckdb_rows_to_arrow(rows: &mut duckdb::Rows, schema: SchemaRef) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let mut col_buffers: Vec<Vec<ScalarValue>> = vec![vec![]; num_cols];

    while let Some(row) = rows.next()? {
        for (i, buffer) in col_buffers.iter_mut().enumerate() {
            // Safety: We assume schema matches row width.
            // If row has fewer columns, unwrap panics.
            // Should be robust?
            if i >= row.as_ref().column_count() {
                continue; // or error
            }
            let val_ref = row.get_ref(i)?;
            let scalar = duck_val_to_scalar(val_ref, schema.field(i).data_type());
            buffer.push(scalar);
        }
    }

    let mut arrays = Vec::new();
    for buffer in col_buffers {
        let array = ScalarValue::iter_to_array(buffer)?;
        arrays.push(array);
    }

    Ok(arrow::record_batch::RecordBatch::try_new(schema, arrays)?)
}

fn duck_val_to_scalar(val: duckdb::types::ValueRef, dt: &DataType) -> ScalarValue {
    use duckdb::types::ValueRef;

    // Attempt to match requested type if possible
    match val {
        ValueRef::Null => ScalarValue::try_from(dt).unwrap(),
        ValueRef::Boolean(b) => ScalarValue::Boolean(Some(b)),
        ValueRef::TinyInt(i) => ScalarValue::Int8(Some(i)),
        ValueRef::SmallInt(i) => ScalarValue::Int16(Some(i)),
        ValueRef::Int(i) => {
            if let DataType::Int64 = dt {
                ScalarValue::Int64(Some(i as i64))
            } else {
                ScalarValue::Int32(Some(i))
            }
        }
        ValueRef::BigInt(i) => ScalarValue::Int64(Some(i)),
        ValueRef::HugeInt(i) => ScalarValue::Decimal128(Some(i), 38, 0),
        ValueRef::Float(f) => ScalarValue::Float32(Some(f)),
        ValueRef::Double(f) => ScalarValue::Float64(Some(f)),
        ValueRef::Text(s) => ScalarValue::Utf8(Some(String::from_utf8_lossy(s).to_string())),
        ValueRef::Blob(b) => ScalarValue::Binary(Some(b.to_vec())),
        ValueRef::Date32(d) => ScalarValue::Date32(Some(d)),
        ValueRef::Timestamp(u, _unit) => ScalarValue::TimestampMicrosecond(Some(u as i64), None),
        _ => ScalarValue::Utf8(Some(format!("{:?}", val))),
    }
}

pub struct DuckDBTableFactory {
    connection_string: String,
}

impl DuckDBTableFactory {
    pub fn new(connection_string: String) -> Self {
        Self { connection_string }
    }
}

#[async_trait]
impl SqlProviderFactory for DuckDBTableFactory {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: FetchedMetadata,
        cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_ref.table();
        let provider =
            DuckDBTableProvider::new(self.connection_string.clone(), table_name.to_string())
                .await?;

        // Wrap with metadata and circuit breaker
        Ok(super::wrappers::wrap_provider(
            Arc::new(provider),
            cb,
            metadata,
        ))
    }
}

pub async fn register_duckdb(params: SqlSourceParams<'_>) -> Result<()> {
    let context = params.context;
    let catalog_name = params.catalog_name;
    let name = params.name;
    let connection_string = params.connection_string;
    let cb = params.cb.clone();
    let explicit_tables = params.explicit_tables;
    let retry_settings = params.retry;

    retry_async(
        &format!("register_duckdb({})", name),
        retry_settings,
        move || {
            let cb = cb.clone();
            async move {
                try_register_duckdb(
                    context,
                    catalog_name,
                    name,
                    connection_string,
                    cb,
                    explicit_tables,
                )
                .await
            }
        },
    )
    .await
}

async fn try_register_duckdb(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
) -> Result<()> {
    // For DuckDB, connection is file path.
    // We don't use a pool yet, just path string.
    let factory = DuckDBTableFactory::new(connection_string.to_string());

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables
            .iter()
            .map(|t| {
                let schema = if t.schema.is_empty() || t.schema == "public" {
                    name.to_string()
                } else {
                    t.schema.clone()
                };
                (t.name.clone(), schema)
            })
            .collect()
    } else {
        introspect_duckdb_tables(connection_string)
            .await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(DuckDBMetadataFetcher {
        db_path: connection_string.to_string(),
    }));

    register_tables(
        context,
        catalog_name,
        name,
        fetcher,
        &factory,
        cb,
        tables_to_register,
    )
    .await?;
    Ok(())
}

pub async fn introspect_duckdb_tables(db_path: &str) -> Result<Vec<String>> {
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open(&db_path)
            .context("Failed to open DuckDB database for introspection")?;

        let mut stmt = conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema='main'")
            .context("Failed to prepare DuckDB introspection query")?;

        let rows = stmt
            .query_map([], |row| row.get(0))
            .context("Failed to execute DuckDB introspection query")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect DuckDB table names")?;

        Ok(rows)
    })
    .await
    .context("Join error during introspection")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use tempfile::tempdir;

    #[test]
    fn test_map_duckdb_type() {
        assert_eq!(map_duckdb_type("BIGINT"), DataType::Int64);
        assert_eq!(map_duckdb_type("INTEGER"), DataType::Int32);
        assert_eq!(map_duckdb_type("VARCHAR"), DataType::Utf8);
        assert_eq!(map_duckdb_type("BOOLEAN"), DataType::Boolean);
        assert_eq!(map_duckdb_type("UNKNOWN"), DataType::Utf8);
    }

    #[test]
    fn test_duckdb_version() -> Result<()> {
        let conn = duckdb::Connection::open_in_memory()?;
        let version: String = conn.query_row("SELECT version()", [], |row| row.get(0))?;
        println!("DuckDB Version: {}", version);
        Ok(())
    }

    #[tokio::test]
    async fn test_duckdb_substrait_handover() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.duckdb");
        let db_path_str = db_path.to_str().unwrap();

        // 1. Setup DuckDB with some data
        {
            let conn = duckdb::Connection::open(db_path_str)?;
            conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)", [])?;
            conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')", [])?;
        }

        let provider =
            DuckDBTableProvider::new(db_path_str.to_string(), "users".to_string()).await?;

        // 2. Create a DataFusion plan
        let ctx = SessionContext::new();

        // Use a simple logical plan that can be converted to Substrait.
        // We use a scan of an empty table with the same schema to generate the plan,
        // then we'll execute it against our DuckDB provider.
        let schema = provider.schema();
        ctx.register_table(
            "users",
            Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
        )?;

        let plan = ctx
            .table("users")
            .await?
            .filter(col("id").eq(lit(1)))?
            .into_optimized_plan()?;

        // 3. Convert to Substrait
        let plan_bytes = strake_sql::substrait_producer::to_substrait_bytes(&plan, &ctx).await?;

        // 4. Handover to DuckDB
        // execute_substrait_plan will try to INSTALL/LOAD substrait
        // In some environments this might fail if no internet.
        // We catch error and skip if it's an extension loading error.
        match provider.execute_substrait_plan(plan_bytes).await {
            Ok(batch) => {
                assert_eq!(batch.num_rows(), 1);
                // Schema has id and name
                let id_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                assert_eq!(id_col.value(0), 1);
                let name_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                assert_eq!(name_col.value(0), "Alice");
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Failed to install substrait extension")
                    || msg.contains("Failed to load substrait extension")
                    || msg.contains("IO Error: Failed to download")
                    || msg.contains("Extension \"substrait\" not found")
                {
                    println!("Skipping test: Substrait extension not available or cannot be downloaded: {}", msg);
                } else {
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
