use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use std::sync::Arc;

use super::common::{
    next_retry_delay, FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams,
};
use super::wrappers::register_tables;
use crate::config::TableConfig;

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
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
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
        // Infer schema using PRAGMA table_info
        // DuckDB connection is not Send, so we should do this synchronously or ensure it doesn't cross await.
        // new is async but we don't await anything problematic here?
        // Actually, connection logic is blocking on file I/O usually with duckdb-rs.

        let connection_string = connection_string.clone();
        let table_name = table_name.clone();

        // Perform schema inference in a blocking task if needed, or just inline.
        // Inline is fine as long as we don't hold it across await.
        // We do use '?' which returns Result, not await.
        // So this is fine.
        let (fields, _conn) = {
            let conn = duckdb::Connection::open(&connection_string)
                .context("Failed to open DuckDB for schema inference")?;

            let mut stmt = conn
                .prepare(&format!("PRAGMA table_info('{}')", table_name))
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
            (fields, conn)
        };

        Ok(Self {
            connection_string,
            table_name,
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

fn map_duckdb_type(type_str: &str) -> DataType {
    let t = type_str.to_uppercase();
    if t.contains("INT") || t == "INTEGER" || t == "BIGINT" {
        DataType::Int64
    } else if t == "VARCHAR" || t == "TEXT" || t == "STRING" {
        DataType::Utf8
    } else if t == "DOUBLE" || t == "FLOAT" {
        DataType::Float64
    } else if t == "BOOLEAN" || t == "BOOL" {
        DataType::Boolean
    } else if t.contains("TIMESTAMP") {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    } else if t == "DATE" {
        DataType::Date32
    } else {
        DataType::Utf8
    } // Fallback
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
        let batch = {
            let conn = duckdb::Connection::open(&self.connection_string)
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let target_schema = if let Some(proj) = projection {
                self.schema.project(proj)?
            } else {
                self.schema.as_ref().clone()
            };

            let col_names: Vec<String> = target_schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();
            let query = format!("SELECT {} FROM {}", col_names.join(", "), self.table_name);

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let num_cols = target_schema.fields().len();

            let mut rows = stmt
                .query([])
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let mut col_buffers: Vec<Vec<ScalarValue>> = vec![vec![]; num_cols];

            while let Some(row) = rows
                .next()
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?
            {
                for (i, buffer) in col_buffers.iter_mut().enumerate() {
                    let val_ref = row.get_ref(i).unwrap();
                    let scalar = duck_val_to_scalar(val_ref, target_schema.field(i).data_type());
                    buffer.push(scalar);
                }
            }

            let mut arrays = Vec::new();
            for buffer in col_buffers {
                let array = ScalarValue::iter_to_array(buffer)
                    .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
                arrays.push(array);
            }

            arrow::record_batch::RecordBatch::try_new(Arc::new(target_schema.clone()), arrays)?
        }; // conn, stmt, rows are dropped here

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

        mem_table.scan(state, None, filters, limit).await
    }
}

fn duck_val_to_scalar(val: duckdb::types::ValueRef, dt: &DataType) -> ScalarValue {
    use duckdb::types::ValueRef;
    match val {
        ValueRef::Null => ScalarValue::try_from(dt).unwrap(),
        ValueRef::Boolean(b) => ScalarValue::Boolean(Some(b)),
        ValueRef::TinyInt(i) => ScalarValue::Int8(Some(i)),
        ValueRef::SmallInt(i) => ScalarValue::Int16(Some(i)),
        ValueRef::Int(i) => ScalarValue::Int32(Some(i)),
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
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_ref.table();
        let provider =
            DuckDBTableProvider::new(self.connection_string.clone(), table_name.to_string())
                .await?;

        Ok(Arc::new(provider))
    }
}

pub async fn register_duckdb(params: SqlSourceParams<'_>) -> Result<()> {
    let mut attempt = 0;
    let retry = params.retry;
    loop {
        match try_register_duckdb(
            params.context,
            params.catalog_name,
            params.name,
            params.connection_string,
            params.cb.clone(),
            params.explicit_tables,
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                attempt += 1;
                if attempt >= retry.max_attempts {
                    tracing::error!(
                        "Failed to register DuckDB source '{}' after {} attempts: {}",
                        params.name,
                        retry.max_attempts,
                        e
                    );
                    return Err(e);
                }
                let delay = next_retry_delay(attempt, retry.base_delay_ms, retry.max_delay_ms);
                tracing::warn!(
                    "Connection failed for source '{}'. Retrying in {:?} (Attempt {}/{}): {}",
                    params.name,
                    delay,
                    attempt,
                    retry.max_attempts,
                    e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn try_register_duckdb(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<crate::query::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
) -> Result<()> {
    // For DuckDB, connection is file path.
    // We don't use a pool yet, just path string.
    let factory = DuckDBTableFactory::new(connection_string.to_string());

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables
            .iter()
            .map(|t| (t.name.clone(), name.to_string()))
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
    let conn = duckdb::Connection::open(db_path)
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
}
