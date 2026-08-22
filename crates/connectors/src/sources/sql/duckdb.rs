//! # DuckDB Table Provider and Execution Plan
//!
//! This module provides the `DuckDBTableProvider` for integrating DuckDB tables into
//! DataFusion, and the `DuckDBScanExec` for physical execution of pushed-down queries.
//!
//! ## Usage
//!
//! ```rust
//! use strake_connectors::sources::sql::duckdb::DuckDBTableProvider;
//! use std::sync::Arc;
//!
//! // Initialization requires a DuckDB connection pool
//! // let provider = DuckDBTableProvider::new(pool, "my_table".to_string()).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Pushdown**: SQL fragments are generated using the `strake-sql` generator.
//! - **Execution**: Results are streamed via DuckDB's native Arrow interface.
//! - **Safety**: `DuckDBScanExec` employs memoized column reordering to ensure data
//!   integrity with minimal per-batch overhead.

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use duckdb::DuckdbConnectionManager;
use r2d2::Pool;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use std::sync::Arc;
use std::sync::OnceLock;
use strake_common::circuit_breaker::AdaptiveCircuitBreaker;

use super::base_connector::GenericSqlConnector;
use super::common::{SchemaMappingRule, SqlProviderFactory, SqlSourceParams};
use super::duckdb_introspect::DuckDBIntrospector;

/// Newtype for a DuckDB database path.
///
/// # Note
/// This type canonicalizes the path on creation to ensure consistent source identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DuckDBPath(String);

impl DuckDBPath {
    /// Creates a new `DuckDBPath` from a string, canonicalizing the path for identity consistency.
    ///
    /// # Note
    /// This function uses `std::fs::canonicalize` which is a blocking operation. It should
    /// primarily be called during synchronous initialization or within `spawn_blocking`.
    pub fn new(path: impl Into<String>) -> Self {
        let path_str = path.into();
        if path_str.is_empty() || path_str == ":memory:" {
            return Self(path_str);
        }

        // Canonicalize the path if possible to ensure "./db" and "db" are treated as the same source.
        let p = std::path::Path::new(&path_str);
        if let Ok(canonical) = std::fs::canonicalize(p)
            && let Some(s) = canonical.to_str()
        {
            return Self(s.to_string());
        }

        Self(path_str)
    }

    /// Returns the path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns true if this is an in-memory database.
    pub fn is_memory(&self) -> bool {
        self.0.is_empty() || self.0 == ":memory:"
    }
}

impl From<String> for DuckDBPath {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl AsRef<std::path::Path> for DuckDBPath {
    fn as_ref(&self) -> &std::path::Path {
        std::path::Path::new(&self.0)
    }
}

impl AsRef<str> for DuckDBPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DuckDBPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Partitioning, PlanProperties};

/// DuckDB connection pool type.
///
/// Wraps `r2d2::Pool` with `DuckdbConnectionManager`.
pub type DuckDBPool = Pool<DuckdbConnectionManager>;

/// Execution plan for scanning a DuckDB table.
///
/// This plan is responsible for executing a SQL query against DuckDB and streaming
/// the results back to DataFusion as Arrow record batches.
///
/// # Performance
/// This plan uses name-based column reordering to ensure data integrity and records
/// execution metrics (row count, compute time).
pub struct DuckDBScanExec {
    pool: Arc<DuckDBPool>,
    query: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// Memoized column index mapping to avoid per-batch reordering overhead.
    /// Stores Result to propagate initialization errors safely instead of panicking.
    memoized_index_map: Arc<OnceLock<Result<Vec<usize>, datafusion::error::DataFusionError>>>,
}

impl std::fmt::Debug for DuckDBScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBScanExec")
            .field("query", &self.query)
            .field("schema", &self.schema)
            .field("metrics", &"ExecutionPlanMetricsSet")
            .finish()
    }
}

impl DuckDBScanExec {
    /// Creates a new `DuckDBScanExec` execution plan node.
    pub fn new(pool: Arc<DuckDBPool>, query: String, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            pool,
            query,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            memoized_index_map: Arc::new(OnceLock::new()),
        }
    }
}

impl DisplayAs for DuckDBScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBScanExec: query={}", self.query)
    }
}

#[async_trait]
impl ExecutionPlan for DuckDBScanExec {
    fn name(&self) -> &str {
        "DuckDBScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let query = self.query.clone();

        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 2);
        let tx = builder.tx();
        let metrics = BaselineMetrics::new(&self.metrics, _partition);
        let bytes_metrics = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .output_bytes(_partition);
        let index_map_cache = self.memoized_index_map.clone();

        builder.spawn_blocking(move || {
            let _timer = metrics.elapsed_compute().timer();
            let conn = pool.get()
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            let batches = stmt.query_arrow([])
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

            for batch in batches {
                // Name-based column reordering to prevent silent data corruption.
                // DuckDB might return columns in a different order than expected by DataFusion.
                let batch = if batch.schema() != schema {
                    // Avoid per-batch HashMap allocation.
                    // We use a memoized index map computed once per ExecutionPlan.
                    let batch_schema = batch.schema();
                    let index_map = index_map_cache.get_or_init(|| {
                        schema.fields().iter().map(|field| {
                            batch_schema.index_of(field.name()).map_err(|_| {
                                datafusion::error::DataFusionError::Execution(
                                    format!("Missing column '{}' in DuckDB result batch", field.name())
                                )
                            })
                        }).collect::<Result<Vec<_>, _>>()
                    }).as_ref().map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                    // Defensive check: Ensure the current batch is compatible with the memoized mapping.
                    if index_map.iter().any(|&idx| idx >= batch_schema.fields().len()) {
                         return Err(datafusion::error::DataFusionError::Execution(
                            "DuckDB batch schema is incompatible with memoized index map".to_string()
                        ));
                    }

                    let columns: Vec<_> = index_map
                        .iter()
                        .map(|&idx| batch.column(idx).clone())
                        .collect();

                    RecordBatch::try_new(schema.clone(), columns)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
                } else {
                    batch
                };

                metrics.record_output(batch.num_rows());
                bytes_metrics.add(batch.get_array_memory_size());

                if let Err(e) = tx.blocking_send(Ok(batch)) {
                    tracing::debug!(target: "connector", error = %e, "Failed to send batch to execution stream");
                    break;
                }
            }
            Ok(())
        });

        Ok(builder.build())
    }
}

impl crate::sources::federated::FederatedPlan for DuckDBScanExec {
    fn pushed_sql(&self) -> Option<&str> {
        Some(&self.query)
    }
}

/// Safely quotes a SQL identifier (table or column name) for DuckDB.
pub fn quote_identifier(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// Safely escapes a string literal for DuckDB.
pub fn escape_literal(lit: &str) -> String {
    lit.replace('\'', "''")
}

/// Implementation of `TableProvider` for DuckDB.
///
/// Supports filter pushdown, limit pushdown, and native Arrow data exchange.
pub struct DuckDBTableProvider {
    pool: Arc<DuckDBPool>,
    table_name: String,
    schema: SchemaRef,
    dialect: Arc<dyn datafusion::sql::unparser::dialect::Dialect + Send + Sync>,
}

impl std::fmt::Debug for DuckDBTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTableProvider")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DuckDBTableProvider {
    /// Creates a new `DuckDBTableProvider`.
    ///
    /// # Errors
    /// Returns an error if schema inference fails.
    pub async fn new(pool: Arc<DuckDBPool>, table_name: String) -> Result<Self> {
        let fields = infer_duckdb_schema(pool.clone(), &table_name).await?;
        let dialect = Arc::new(datafusion::sql::unparser::dialect::DuckDBDialect::new());

        Ok(Self {
            pool,
            table_name,
            schema: Arc::new(Schema::new(fields)),
            dialect,
        })
    }

    /// Generates the SQL query for pushdown.
    pub fn generate_pushdown_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<(SchemaRef, String)> {
        let unparser = datafusion::sql::unparser::Unparser::new(self.dialect.as_ref());
        generate_duckdb_pushdown_sql(
            &self.table_name,
            &self.schema,
            &unparser,
            projection,
            filters,
            limit,
        )
    }
}

/// Infers the schema for a DuckDB table.
pub async fn infer_duckdb_schema(pool: Arc<DuckDBPool>, table_name: &str) -> Result<Vec<Field>> {
    let pool_cloned = pool.clone();
    let tbl_name = table_name.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool_cloned
            .get()
            .context("Failed to get DuckDB connection from pool")?;

        let mut stmt = conn
            .prepare(&format!(
                "PRAGMA table_info('{}')",
                escape_literal(&tbl_name)
            ))
            .context("Failed to prepare table_info query")?;

        let rows = stmt
            .query_map([], |row| {
                let name: String = row.get("name")?;
                let type_str: String = row.get("type")?;
                let notnull: bool = row.get("notnull")?;
                Ok((name, type_str, notnull))
            })
            .context("Failed to execute table_info")?;

        let mut fields = Vec::with_capacity(16);
        for row in rows {
            let (name, type_str, notnull) = row?;
            let dt = map_duckdb_type(&type_str);
            fields.push(Field::new(name, dt, !notnull));
        }
        Ok::<_, anyhow::Error>(fields)
    })
    .await
    .context("Join error during schema inference")?
}

/// Generates the SQL query for pushdown.
pub fn generate_duckdb_pushdown_sql(
    table_name: &str,
    schema: &SchemaRef,
    unparser: &datafusion::sql::unparser::Unparser,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> datafusion::error::Result<(SchemaRef, String)> {
    let target_schema = if let Some(proj) = projection {
        schema.project(proj)?
    } else {
        schema.as_ref().clone()
    };
    let target_schema = Arc::new(target_schema);

    let col_names: Vec<String> = target_schema
        .fields()
        .iter()
        .map(|f| quote_identifier(f.name()))
        .collect();

    let mut query = format!(
        "SELECT {} FROM {}",
        col_names.join(", "),
        quote_identifier(table_name)
    );

    let mut where_clauses = Vec::new();
    for filter in filters {
        if let Ok(sql) = unparser.expr_to_sql(filter) {
            where_clauses.push(sql.to_string());
        } else {
            tracing::warn!(filter = ?filter, "Failed to unparse filter for pushdown");
        }
    }

    if !where_clauses.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&where_clauses.join(" AND "));
    }

    if let Some(n) = limit {
        query.push_str(&format!(" LIMIT {}", n));
    }

    Ok((target_schema, query))
}

/// Maps a DuckDB type string to an Arrow `DataType`.
///
/// # Note
/// This mapping defaults to `Decimal64(18, 2)` for general `DECIMAL` types.
/// However, users should be aware that certain DataFusion arithmetic kernels
/// may internally widen these to `Decimal128`, which might impact performance
/// or type consistency in complex expressions.
pub fn map_duckdb_type(type_str: &str) -> DataType {
    if type_str.starts_with("DECIMAL") || type_str.starts_with("decimal") {
        let t = type_str.to_uppercase();
        // Parse DECIMAL(P, S)
        let (p, s) = if let (Some(start), Some(end)) = (t.find('('), t.find(')')) {
            let parts: Vec<&str> = t[start + 1..end].split(',').map(|s| s.trim()).collect();
            if parts.len() == 2
                && let (Ok(p), Ok(s)) = (parts[0].parse::<u8>(), parts[1].parse::<i8>())
            {
                (p, s)
            } else {
                (18, 2)
            }
        } else {
            (18, 2)
        };

        return DataType::Decimal128(p, s);
    }

    if type_str.eq_ignore_ascii_case("BIGINT")
        || type_str.eq_ignore_ascii_case("INT8")
        || type_str.eq_ignore_ascii_case("LONG")
    {
        DataType::Int64
    } else if type_str.eq_ignore_ascii_case("INTEGER")
        || type_str.eq_ignore_ascii_case("INT")
        || type_str.eq_ignore_ascii_case("INT4")
        || type_str.eq_ignore_ascii_case("SIGNED")
    {
        DataType::Int32
    } else if type_str.eq_ignore_ascii_case("SMALLINT")
        || type_str.eq_ignore_ascii_case("INT2")
        || type_str.eq_ignore_ascii_case("SHORT")
    {
        DataType::Int16
    } else if type_str.eq_ignore_ascii_case("TINYINT") || type_str.eq_ignore_ascii_case("INT1") {
        DataType::Int8
    } else if type_str.eq_ignore_ascii_case("UBIGINT") {
        DataType::UInt64
    } else if type_str.eq_ignore_ascii_case("UINTEGER") || type_str.eq_ignore_ascii_case("UINT") {
        DataType::UInt32
    } else if type_str.eq_ignore_ascii_case("USMALLINT") || type_str.eq_ignore_ascii_case("USHORT")
    {
        DataType::UInt16
    } else if type_str.eq_ignore_ascii_case("UTINYINT") {
        DataType::UInt8
    } else if type_str.eq_ignore_ascii_case("VARCHAR")
        || type_str.eq_ignore_ascii_case("TEXT")
        || type_str.eq_ignore_ascii_case("STRING")
        || type_str.eq_ignore_ascii_case("CHAR")
        || type_str.eq_ignore_ascii_case("BPCHAR")
    {
        DataType::Utf8
    } else if type_str.eq_ignore_ascii_case("DOUBLE") || type_str.eq_ignore_ascii_case("FLOAT8") {
        DataType::Float64
    } else if type_str.eq_ignore_ascii_case("FLOAT")
        || type_str.eq_ignore_ascii_case("FLOAT4")
        || type_str.eq_ignore_ascii_case("REAL")
    {
        DataType::Float32
    } else if type_str.eq_ignore_ascii_case("BOOLEAN") || type_str.eq_ignore_ascii_case("BOOL") {
        DataType::Boolean
    } else if type_str.to_uppercase().contains("TIMESTAMP") {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    } else if type_str.eq_ignore_ascii_case("DATE") {
        DataType::Date32
    } else if type_str.eq_ignore_ascii_case("BLOB")
        || type_str.eq_ignore_ascii_case("BYTEA")
        || type_str.eq_ignore_ascii_case("BINARY")
        || type_str.eq_ignore_ascii_case("VARBINARY")
    {
        DataType::Binary
    } else {
        // Fallback
        DataType::Utf8
    }
}

#[async_trait]
impl TableProvider for DuckDBTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let (target_schema, query) = self.generate_pushdown_sql(projection, filters, limit)?;

        tracing::info!(query = %query, "Executing Streaming DuckDB Pushdown Query");

        Ok(Arc::new(DuckDBScanExec::new(
            self.pool.clone(),
            query,
            target_schema,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        let unparser = datafusion::sql::unparser::Unparser::new(self.dialect.as_ref());

        // Only return Exact if we can actually unparse the filter.
        Ok(filters
            .iter()
            .map(|f| {
                if unparser.expr_to_sql(f).is_ok() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

// Manual conversion functions removed in favor of duckdb's native query_arrow

/// Factory for creating DuckDB table providers.
///
/// Implements `SqlProviderFactory` to integrate with the Strake source registration system.
pub struct DuckDBTableFactory {
    pool: Arc<DuckDBPool>,
    /// Shared federation provider across all tables from this database.
    federation_provider: Arc<dyn datafusion_federation::FederationProvider>,
    /// Maximum number of concurrent queries allowed for this source (0 = unlimited).
    max_concurrent_queries: usize,
}

impl DuckDBTableFactory {
    /// Creates a new `DuckDBTableFactory`.
    ///
    /// # Errors
    /// Returns an error if the connection pool cannot be initialized.
    pub fn new(path: DuckDBPath) -> Result<Self> {
        let manager = DuckdbConnectionManager::file(path.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to create DuckDB connection manager: {}", e))?;
        let pool = r2d2::Pool::builder()
            .max_size(10)
            .build(manager)
            .context("Failed to create DuckDB connection pool")?;

        let pool = Arc::new(pool);
        let executor = Arc::new(super::duckdb_federation::DuckDBExecutor::new(
            pool.clone(),
            path.clone(),
        )?);
        let federation_provider = executor.create_federation_provider();

        Ok(Self {
            pool,
            federation_provider,
            max_concurrent_queries: 0,
        })
    }

    /// Sets the maximum number of concurrent queries allowed for this source.
    pub fn with_max_concurrent_queries(mut self, max: usize) -> Self {
        self.max_concurrent_queries = max;
        self
    }
}

#[async_trait]
impl SqlProviderFactory for DuckDBTableFactory {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        cb: Arc<AdaptiveCircuitBreaker>,
        custom_schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_ref.table();
        let provider = DuckDBTableProvider::new(self.pool.clone(), table_name.to_string()).await?;

        let schema_adapted: Arc<dyn TableProvider> = if let Some(custom_schema) = custom_schema {
            Arc::new(super::wrappers::SchemaAdaptingTableProvider::new(
                Arc::new(provider),
                custom_schema,
            ))
        } else {
            Arc::new(provider)
        };

        // First wrap with circuit breaker
        // DuckDB is local/authoritative, so we skip schema drift detection.
        let wrapped_provider = super::wrappers::wrap_provider(schema_adapted, cb, false);

        // Enable federation support using the SHARED federation provider from the factory.
        // This ensures the federation optimizer identifies tables as coming from the same source.
        let table_source = Arc::new(DuckDBTableSource::new(
            self.federation_provider.clone(),
            wrapped_provider.clone(),
        ));
        let federated_provider = Arc::new(
            datafusion_federation::FederatedTableProviderAdaptor::new_with_provider(
                table_source,
                wrapped_provider,
            ),
        );

        Ok(super::wrappers::wrap_concurrent(
            federated_provider,
            self.max_concurrent_queries,
        ))
    }
}

/// A custom TableSource for DuckDB that integrates with our custom federation provider.
///
/// # FIXME (#st-1234)
/// This struct duplicates logic from `SQLTableSource`. It should ideally be refactored
/// to use a more generic approach once the `datafusion-federation` API stabilizes.
pub struct DuckDBTableSource {
    federation_provider: Arc<dyn datafusion_federation::FederationProvider>,
    table_provider: Arc<dyn TableProvider>,
}

impl DuckDBTableSource {
    /// Creates a new `DuckDBTableSource` with the given federation provider and table provider.
    pub fn new(
        federation_provider: Arc<dyn datafusion_federation::FederationProvider>,
        table_provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            federation_provider,
            table_provider,
        }
    }
}

impl datafusion::logical_expr::TableSource for DuckDBTableSource {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.table_provider.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.table_provider.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.table_provider.supports_filters_pushdown(filters)
    }
}

impl datafusion_federation::FederatedTableSource for DuckDBTableSource {
    fn federation_provider(&self) -> Arc<dyn datafusion_federation::FederationProvider> {
        self.federation_provider.clone()
    }
}

/// Registers a DuckDB data source using the provided parameters.
pub async fn register_duckdb(params: SqlSourceParams) -> Result<()> {
    let connection_string = params.connection_string.clone();
    let db_path_str = connection_string.expose_secret().to_string();
    let db_path = tokio::task::spawn_blocking(move || DuckDBPath::new(db_path_str))
        .await
        .context("Blocking task panicked")?;
    let db_path_factory = db_path.clone();
    let factory = tokio::task::spawn_blocking(move || DuckDBTableFactory::new(db_path_factory))
        .await
        .context("Blocking task panicked")??
        .with_max_concurrent_queries(params.max_concurrent_queries);

    let connector = GenericSqlConnector {
        introspector: Arc::new(DuckDBIntrospector {
            db_path: SecretString::from(connection_string.clone()),
        }),
        factory: Arc::new(factory),
        schema_mapping: SchemaMappingRule::sqlite(&params.schema_mapping),
    };

    connector.register(params).await
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn test_duckdb_table_provider_bad_path() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("bad.duckdb");
        let db_path_str = db_path.to_str().unwrap();

        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(duckdb::DuckdbConnectionManager::file(db_path_str)?)?;
        let pool = Arc::new(pool);

        // Non-existent table
        let provider = DuckDBTableProvider::new(pool, "ghost".to_string()).await;
        // Provider creation might fail during schema inference if table doesn't exist
        assert!(provider.is_err());

        Ok(())
    }
}
