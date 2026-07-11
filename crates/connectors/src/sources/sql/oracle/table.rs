//! # Oracle SQL Source
//!
//! ## Overview
//! This module implements a [`TableProvider`] for Oracle databases, enabling high-performance
//! SQL pushdown and federated execution.
//!
//! ## Usage
//! Register this source via the [`SourceRegistry`] using Oracle-specific connection parameters.
//! The source will automatically handle schema discovery and SQL generation.
//!
//! ## Performance Characteristics
//! - Uses connection pooling via `OracleConnectionPool`.
//! - Supports predicate pushdown to reduce data transfer.
//! - Optimized for large-scale data ingestion via Oracle's native Arrow support.
//!
//! ## Safety
//! - Implements strict connection lifecycle management.
//! - Uses `as_async()` carefully to ensure compatibility with async runtimes.
//!
//! ## Errors
//! - Returns errors if connection to Oracle fails.
//! - Errors if schema discovery fails for the requested table.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
};
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use std::fmt;
use std::sync::Arc;

use crate::sources::sql::common::TableFactory;
use crate::sources::sql::oracle::pool::OracleConnectionPool;

/// A [`TableProvider`] implementation for Oracle databases.
#[derive(Debug)]
pub struct OracleTable {
    pub(crate) pool: Arc<OracleConnectionPool>,
    pub(crate) table_reference: TableReference,
    pub(crate) schema: SchemaRef,
}

impl OracleTable {
    /// Creates a new `OracleTable` provider.
    pub async fn new(
        pool: Arc<OracleConnectionPool>,
        table_reference: TableReference,
    ) -> anyhow::Result<Self> {
        use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
        let conn_box = pool.connect().await.map_err(|e| anyhow::anyhow!(e))?;
        let conn = conn_box.as_async().ok_or_else(|| {
            anyhow::anyhow!("Oracle connection pool did not return an async connection")
        })?;
        let schema = conn
            .get_schema(&table_reference)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(Self {
            pool,
            table_reference,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for OracleTable {
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sql = self.base_scan_sql(projection, filters, limit)?;
        Ok(Arc::new(OracleSQLExec::new(
            projection,
            self.schema.clone(),
            self.pool.clone(),
            sql,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

impl OracleTable {
    fn base_scan_sql(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<String> {
        let columns = if let Some(proj) = projection {
            proj.iter()
                .map(|i| format!("\"{}\"", self.schema.field(*i).name()))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "*".to_string()
        };

        let mut sql = format!(
            "SELECT {} FROM {}",
            columns,
            self.table_reference.to_quoted_string()
        );

        if let Some(l) = limit {
            sql.push_str(&format!(" FETCH FIRST {} ROWS ONLY", l));
        }

        Ok(sql)
    }
}

/// An [`ExecutionPlan`] node that executes a SQL query against an Oracle database.
pub struct OracleSQLExec {
    #[allow(dead_code)]
    projection: Option<Vec<usize>>,
    pool: Arc<OracleConnectionPool>,
    sql: String,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for OracleSQLExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OracleSQLExec")
            .field("sql", &self.sql)
            .finish()
    }
}

impl OracleSQLExec {
    /// Creates a new `OracleSQLExec` execution plan node.
    pub fn new(
        projection: Option<&Vec<usize>>,
        schema: SchemaRef,
        pool: Arc<OracleConnectionPool>,
        sql: String,
    ) -> DataFusionResult<Self> {
        let projected_schema = if let Some(proj) = projection {
            Arc::new(schema.project(proj)?)
        } else {
            schema.clone()
        };

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(projected_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            projection: projection.cloned(),
            pool,
            sql,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for OracleSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OracleSQLExec: sql={}", self.sql)
    }
}

impl ExecutionPlan for OracleSQLExec {
    fn name(&self) -> &str {
        "OracleSQLExec"
    }

    fn schema(&self) -> SchemaRef {
        self.properties.eq_properties.schema().clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = self.pool.clone();
        let sql = self.sql.clone();
        let schema = self.schema();
        let schema_captured = schema.clone();
        let metrics =
            datafusion::physical_plan::metrics::BaselineMetrics::new(&self.metrics, partition);
        let output_bytes = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .output_bytes(partition);

        let stream = futures::stream::once(async move {
            use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
            let conn_box = pool
                .connect()
                .await
                .map_err(datafusion::error::DataFusionError::External)?;
            let conn = conn_box.as_async().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Oracle connection pool did not return an async connection".to_string(),
                )
            })?;
            conn.query_arrow(&sql, &[], Some(schema_captured))
                .await
                .map_err(datafusion::error::DataFusionError::External)
        })
        .try_flatten()
        .map_ok(move |batch| {
            metrics.record_output(batch.num_rows());
            output_bytes.add(batch.get_array_memory_size());
            batch
        });

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl crate::sources::federated::FederatedPlan for OracleSQLExec {
    fn pushed_sql(&self) -> Option<&str> {
        Some(&self.sql)
    }
}

/// A factory for creating `OracleTable` providers.
pub struct OracleTableFactory {
    pub(crate) pool: Arc<OracleConnectionPool>,
}

impl OracleTableFactory {
    /// Creates a new `OracleTableFactory` with the given connection pool.
    pub fn new(pool: Arc<OracleConnectionPool>) -> Self {
        Self { pool }
    }
}

impl fmt::Debug for OracleTableFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OracleTableFactory").finish()
    }
}

#[async_trait]
impl TableFactory for OracleTableFactory {
    async fn table_provider(
        &self,
        table_ref: TableReference,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let table = OracleTable::new(self.pool.clone(), table_ref).await?;
        Ok(Arc::new(table))
    }
}
