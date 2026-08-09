//! # Oracle SQL Source
//!
//! Provides a [`TableProvider`] that pushes down filters and limits to Oracle databases.
//!
//! ## Overview
//! This module implements a [`TableProvider`] for Oracle databases, enabling high-performance
//! SQL pushdown and federated execution.
//!
//! ## Usage
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use std::sync::Arc;
//! use strake_connectors::sources::sql::common::TableFactory;
//! use strake_connectors::sources::sql::oracle::pool::OracleConnectionPool;
//! use strake_connectors::sources::sql::oracle::table::OracleTableFactory;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = Arc::new(OracleConnectionPool::new("oracle://user:pass@localhost:1521/XE", 4).await?);
//! let factory = OracleTableFactory::new(pool.clone());
//! let ctx = SessionContext::new();
//! ctx.register_table("orders", factory.table_provider("sales.orders".into()).await?)?;
//! let df = ctx.sql("SELECT * FROM orders WHERE status = 'ACTIVE' LIMIT 10").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Characteristics
//! - Uses connection pooling via `OracleConnectionPool`.
//! - Supports predicate pushdown to reduce data transfer.
//! - Optimized for large-scale data ingestion via Oracle's native Arrow support.
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

use crate::sources::sql::common::{SqlDialect, TableFactory};
use crate::sources::sql::oracle::pool::OracleConnectionPool;

/// A [`TableProvider`] implementation for Oracle databases.
#[derive(Debug)]
pub struct OracleTable {
    /// Connection pool used to execute queries against Oracle.
    pub(crate) pool: Arc<OracleConnectionPool>,
    /// Fully qualified or partial table reference for the source table.
    pub(crate) table_reference: TableReference,
    /// Arrow schema of the Oracle table.
    pub(crate) schema: SchemaRef,
}

impl OracleTable {
    /// Creates a new `OracleTable` provider.
    ///
    /// # Errors
    /// Returns an error if connecting to the Oracle database fails or if schema
    /// discovery for `table_reference` cannot be performed.
    ///
    /// # Panics
    /// Cannot panic under normal execution.
    ///
    /// # Examples
    /// ```rust,no_run
    /// use datafusion::sql::TableReference;
    /// use std::sync::Arc;
    /// use strake_connectors::sources::sql::oracle::pool::OracleConnectionPool;
    /// use strake_connectors::sources::sql::oracle::table::OracleTable;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = Arc::new(OracleConnectionPool::new("oracle://...", 1).await?);
    /// let table = OracleTable::new(pool, TableReference::bare("orders")).await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Scans the Oracle table with optional column projections, filters, and row limit.
    ///
    /// Unparses translatable DataFusion filter expressions and limit bounds into
    /// a remote Oracle SQL statement (`SELECT ... WHERE ... FETCH FIRST n ROWS ONLY`),
    /// delegating physical execution to [`OracleSQLExec`].
    ///
    /// # Errors
    /// Returns an error if any pushed filter expression fails SQL unparsing.
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

    /// Evaluates which DataFusion filter expressions can be pushed down to Oracle.
    ///
    /// Uses [`strake_sql::dialects::OracleDialect`] and DataFusion's SQL [`datafusion::sql::unparser::Unparser`]
    /// to test if each filter expression can be rendered as valid Oracle SQL. Expressions
    /// that unparse successfully return [`TableProviderFilterPushDown::Exact`], while unsupported
    /// expressions return [`TableProviderFilterPushDown::Unsupported`].
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if strake_sql::sql_gen::unparse_expr_to_sql(f, SqlDialect::Oracle.as_str()).is_ok()
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

impl OracleTable {
    fn base_scan_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
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

        let mut where_clauses = Vec::new();
        for filter in filters {
            let expr_sql =
                strake_sql::sql_gen::unparse_expr_to_sql(filter, SqlDialect::Oracle.as_str())
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to unparse filter expression for Oracle pushdown: {e}"
                        ))
                    })?;
            where_clauses.push(expr_sql);
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        if let Some(l) = limit {
            sql.push_str(&format!(" FETCH FIRST {} ROWS ONLY", l));
        }

        Ok(sql)
    }
}

/// An [`ExecutionPlan`] node that executes a generated SQL query against an Oracle database.
pub struct OracleSQLExec {
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
    ///
    /// # Errors
    /// Returns a [`datafusion::error::DataFusionError`] if schema projection fails.
    ///
    /// # Panics
    /// Cannot panic under normal execution.
    ///
    /// # Examples
    /// ```rust,no_run
    /// use datafusion::arrow::datatypes::Schema;
    /// use std::sync::Arc;
    /// use strake_connectors::sources::sql::oracle::pool::OracleConnectionPool;
    /// use strake_connectors::sources::sql::oracle::table::OracleSQLExec;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = Arc::new(OracleConnectionPool::new("oracle://...", 1).await?);
    /// let schema = Arc::new(Schema::empty());
    /// let exec = OracleSQLExec::new(None, schema, pool, "SELECT 1 FROM DUAL".into())?;
    /// # Ok(())
    /// # }
    /// ```
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
        let elapsed_compute = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .elapsed_compute(partition);

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
            let _timer = elapsed_compute.timer();
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
    /// Connection pool shared by table providers created by this factory.
    pub(crate) pool: Arc<OracleConnectionPool>,
}

impl OracleTableFactory {
    /// Creates a new [`OracleTableFactory`] with the given connection pool.
    ///
    /// # Errors
    /// Cannot return an error.
    ///
    /// # Panics
    /// Cannot panic.
    ///
    /// # Examples
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use strake_connectors::sources::sql::oracle::pool::OracleConnectionPool;
    /// use strake_connectors::sources::sql::oracle::table::OracleTableFactory;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = Arc::new(OracleConnectionPool::new("oracle://...", 1).await?);
    /// let factory = OracleTableFactory::new(pool);
    /// # Ok(())
    /// # }
    /// ```
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
    use datafusion::sql::TableReference;

    async fn create_test_table() -> OracleTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ORDER_ID", DataType::Int64, false),
            Field::new("CUSTOMER_ID", DataType::Int64, false),
            Field::new("STATUS", DataType::Utf8, true),
            Field::new("REGION_CODE", DataType::Utf8, true),
            Field::new("AMOUNT", DataType::Float64, true),
            Field::new(
                "CREATED_AT",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let table_reference = TableReference::partial("sales_dwh", "ORDERS");

        let pool = Arc::new(
            OracleConnectionPool::new("oracle://user:pass@localhost:1521/FREEPDB1", 1)
                .await
                .unwrap(),
        );

        OracleTable {
            pool,
            table_reference,
            schema,
        }
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_single_and_multiple() {
        let table = create_test_table().await;

        let f1 = col("CUSTOMER_ID").eq(lit(1001));
        let f2 = col("STATUS").eq(lit("ACTIVE"));
        let f3 = col("REGION_CODE").in_list(vec![lit("US"), lit("EU")], false);
        let f4 = col("AMOUNT").is_not_null();

        let res = table
            .supports_filters_pushdown(&[&f1, &f2, &f3, &f4])
            .unwrap();

        assert_eq!(
            res,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        );
    }

    #[tokio::test]
    async fn test_base_scan_sql_with_single_equality_and_limit() {
        let table = create_test_table().await;

        let filter = col("\"CUSTOMER_ID\"").eq(lit(1001));
        let sql = table
            .base_scan_sql(Some(&vec![0, 1]), &[filter], Some(1))
            .unwrap();

        assert_eq!(
            sql,
            "SELECT \"ORDER_ID\", \"CUSTOMER_ID\" FROM sales_dwh.\"ORDERS\" WHERE (\"CUSTOMER_ID\" = 1001) FETCH FIRST 1 ROWS ONLY"
        );
    }

    #[tokio::test]
    async fn test_base_scan_sql_with_multiple_filters_and_limit() {
        let table = create_test_table().await;

        let f1 = col("\"STATUS\"").eq(lit("ACTIVE"));
        let f2 = col("\"REGION_CODE\"").in_list(vec![lit("US"), lit("EU")], false);
        let f3 = col("\"AMOUNT\"").is_not_null();

        let sql = table
            .base_scan_sql(Some(&vec![2, 3, 4]), &[f1, f2, f3], Some(500))
            .unwrap();

        assert_eq!(
            sql,
            "SELECT \"STATUS\", \"REGION_CODE\", \"AMOUNT\" FROM sales_dwh.\"ORDERS\" WHERE (\"STATUS\" = 'ACTIVE') AND \"REGION_CODE\" IN ('US', 'EU') AND \"AMOUNT\" IS NOT NULL FETCH FIRST 500 ROWS ONLY"
        );
    }

    #[tokio::test]
    async fn test_oracle_mixed_filters_pushdown_and_residuals() {
        let table = create_test_table().await;

        let f1 = col("\"STATUS\"").eq(lit("ACTIVE"));
        // Custom scalar function without SQL unparser mapping returns Err in expr_to_sql
        let f_unsupported = col("x").eq(Expr::Literal(
            datafusion::scalar::ScalarValue::Struct(Arc::new(
                datafusion::arrow::array::StructArray::new_null(
                    datafusion::arrow::datatypes::Fields::empty(),
                    1,
                ),
            )),
            None,
        ));
        let f2 = col("\"REGION_CODE\"").in_list(vec![lit("US"), lit("EU")], false);

        let pushdown_res = table
            .supports_filters_pushdown(&[&f1, &f_unsupported, &f2])
            .unwrap();

        assert_eq!(
            pushdown_res,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Exact,
            ]
        );

        // DataFusion passes only pushed Exact filters to scan/base_scan_sql.
        // The unsupported filter remains as a residual local filter above the scan.
        let sql = table
            .base_scan_sql(Some(&vec![2, 3]), &[f1, f2], Some(500))
            .unwrap();

        assert_eq!(
            sql,
            "SELECT \"STATUS\", \"REGION_CODE\" FROM sales_dwh.\"ORDERS\" WHERE (\"STATUS\" = 'ACTIVE') AND \"REGION_CODE\" IN ('US', 'EU') FETCH FIRST 500 ROWS ONLY"
        );
    }

    #[tokio::test]
    async fn test_base_scan_sql_with_temporal_filters() {
        let table = create_test_table().await;

        let filter = col("\"CREATED_AT\"").gt(lit(
            datafusion::scalar::ScalarValue::TimestampMicrosecond(Some(1785649509392963), None),
        ));
        let sql = table
            .base_scan_sql(Some(&vec![0, 5]), &[filter], Some(10))
            .unwrap();

        assert_eq!(
            sql,
            "SELECT \"ORDER_ID\", \"CREATED_AT\" FROM sales_dwh.\"ORDERS\" WHERE \"CREATED_AT\" > TO_TIMESTAMP('2026-08-02 05:45:09.392963', 'YYYY-MM-DD HH24:MI:SS.FF') FETCH FIRST 10 ROWS ONLY"
        );
    }
}
