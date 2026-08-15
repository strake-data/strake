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

        let combined = filters.iter().cloned().reduce(|acc, f| acc.and(f));
        if let Some(predicate) = combined {
            let expr_sql =
                strake_sql::sql_gen::unparse_expr_to_sql(&predicate, SqlDialect::Oracle.as_str())
                    .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to unparse filter expression for Oracle pushdown: {e}"
                    ))
                })?;
            sql.push_str(" WHERE ");
            sql.push_str(&expr_sql);
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
            "SELECT \"ORDER_ID\", \"CUSTOMER_ID\" FROM sales_dwh.\"ORDERS\" WHERE \"CUSTOMER_ID\" = 1001 FETCH FIRST 1 ROWS ONLY"
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
            "SELECT \"STATUS\", \"REGION_CODE\", \"AMOUNT\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" = 'ACTIVE' AND \"REGION_CODE\" IN ('US', 'EU') AND \"AMOUNT\" IS NOT NULL FETCH FIRST 500 ROWS ONLY"
        );
    }

    #[tokio::test]
    async fn test_oracle_mixed_filters_pushdown_and_residuals() {
        let table = create_test_table().await;

        let f1 = col("\"STATUS\"").eq(lit("ACTIVE"));
        // Struct literal with a non-null value has no SQL unparser mapping and
        // returns Err in expr_to_sql, so it must remain a residual filter.
        // (Note: an all-null Struct would be rendered as SQL NULL via is_null().)
        let f_unsupported = col("x").eq(Expr::Literal(
            datafusion::scalar::ScalarValue::Struct(Arc::new(
                datafusion::arrow::array::StructArray::from(vec![(
                    Arc::new(datafusion::arrow::datatypes::Field::new(
                        "a",
                        datafusion::arrow::datatypes::DataType::Int32,
                        false,
                    )),
                    Arc::new(datafusion::arrow::array::Int32Array::from(vec![1]))
                        as datafusion::arrow::array::ArrayRef,
                )]),
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
            "SELECT \"STATUS\", \"REGION_CODE\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" = 'ACTIVE' AND \"REGION_CODE\" IN ('US', 'EU') FETCH FIRST 500 ROWS ONLY"
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

    #[tokio::test]
    async fn test_base_scan_sql_or_chains_parenthesised() {
        let table = create_test_table().await;

        let or_chain_1 = col("\"STATUS\"")
            .eq(lit("ACTIVE"))
            .or(col("\"STATUS\"").eq(lit("PENDING")));
        let or_chain_2 = col("\"REGION_CODE\"")
            .eq(lit("US"))
            .or(col("\"REGION_CODE\"").eq(lit("EU")));

        let sql = table
            .base_scan_sql(Some(&vec![2, 3]), &[or_chain_1, or_chain_2], None)
            .unwrap();

        assert_eq!(
            sql,
            "SELECT \"STATUS\", \"REGION_CODE\" FROM sales_dwh.\"ORDERS\" WHERE (\"STATUS\" = 'ACTIVE' OR \"STATUS\" = 'PENDING') AND (\"REGION_CODE\" = 'US' OR \"REGION_CODE\" = 'EU')"
        );
    }

    /// Bug report §1: `x IN (1)` — single-element IN preserved as-is.
    #[tokio::test]
    async fn test_base_scan_sql_single_element_in_list() {
        let table = create_test_table().await;

        let f = col("\"STATUS\"").in_list(vec![lit("ACTIVE")], false);
        let sql = table.base_scan_sql(None, &[f], None).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" IN ('ACTIVE')"
        );
    }

    /// Bug report §1: `x IN (1, 2, 3)` — multi-element IN preserved as-is by the unparser.
    #[tokio::test]
    async fn test_base_scan_sql_three_element_in_list() {
        let table = create_test_table().await;

        let f =
            col("\"STATUS\"").in_list(vec![lit("ACTIVE"), lit("PENDING"), lit("CLOSED")], false);
        let sql = table.base_scan_sql(Some(&vec![2]), &[f], None).unwrap();
        assert_eq!(
            sql,
            "SELECT \"STATUS\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" IN ('ACTIVE', 'PENDING', 'CLOSED')"
        );
    }

    /// Bug report §1: three OR chains combined by AND — the triple-filter case.
    #[tokio::test]
    async fn test_base_scan_sql_three_or_chains_under_and() {
        let table = create_test_table().await;

        let f1 = col("\"STATUS\"")
            .eq(lit("A"))
            .or(col("\"STATUS\"").eq(lit("B")));
        let f2 = col("\"REGION_CODE\"")
            .eq(lit("US"))
            .or(col("\"REGION_CODE\"").eq(lit("EU")));
        let f3 = col("\"AMOUNT\"")
            .gt(lit(100.0))
            .or(col("\"AMOUNT\"").lt(lit(10.0)));

        let sql = table
            .base_scan_sql(Some(&vec![2, 3, 4]), &[f1, f2, f3], None)
            .unwrap();

        // Every OR group must be parenthesised
        assert!(
            sql.contains("(\"STATUS\" = 'A' OR \"STATUS\" = 'B')"),
            "First OR group not parenthesised: {sql}"
        );
        assert!(
            sql.contains("(\"REGION_CODE\" = 'US' OR \"REGION_CODE\" = 'EU')"),
            "Second OR group not parenthesised: {sql}"
        );
        assert!(
            sql.contains(") AND ("),
            "AND between OR groups missing parentheses: {sql}"
        );
    }

    /// Bug report §1: `A OR (B AND C)` — mixed precedence, no wrapping needed for AND.
    #[tokio::test]
    async fn test_base_scan_sql_or_with_nested_and() {
        let table = create_test_table().await;

        // Single filter: STATUS = 'A' OR (REGION_CODE = 'US' AND AMOUNT > 100)
        let f = col("\"STATUS\"").eq(lit("A")).or(col("\"REGION_CODE\"")
            .eq(lit("US"))
            .and(col("\"AMOUNT\"").gt(lit(100.0))));

        let sql = table
            .base_scan_sql(Some(&vec![2, 3, 4]), &[f], None)
            .unwrap();

        // AND binds tighter than OR, so no parentheses are required or added.
        assert_eq!(
            sql,
            "SELECT \"STATUS\", \"REGION_CODE\", \"AMOUNT\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" = 'A' OR \"REGION_CODE\" = 'US' AND \"AMOUNT\" > 100"
        );
    }

    /// Bug report §1: single filter produces no spurious wrapping.
    #[tokio::test]
    async fn test_base_scan_sql_single_filter() {
        let table = create_test_table().await;

        let f = col("\"STATUS\"").eq(lit("ACTIVE"));
        let sql = table.base_scan_sql(Some(&vec![2]), &[f], None).unwrap();

        assert_eq!(
            sql,
            "SELECT \"STATUS\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" = 'ACTIVE'"
        );
    }

    /// Empty filters should produce no WHERE clause.
    #[tokio::test]
    async fn test_base_scan_sql_no_filters() {
        let table = create_test_table().await;

        let sql = table.base_scan_sql(None, &[], Some(10)).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM sales_dwh.\"ORDERS\" FETCH FIRST 10 ROWS ONLY"
        );
    }

    /// Bug report §1: `NOT IN` — negated IN list.
    #[tokio::test]
    async fn test_base_scan_sql_not_in_list() {
        let table = create_test_table().await;

        let f = col("\"STATUS\"").in_list(vec![lit("CANCELLED"), lit("REJECTED")], true);
        let sql = table.base_scan_sql(Some(&vec![2]), &[f], None).unwrap();
        assert_eq!(
            sql,
            "SELECT \"STATUS\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" NOT IN ('CANCELLED', 'REJECTED')"
        );
    }

    /// Bug report §1: NULL in IN list — `x IN (1, NULL)`.
    #[tokio::test]
    async fn test_base_scan_sql_null_in_in_list() {
        let table = create_test_table().await;

        let f = col("\"STATUS\"").in_list(
            vec![
                lit("ACTIVE"),
                Expr::Literal(datafusion::scalar::ScalarValue::Utf8(None), None),
            ],
            false,
        );
        let sql = table.base_scan_sql(Some(&vec![2]), &[f], None).unwrap();
        assert_eq!(
            sql,
            "SELECT \"STATUS\" FROM sales_dwh.\"ORDERS\" WHERE \"STATUS\" IN ('ACTIVE', NULL)"
        );
    }

    /// Bug report §1: escaped strings — single quotes in values.
    #[tokio::test]
    async fn test_base_scan_sql_escaped_strings() {
        let table = create_test_table().await;

        let f = col("\"STATUS\"").eq(lit("it's active"));
        let sql = table.base_scan_sql(Some(&vec![2]), &[f], None).unwrap();
        assert!(
            sql.contains("it''s active") || sql.contains("it\\'s active"),
            "Single quote not escaped in string literal: {sql}"
        );
    }

    /// Bug report §2: Full DataFusion optimizer pipeline test.
    ///
    /// Registers OracleTable with a SessionContext, runs a query with IN predicates
    /// so DataFusion's optimizer expands short IN lists to OR chains, then
    /// inspects the physical plan's OracleSQLExec to verify parenthesisation.
    /// Does NOT call `get_sql_for_plan` or `base_scan_sql` directly.
    #[tokio::test]
    async fn test_full_optimizer_pipeline_in_expansion_parenthesised() {
        use datafusion::prelude::SessionContext;

        let table = create_test_table().await;
        let ctx = SessionContext::new();
        ctx.register_table("test_orders", Arc::new(table))
            .expect("Failed to register table");

        // DataFusion 54 expands IN lists with <= 3 elements into OR chains.
        // This query MUST go through the optimizer to trigger the expansion.
        let df = ctx
            .sql(
                r#"SELECT "STATUS", "REGION_CODE"
                   FROM test_orders
                   WHERE "STATUS" IN ('ACTIVE', 'PENDING')
                     AND "REGION_CODE" IN ('US', 'EU')"#,
            )
            .await
            .expect("Failed to create logical plan");

        let physical = df
            .create_physical_plan()
            .await
            .expect("Failed to create physical plan");

        let plan_display = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        // The plan must contain OracleSQLExec with properly parenthesised SQL
        assert!(
            plan_display.contains("OracleSQLExec"),
            "OracleSQLExec not found in plan:\n{plan_display}"
        );

        // After IN->OR expansion, OR groups under AND must be parenthesised.
        // Assert the full conjunctive invariant: a regression that skips the
        // parenthesisation (or preserves the IN lists unexpanded) fails this.
        assert!(
            plan_display.contains(") AND ("),
            "OR groups under AND not parenthesised in pushed SQL:\n{plan_display}"
        );
        assert!(
            plan_display.contains("(\"STATUS\" = 'ACTIVE' OR \"STATUS\" = 'PENDING')"),
            "STATUS OR group not parenthesised in pushed SQL:\n{plan_display}"
        );
        assert!(
            plan_display.contains("(\"REGION_CODE\" = 'US' OR \"REGION_CODE\" = 'EU')"),
            "REGION_CODE OR group not parenthesised in pushed SQL:\n{plan_display}"
        );
    }
}
