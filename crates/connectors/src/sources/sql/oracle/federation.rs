//! Oracle Federation Executor
//!
//! Implements `SQLExecutor` for Oracle to enable same-source join pushdown.
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::SQLExecutor;
use futures::TryStreamExt;
use std::sync::Arc;

use crate::sources::sql::oracle::table::OracleTableFactory;
use crate::sources::sql::strake_federation::StrakeFederationProvider;

impl OracleTableFactory {
    /// Creates a federation provider for this Oracle source to enable pushdown optimizations.
    pub fn create_federation_provider(&self) -> Arc<StrakeFederationProvider> {
        Arc::new(StrakeFederationProvider::new(
            Arc::new(OracleSQLExecutor {
                pool: self.pool.clone(),
            }),
            crate::sources::sql::common::SqlDialect::Oracle,
        ))
    }
}

/// Executes SQL queries against an Oracle database for federated query execution.
pub struct OracleSQLExecutor {
    pool: Arc<crate::sources::sql::oracle::pool::OracleConnectionPool>,
}

#[async_trait]
impl SQLExecutor for OracleSQLExecutor {
    fn name(&self) -> &str {
        "OracleSQLExecutor"
    }

    fn compute_context(&self) -> Option<String> {
        // Return a unique identifier for the Oracle source to enable same-source join pushdown.
        // We use the connection string (excluding credentials if possible, but here we just have the full string).
        Some(format!("oracle:{}", self.pool.connection_string()))
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(strake_sql::dialects::OracleDialect::new())
    }

    fn logical_optimizer(&self) -> Option<datafusion_federation::sql::LogicalOptimizer> {
        None
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _params: &[Arc<dyn PhysicalExpr>],
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = self.pool.clone();
        let query = query.to_string();
        let schema_captured = schema.clone();

        tracing::info!(target: "federation", db = "[REDACTED]", "Oracle executing federated query: {}", query);

        let stream = futures::stream::once(async move {
            use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
            let conn_box = pool
                .connect()
                .await
                .map_err(datafusion::error::DataFusionError::External)?;
            let conn = conn_box.as_async().unwrap();
            conn.query_arrow(&query, &[], Some(schema_captured))
                .await
                .map_err(datafusion::error::DataFusionError::External)
        })
        .try_flatten();

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        ))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Ok(vec![])
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
        let conn_box = self
            .pool
            .connect()
            .await
            .map_err(datafusion::error::DataFusionError::External)?;
        let conn = conn_box.as_async().unwrap();
        conn.get_schema(&TableReference::from(table_name))
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    }
}
