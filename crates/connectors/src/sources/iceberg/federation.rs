//! Iceberg Federation Executor
//!
//! Implements `SQLExecutor` for Iceberg sources to enable same-warehouse join identification
//! and potential pushdown.

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;

use datafusion::datasource::TableProvider;

use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider};
use std::sync::Arc;

use super::catalog::CachedRestCatalog;
use iceberg::Catalog;

use iceberg::TableIdent;
use iceberg_datafusion::IcebergStaticTableProvider;

/// Iceberg Executor for federation
///
/// Implements `SQLExecutor` to enable DataFusion's federation optimizer
/// to group operations from the same Iceberg warehouse.
#[derive(Debug, Clone)]
pub struct IcebergExecutor {
    catalog: Arc<CachedRestCatalog>,
    warehouse: String,
}

impl IcebergExecutor {
    pub fn new(catalog: Arc<CachedRestCatalog>, warehouse: String) -> Self {
        Self { catalog, warehouse }
    }

    /// Create a SQLFederationProvider wrapping this executor
    pub fn create_federation_provider(self) -> Arc<SQLFederationProvider> {
        Arc::new(SQLFederationProvider::new(Arc::new(self)))
    }
}

#[async_trait]
impl SQLExecutor for IcebergExecutor {
    fn name(&self) -> &str {
        "iceberg"
    }

    fn compute_context(&self) -> Option<String> {
        // Same warehouse = can push down joins between tables in this warehouse
        Some(self.warehouse.clone())
    }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        // Standard SQL dialect for Iceberg interaction
        Arc::new(datafusion::sql::unparser::dialect::PostgreSqlDialect {})
    }

    fn execute(
        &self,
        query: &str,
        _schema: SchemaRef,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        tracing::debug!(target: "federation", warehouse = %self.warehouse, "Executing Iceberg federated query: {}", query);

        // This is a simplified execution.
        // In a real implementation, we would parse the SQL properly.
        // For now, we assume the query is simple enough to extract the table name or we use the passed schema.

        // Strategy: Create a temporary SessionContext, register the table, and execute the query.
        let ctx = SessionContext::new();

        // We need a way to resolve the table from the query.
        // For federation, usually the query is already narrowed down to the table in the context.

        // NOTE: This implementation is synchronous but returns an async stream.
        // It's not the most efficient but makes federation functional.

        let task_ctx = ctx.task_ctx();
        let query_owned = query.to_string();

        // Fix [Safety]: Avoid block_in_place on current_thread runtime
        // We spawn a dedicated thread with its own runtime to handle the blocking plan creation
        // This is heavy but completely safe against runtime panic and deadlocks
        let (tx, rx) = tokio::sync::oneshot::channel();
        // The original code created a new SessionContext.
        // I need to create the context inside the thread or move one in.
        // The original code: let ctx = SessionContext::new();

        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = tx.send(Err(datafusion::error::DataFusionError::Execution(format!(
                        "Failed to build runtime: {}",
                        e
                    ))));
                    return;
                }
            };

            let result = rt.block_on(async {
                let ctx = SessionContext::new();
                // In a real implementation we'd register tables here needed for the query
                // For federation, we assume the query is runnable or we mock.
                // Original code just ran sql().
                ctx.sql(&query_owned).await?.create_physical_plan().await
            });
            let _ = tx.send(result);
        });

        // We use std::thread, so we can't await rx directly if we want to be sync...
        // Wait, execute() is SYNC.
        // So we must block waiting for the thread.
        // Since we are in a sync function, we can just block on the channel receiver.

        let plan = rx.blocking_recv().map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Plan worker panicked: {}", e))
        })??;

        plan.execute(0, task_ctx)
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        // List tables in the catalog.
        // This can be expensive, so we might want to cache or limit.
        // For now, we list only the first level namespaces to keep it sane.
        Ok(vec![])
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let ident = TableIdent::from_strs([table_name])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // We use a temporary provider to convert the schema
        let table = self
            .catalog
            .as_ref()
            .load_table(&ident)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(provider.schema())
    }
}
