//! Stateless query execution pipeline.
//!
//! This module provides the `QueryPipeline` which handles the final physical
//! planning and execution of DataFusion logical plans.
//!
//! # Usage
//!
//! ```rust
//! // let pipeline = QueryPipeline::new(context);
//! // let (schema, stream) = pipeline.execute_plan(plan, &collector).await?;
//! ```
//!
//! # Performance Characteristics
//!
//! Query execution is purely CPU-bound at this layer, as it delegates actual I/O
//! to the underlying DataFusion execution plan and registered connectors.
//!
//! # Safety
//!
//! This module uses no unsafe code. It relies on DataFusion's safety guarantees
//! for memory management and query execution.
//!
//! # Errors
//!
//! Returns errors if:
//! - Physical planning fails (e.g., unsupported operators).
//! - Execution fails (e.g., connector connectivity issues).

use anyhow::{Context, Result};

use arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use strake_common::warnings::WarningCollector;

/// A stateless pipeline for executing DataFusion logical plans.
///
/// The `QueryPipeline` is a deep module responsible for the physical execution
/// of a query within a given `SessionContext`. It does not handle caching or
/// orchestration policies.
pub struct QueryPipeline {
    context: SessionContext,
}

impl QueryPipeline {
    /// Create a new query pipeline.
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }

    /// Execute a logical plan and return a streaming result.
    ///
    /// This method performs the final physical planning and execution.
    /// It uses the `WarningCollector` scoped to the execution to capture
    /// any runtime warnings from connectors.
    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
        collector: &WarningCollector,
    ) -> Result<(SchemaRef, SendableRecordBatchStream)> {
        strake_common::warnings::QUERY_WARNINGS
            .scope(collector.inner(), async {
                let df = self
                    .context
                    .execute_logical_plan(plan)
                    .await
                    .context("Failed to execute logical plan")?;

                let df_stream = df.execute_stream().await?;
                let schema = df_stream.schema();

                Ok::<(SchemaRef, SendableRecordBatchStream), anyhow::Error>((schema, df_stream))
            })
            .await
    }

    /// Returns the underlying SessionContext.
    pub fn context(&self) -> &SessionContext {
        &self.context
    }
}

/// A trait for mocking the query pipeline in orchestrator tests.
#[async_trait::async_trait]
pub trait Pipeline: Send + Sync {
    /// Execute a logical plan and return a streaming result.
    async fn execute(
        &self,
        plan: LogicalPlan,
        collector: &WarningCollector,
    ) -> Result<(SchemaRef, SendableRecordBatchStream)>;
}

#[async_trait::async_trait]
impl Pipeline for QueryPipeline {
    async fn execute(
        &self,
        plan: LogicalPlan,
        collector: &WarningCollector,
    ) -> Result<(SchemaRef, SendableRecordBatchStream)> {
        self.execute_plan(plan, collector).await
    }
}
