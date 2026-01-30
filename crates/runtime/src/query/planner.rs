use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner as DFQueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_federation::FederatedPlanner;
use std::fmt;
use std::sync::Arc;
use strake_sql::schema_adapter::SchemaAdapterPlanner;

/// Custom QueryPlanner that integrates Strake's federation capabilities.
///
/// It extends the DefaultPhysicalPlanner with:
/// 1. FederatedPlanner: Handles execution of remote plan fragments
/// 2. SchemaAdapterPlanner: Adapts remote schemas to local expectations
pub struct QueryPlanner {
    physical_planner: Arc<dyn PhysicalPlanner>,
}

// Manual Debug implementation because Arc<dyn PhysicalPlanner> does not implement Debug
impl fmt::Debug for QueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryPlanner")
            .field("physical_planner", &"Arc<dyn PhysicalPlanner>")
            .finish()
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryPlanner {
    pub fn new() -> Self {
        let extensions: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(FederatedPlanner::new()),
            Arc::new(SchemaAdapterPlanner),
        ];

        Self {
            physical_planner: Arc::new(DefaultPhysicalPlanner::with_extension_planners(extensions)),
        }
    }
}

#[async_trait]
impl DFQueryPlanner for QueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
