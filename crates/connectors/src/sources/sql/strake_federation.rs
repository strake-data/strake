//! # Shared Federation Provider for SQL Dialects
//!
//! Provides a unified federation architecture for Strake SQL connectors.
//!
//! ## Usage
//!
//! Dialect-specific connectors (e.g., Oracle, Postgres) register themselves by
//! creating a `StrakeFederationProvider` with their respective `SQLExecutor`.
//!
//! ## Performance Characteristics
//!
//! - **Pushdown**: Uses `strake_sql` for high-fidelity SQL generation, enabling pushdown of joins, aggregations, and complex filters.
//! - **Normalization**: Automatically strips table qualifiers and normalizes identifiers to match source-specific scoping rules.
//!
//! ## Safety
//!
//! - **Thread Safety**: `GenericFederatedExec` captures only the SQL string and executor, ensuring compatibility with DataFusion's multi-threaded execution.
//! - **Soundness**: Avoids holding non-`Sync` streams across thread boundaries.
//!
//! ## Errors
//!
//! - **SQL Generation**: Returns `DataFusionError::Execution` if `strake_sql` fails to generate valid SQL.
//! - **Execution**: Propagates errors from the underlying `SQLExecutor`.

use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use datafusion_federation::sql::SQLExecutor;
use datafusion_federation::{
    FederatedPlanNode, FederatedTableSource, FederationPlanner, FederationProvider,
};
use futures::StreamExt;
use std::sync::Arc;

use super::common::SqlDialect;

/// A shared federation provider for all Strake SQL dialects.
#[derive(Debug)]
pub struct StrakeFederationProvider {
    executor: Arc<dyn SQLExecutor>,
    dialect: SqlDialect,
}

impl StrakeFederationProvider {
    pub fn new(executor: Arc<dyn SQLExecutor>, dialect: SqlDialect) -> Self {
        Self { executor, dialect }
    }
}

impl FederationProvider for StrakeFederationProvider {
    fn name(&self) -> &str {
        self.dialect.as_str()
    }

    fn compute_context(&self) -> Option<String> {
        self.executor.compute_context()
    }

    fn optimizer(&self) -> Option<Arc<datafusion::optimizer::optimizer::Optimizer>> {
        let rule = Arc::new(StrakeFederationOptimizerRule::new(
            self.executor.clone(),
            self.dialect.clone(),
        ));
        Some(Arc::new(
            datafusion::optimizer::optimizer::Optimizer::with_rules(vec![rule]),
        ))
    }
}

/// Custom Optimizer Rule for Strake SQL federation.
#[derive(Debug)]
pub struct StrakeFederationOptimizerRule {
    planner: Arc<StrakeFederationPlanner>,
    dialect: SqlDialect,
}

impl StrakeFederationOptimizerRule {
    pub fn new(executor: Arc<dyn SQLExecutor>, dialect: SqlDialect) -> Self {
        Self {
            planner: Arc::new(StrakeFederationPlanner::new(executor, dialect.clone())),
            dialect,
        }
    }
}

impl OptimizerRule for StrakeFederationOptimizerRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<datafusion::common::tree_node::Transformed<LogicalPlan>> {
        if let LogicalPlan::Extension(Extension { ref node }) = plan
            && node.name() == "Federated"
        {
            return Ok(Transformed::no(plan));
        }

        // We need to check if the plan exclusively belongs to our provider.
        if !is_federated_plan(&plan, self.dialect.as_str())? {
            return Ok(datafusion::common::tree_node::Transformed::no(plan));
        }

        // Use the plan as-is. strake_sql will handle dialect-specific normalization.
        let normalized_plan = plan.clone();

        // Wrap the plan in our Federated extension node.
        let fed_plan = FederatedPlanNode::new(normalized_plan, self.planner.clone());
        Ok(datafusion::common::tree_node::Transformed::yes(
            LogicalPlan::Extension(Extension {
                node: Arc::new(fed_plan),
            }),
        ))
    }

    fn name(&self) -> &str {
        "strake_federation_rule"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }
}

/// A shared federated planner that uses `strake_sql` for SQL generation.
#[derive(Debug)]
pub struct StrakeFederationPlanner {
    executor: Arc<dyn SQLExecutor>,
    dialect: SqlDialect,
}

impl StrakeFederationPlanner {
    pub fn new(executor: Arc<dyn SQLExecutor>, dialect: SqlDialect) -> Self {
        Self { executor, dialect }
    }
}

#[async_trait]
impl FederationPlanner for StrakeFederationPlanner {
    async fn plan_federation(
        &self,
        node: &FederatedPlanNode,
        _session_state: &datafusion::execution::context::SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let plan = node.plan();

        // Use strake_sql to generate dialect-specific SQL
        let sql = strake_sql::sql_gen::get_sql_for_plan(plan, self.dialect.as_str())
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Federation SQL generation failed for {}: {}",
                    self.dialect.as_str(),
                    e
                ))
            })?
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to generate SQL for {} plan",
                    self.dialect.as_str()
                ))
            })?;

        tracing::info!(target: "federation", sql = %sql, dialect = %self.dialect.as_str(), "Generated federated SQL using strake_sql");

        let schema = plan.schema().inner().clone();

        // Actual execute() must occur inside ExecutionPlan::execute().
        Ok(Arc::new(StrakeFederationExec::new(
            sql,
            self.executor.clone(),
            schema,
        )) as Arc<dyn ExecutionPlan>)
    }
}

fn is_federated_plan(plan: &LogicalPlan, provider_name: &str) -> datafusion::error::Result<bool> {
    use datafusion::datasource::TableProvider;
    use datafusion_federation::FederatedTableProviderAdaptor;

    let mut has_source = false;
    let mut all_sources = true;

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let mut is_match = false;

            fn check_provider(provider: &Arc<dyn TableProvider>, provider_name: &str) -> bool {
                let any = provider.as_any();

                if let Some(w) = any.downcast_ref::<crate::sources::sql::wrappers::MetadataEnrichedTableProvider>() {
                    return check_provider(&w.inner(), provider_name);
                }
                if let Some(w) = any.downcast_ref::<crate::sources::sql::wrappers::ConcurrencyLimitedTableProvider>() {
                    return check_provider(&w.inner(), provider_name);
                }
                if let Some(w) = any.downcast_ref::<crate::resilience::circuit_breaker::CircuitBreakerTableProvider>() {
                    return check_provider(&w.inner(), provider_name);
                }
                if let Some(w) = any.downcast_ref::<crate::sources::schema_drift::SchemaDriftTableProvider>() {
                    return check_provider(&w.inner(), provider_name);
                }
                if let Some(source) = any.downcast_ref::<datafusion_federation::sql::SQLTableSource>()
                    && source.federation_provider().name() == provider_name
                {
                    return true;
                }
                if let Some(source) = any.downcast_ref::<StrakeTableSource>()
                    && source.federation_provider().name() == provider_name
                {
                    return true;
                }
                if let Some(adaptor) = any.downcast_ref::<FederatedTableProviderAdaptor>()
                    && adaptor.source.federation_provider().name() == provider_name
                {
                    return true;
                }
                if let Some(adaptor) = any.downcast_ref::<FederatedTableProviderAdaptor>()
                    && let Some(inner) = adaptor.table_provider.as_ref()
                {
                    return check_provider(inner, provider_name);
                }
                false
            }

            if let Some(default_source) = scan.source.as_any().downcast_ref::<datafusion::datasource::DefaultTableSource>()
                && check_provider(&default_source.table_provider, provider_name)
            {
                is_match = true;
            } else if let Some(adaptor) = scan.source.as_any().downcast_ref::<FederatedTableProviderAdaptor>()
                && adaptor.source.federation_provider().name() == provider_name
            {
                is_match = true;
            } else if let Some(source) = scan.source.as_any().downcast_ref::<datafusion_federation::sql::SQLTableSource>()
                && source.federation_provider().name() == provider_name
            {
                is_match = true;
            } else if let Some(source) = scan.source.as_any().downcast_ref::<crate::sources::sql::duckdb::DuckDBTableSource>()
                && source.federation_provider().name() == provider_name
            {
                is_match = true;
            } else if let Some(source) = scan.source.as_any().downcast_ref::<StrakeTableSource>()
                && source.federation_provider().name() == provider_name
            {
                is_match = true;
            }

            if is_match {
                has_source = true;
            } else {
                all_sources = false;
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;

    Ok(has_source && all_sources)
}

/// Generic execution plan that runs a federated SQL query.
pub struct StrakeFederationExec {
    sql: String,
    executor: Arc<dyn SQLExecutor>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    properties: Arc<datafusion::physical_plan::PlanProperties>,
    metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for StrakeFederationExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrakeFederationExec")
            .field("sql", &self.sql)
            .field("schema", &self.schema)
            .finish()
    }
}

impl StrakeFederationExec {
    pub fn new(
        sql: String,
        executor: Arc<dyn SQLExecutor>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let properties = datafusion::physical_plan::PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            sql,
            executor,
            schema,
            properties: Arc::new(properties),
            metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
        }
    }
}

impl datafusion::physical_plan::DisplayAs for StrakeFederationExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "StrakeFederationExec({}): sql={}",
            self.executor.name().to_lowercase(),
            self.sql
        )
    }
}

impl ExecutionPlan for StrakeFederationExec {
    fn name(&self) -> &str {
        "StrakeFederationExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let metrics =
            datafusion::physical_plan::metrics::BaselineMetrics::new(&self.metrics, partition);
        let output_bytes = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .output_bytes(partition);

        let stream = self.executor.execute(&self.sql, self.schema.clone(), &[])?;

        let schema = self.schema.clone();
        let stream = stream.map(move |batch| {
            if let Ok(ref b) = batch {
                metrics.record_output(b.num_rows());
                output_bytes.add(b.get_array_memory_size());
            }
            batch
        });

        // Make Send explicit for robust metrics handling
        let sendable_stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        );

        Ok(sendable_stream)
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Custom Federated Table Source that works with StrakeFederationProvider.
#[derive(Debug)]
pub struct StrakeTableSource {
    provider: Arc<StrakeFederationProvider>,
    table_name: TableReference,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl StrakeTableSource {
    pub fn new(
        provider: Arc<StrakeFederationProvider>,
        table_name: TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        Self {
            provider,
            table_name,
            schema,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn table_name(&self) -> &TableReference {
        &self.table_name
    }
}

impl datafusion::logical_expr::TableSource for StrakeTableSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
}

impl FederatedTableSource for StrakeTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        self.provider.clone()
    }
}
