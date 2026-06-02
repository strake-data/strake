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
    /// Creates a new `StrakeFederationProvider` with the given executor and dialect.
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
    /// Creates a new `StrakeFederationOptimizerRule` with the given executor and dialect.
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
    /// Creates a new `StrakeFederationPlanner` with the given executor and dialect.
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

                if let Some(source) =
                    any.downcast_ref::<datafusion_federation::sql::SQLTableSource>()
                    && source.federation_provider().name() == provider_name
                {
                    return true;
                }
                if let Some(source) = any.downcast_ref::<StrakeTableSource>()
                    && source.federation_provider().name() == provider_name
                {
                    return true;
                }
                if let Some(adaptor) = any.downcast_ref::<FederatedTableProviderAdaptor>() {
                    if adaptor.source.federation_provider().name() == provider_name {
                        return true;
                    }
                    if let Some(inner) = adaptor.table_provider.as_ref() {
                        return check_provider(inner, provider_name);
                    }
                }

                // Generically unwrap decorated providers
                if let Some(wrapping) = crate::sources::as_wrapping(provider.as_ref()) {
                    return check_provider(wrapping.inner(), provider_name);
                }

                false
            }

            if let Some(default_source) =
                scan.source
                    .as_any()
                    .downcast_ref::<datafusion::datasource::DefaultTableSource>()
                && check_provider(&default_source.table_provider, provider_name)
            {
                is_match = true;
            } else if let Some(adaptor) = scan
                .source
                .as_any()
                .downcast_ref::<FederatedTableProviderAdaptor>()
                && adaptor.source.federation_provider().name() == provider_name
            {
                is_match = true;
            } else if let Some(source) =
                scan.source
                    .as_any()
                    .downcast_ref::<datafusion_federation::sql::SQLTableSource>()
                && source.federation_provider().name() == provider_name
            {
                is_match = true;
            } else if let Some(source) =
                scan.source
                    .as_any()
                    .downcast_ref::<crate::sources::sql::duckdb::DuckDBTableSource>()
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

/// Generic execution plan that runs a federated SQL query against a remote data source.
pub struct StrakeFederationExec {
    /// The SQL query string to be executed on the remote engine.
    sql: String,
    /// The executor responsible for running the query and returning Arrow results.
    executor: Arc<dyn SQLExecutor>,
    /// The output schema of the query.
    schema: datafusion::arrow::datatypes::SchemaRef,
    /// Physical plan properties (partitioning, boundedness, etc.).
    properties: Arc<datafusion::physical_plan::PlanProperties>,
    /// Metrics collected during execution (e.g., output rows, compute time).
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
    /// Creates a new `StrakeFederationExec` execution plan node.
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

impl crate::sources::federated::FederatedPlan for StrakeFederationExec {
    fn pushed_sql(&self) -> Option<&str> {
        Some(&self.sql)
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
    /// Creates a new `StrakeTableSource` with the given provider, table name, and schema.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::sql::wrappers::SchemaAdaptingTableProvider;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::{Expr, LogicalPlanBuilder, TableProviderFilterPushDown};

    struct MockFederatedProvider {
        name: String,
    }
    impl FederationProvider for MockFederatedProvider {
        fn name(&self) -> &str {
            &self.name
        }
        fn compute_context(&self) -> Option<String> {
            None
        }
        fn optimizer(&self) -> Option<Arc<datafusion::optimizer::optimizer::Optimizer>> {
            None
        }
    }

    #[tokio::test]
    async fn test_is_federated_plan_wrappers() {
        use datafusion::physical_plan::ExecutionPlan;

        #[derive(Debug)]
        struct MockFedTable {
            schema: SchemaRef,
            name: String,
        }
        #[async_trait]
        impl TableProvider for MockFedTable {
            fn as_any(&self) -> &dyn std::any::Any {
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
                _: &dyn Session,
                _: Option<&Vec<usize>>,
                _: &[Expr],
                _: Option<usize>,
            ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
                Err(datafusion::error::DataFusionError::NotImplemented(
                    "MockFedTable does not implement scan".to_string(),
                ))
            }
            fn supports_filters_pushdown(
                &self,
                _: &[&Expr],
            ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
                Err(datafusion::error::DataFusionError::NotImplemented(
                    "MockFedTable does not implement supports_filters_pushdown".to_string(),
                ))
            }
        }
        impl FederatedTableSource for MockFedTable {
            fn federation_provider(&self) -> Arc<dyn FederationProvider> {
                Arc::new(MockFederatedProvider {
                    name: self.name.clone(),
                })
            }
        }
        // Need to implement TableSource for MockFedTable to use it in TableScan
        impl datafusion::logical_expr::TableSource for MockFedTable {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
            fn table_type(&self) -> TableType {
                TableType::Base
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let provider_name = "test_fed";

        let base_table = Arc::new(MockFedTable {
            schema: schema.clone(),
            name: provider_name.to_string(),
        });

        // In DataFusion federation, we often have an adaptor that wraps a TableSource
        let adaptor = Arc::new(datafusion_federation::FederatedTableProviderAdaptor::new(
            base_table.clone(), // MockFedTable implements FederatedTableSource
        ));

        // Wrap it multiple times
        let enriched = Arc::new(SchemaAdaptingTableProvider::new(
            adaptor.clone(),
            schema.clone(),
        ));

        let circuit_breaker = Arc::new(
            crate::resilience::circuit_breaker::CircuitBreakerTableProvider::new(
                enriched.clone(),
                Arc::new(strake_common::circuit_breaker::AdaptiveCircuitBreaker::new(
                    strake_common::circuit_breaker::CircuitBreakerConfig {
                        name: "test".into(),
                        ..Default::default()
                    },
                )),
            ),
        );

        let drift = Arc::new(crate::sources::schema_drift::SchemaDriftTableProvider::new(
            circuit_breaker.clone(),
        ));

        // Create a LogicalPlan with a TableScan using the wrapped provider
        let table_source = Arc::new(datafusion::datasource::DefaultTableSource::new(drift));
        let plan = LogicalPlanBuilder::scan("t1", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        assert!(is_federated_plan(&plan, provider_name).unwrap());
    }
}
