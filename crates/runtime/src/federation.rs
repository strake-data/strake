//! Core query orchestration engine.
//!
//! The `FederationEngine` is the central entry point for executing distributed queries.
//! It manages session state, planning, and delegates execution to the `ExecutionOrchestrator`.
//!
//! # Usage
//!
//! ```rust
//! # use std::sync::Arc;
//! # use strake_runtime::federation::{FederationEngine, FederationEngineOptions};
//! # use strake_common::config::Config;
//! # async fn example() -> anyhow::Result<()> {
//! // Initialization requires context, cache, and configuration
//! // let options = FederationEngineOptions { ... };
//! // let engine = FederationEngine::new(options).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Query planning is relatively fast but can be impacted by the number of
//! registered sources and the complexity of the SQL query. The engine applies
//! defensive validation to prevent execution of queries that exceed
//! resource budgets.
//!
//! # Safety
//!
//! The `FederationEngine` uses atomic counters to track active queries and
//! ensures resources are correctly released even on early query termination.
//!
//! # Errors
//!
//! Returns errors if:
//! - Source registration fails during initialization.
//! - SQL parsing or logical planning fails.
//! - The underlying execution orchestrator returns an error.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use anyhow::{Context, Result};
use datafusion::catalog::CatalogProvider;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

use datafusion_federation::FederationOptimizerRule;
use tracing::debug;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use crate::query::cache::CacheConfig as InternalCacheConfig;
use crate::query::cache::QueryCache;
use crate::query::cost_validator::CostBasedValidator;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use std::collections::HashMap;
use std::path::PathBuf;
use strake_common::config::{Config, ResourceConfig, SourceConfig};
use strake_common::models::SourceName;
use strake_connectors::sources::{self, SourceProvider, SourceRegistry};

/// Pure logic for cache determination, extracted for testability.
///
/// Returns true if the plan is allowed to be cached based on global and source-level configs.
pub(crate) fn should_cache_plan(
    plan: &LogicalPlan,
    global_enabled: bool,
    source_configs: &HashMap<SourceName, SourceConfig>,
) -> bool {
    // If global cache is disabled, we cannot cache (system not active)
    if !global_enabled {
        return false;
    }

    let mut explicit_disable = false;

    // Traverse plan to check for source-specific overrides
    // If ANY source explicitly disables caching, we respect that (safety/freshness priority)
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            // In Strake, the source name could be in the 'schema' part OR the 'table' part
            // typically depending on the connector type (e.g. database vs file).
            let names_to_check = match &scan.table_name {
                datafusion::sql::TableReference::Full { schema, table, .. } => {
                    vec![schema.as_ref(), table.as_ref()]
                }
                datafusion::sql::TableReference::Partial { schema, table } => {
                    vec![schema.as_ref(), table.as_ref()]
                }
                datafusion::sql::TableReference::Bare { table } => vec![table.as_ref()],
            };

            for name in names_to_check {
                let sn = SourceName::from(name);
                if let Some(source_config) = source_configs.get(&sn)
                    && let Some(cache_override) = &source_config.cache
                    && !cache_override.enabled
                {
                    explicit_disable = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });

    !explicit_disable
}
use strake_sql::optimizer::defensive_trace::DefensiveLimitRule;
use strake_sql::optimizer::distinct_decorrelation::CorrelatedDistinctPushdownRule;

use crate::query::orchestrator::{BudgetPolicy, CachePolicy, ExecutionPolicy};
use tokio::sync::Semaphore;

/// The main engine for executing federated queries.
pub struct FederationEngine {
    /// The DataFusion session context used to organize catalog registration, plan queries, and optimize plans.
    context: SessionContext,
    /// An atomic counter tracking the number of queries currently executing.
    active_queries: Arc<AtomicUsize>,
    /// The registry mapping source types/schemes to their corresponding catalog provider builders.
    registry: SourceRegistry,
    /// The name of the catalog managed by this engine instance.
    catalog_name: String,
    /// A global semaphore limiting the number of concurrent connections across all active queries.
    connection_budget: Arc<Semaphore>,
    /// The query result cache structure for caching query plan outputs.
    cache: QueryCache,
    /// Per-source configurations for cache overrides.
    ///
    /// NOTE: Uses `parking_lot::RwLock` for performance. Under sustained high
    /// query throughput, writer (reload) acquisition may be delayed. If reload
    /// latency becomes problematic, consider `tokio::sync::RwLock` with
    /// write-priority fair scheduling.
    source_configs: parking_lot::RwLock<HashMap<SourceName, SourceConfig>>,
    /// Global cache configuration (default)
    global_cache_config: strake_common::config::QueryCacheConfig,
    /// Threshold configurations for preventing execution of overly complex or expensive queries.
    query_limits: strake_common::config::QueryLimits,
    /// Mutex for synchronizing hot-reload processes.
    reload_lock: tokio::sync::Mutex<()>,
}

/// Configuration options for initializing the `FederationEngine`.
pub struct FederationEngineOptions {
    /// The global system configuration.
    pub config: Config,
    /// The name of the catalog managed by this engine.
    pub catalog_name: String,
    /// Thresholds for rejecting expensive queries.
    pub query_limits: strake_common::config::QueryLimits,
    /// Configuration for system resources (CPU, Memory).
    pub resource_config: ResourceConfig,
    /// Custom DataFusion configuration options.
    pub datafusion_config: HashMap<String, String>,
    /// The global concurrency permit budget.
    pub global_budget: usize,
    /// Additional logical optimizer rules to apply.
    pub extra_optimizer_rules:
        Vec<Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>>,
    /// Additional data source providers to register.
    pub extra_sources: Vec<Box<dyn SourceProvider>>,
    /// Retry settings for federated queries.
    pub retry: strake_common::config::RetrySettings,
}

impl FederationEngine {
    /// Access the underlying DataFusion `SessionContext`.
    pub fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Get the current number of active queries.
    pub fn active_queries(&self) -> usize {
        self.active_queries.load(AtomicOrdering::Relaxed)
    }

    /// Create a new `FederationEngine` with the given options.
    pub async fn new(options: FederationEngineOptions) -> Result<Self> {
        let context = Self::build_session_context(
            &options.query_limits,
            &options.catalog_name,
            options.resource_config,
            options.datafusion_config,
            options.extra_optimizer_rules,
        )?;

        // Register our custom catalog
        let catalog = Arc::new(datafusion::catalog::MemoryCatalogProvider::new());
        catalog.register_schema(
            "public",
            Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
        )?;
        context.register_catalog(&options.catalog_name, catalog);

        let mut registry = sources::default_registry(options.retry);
        for provider in options.extra_sources {
            registry.register_provider(provider);
        }

        Self::register_sources(
            &context,
            &options.catalog_name,
            &options.config.sources,
            &registry,
        )
        .await?;

        let cache_config = InternalCacheConfig {
            enabled: options.config.cache.enabled,
            directory: PathBuf::from(&options.config.cache.directory),
            max_size_mb: options.config.cache.max_size_mb,
            ttl_seconds: options.config.cache.ttl_seconds,
        };
        let cache = QueryCache::new(cache_config).await?;

        Ok(Self {
            context,
            active_queries: Arc::new(AtomicUsize::new(0)),
            registry,
            catalog_name: options.catalog_name,
            connection_budget: Arc::new(Semaphore::new(options.global_budget)),
            cache,
            source_configs: parking_lot::RwLock::new(
                options
                    .config
                    .sources
                    .iter()
                    .map(|s| (s.name.clone(), s.clone()))
                    .collect(),
            ),
            global_cache_config: options.config.cache.clone(),
            query_limits: options.query_limits,
            reload_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Get the catalog name managed by this engine.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Get the global cache configuration.
    pub fn global_cache_config(&self) -> &strake_common::config::QueryCacheConfig {
        &self.global_cache_config
    }

    /// Get the configuration for a specific data source.
    pub fn get_source_config(&self, name: &SourceName) -> Option<SourceConfig> {
        self.source_configs.read().get(name).cloned()
    }

    /// List all registered data sources.
    pub fn list_sources(&self) -> Vec<SourceConfig> {
        self.source_configs.read().values().cloned().collect()
    }

    /// Reload sources from a new config on the fly.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Schema registration in the new catalog fails.
    /// - Any source registration fails catastrophically.
    ///
    /// # Panics
    ///
    /// Cannot panic.
    pub async fn reload_sources(&self, new_sources: Vec<SourceConfig>) -> Result<()> {
        let _lock_guard = self.reload_lock.lock().await;

        // 1. Create a fresh catalog provider and populate it offline in a temporary context
        // using a clone of the live state configuration to preserve RuntimeEnv, optimizers, etc.
        let catalog = Arc::new(datafusion::catalog::MemoryCatalogProvider::new());
        catalog.register_schema(
            "public",
            Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
        )?;

        // Build the temp context with an independent CatalogList to avoid mutating the live context
        // during registration (atomic rollback guarantee).
        //
        // NOTE: The temp context shares the live RuntimeEnv (memory pool, disk manager).
        // Source registration that allocates from the memory pool will compete with
        // live queries. If this becomes problematic under memory pressure, construct
        // an independent RuntimeEnv here.
        let independent_catalog_list =
            Arc::new(datafusion::catalog::MemoryCatalogProviderList::new());
        let temp_state = SessionStateBuilder::new_from_existing(self.context.state())
            .with_catalog_list(independent_catalog_list)
            .with_query_planner(Arc::new(crate::query::planner::QueryPlanner::new()))
            .build();
        let temp_context = SessionContext::new_with_state(temp_state);
        temp_context.register_catalog(&self.catalog_name, catalog.clone());

        // 2. Register the new sources using the existing registry under the temp context.
        // If any source fails to connect, we error out without mutating the live catalog (atomic rollback).
        Self::register_sources_strict(
            &temp_context,
            &self.catalog_name,
            &new_sources,
            &self.registry,
        )
        .await?;

        // 3. Update the catalog and source configurations atomically under write lock.
        // We build the new map outside the write lock to prevent partial state on panic.
        let new_map: HashMap<SourceName, SourceConfig> = new_sources
            .into_iter()
            .map(|s| (s.name.clone(), s))
            .collect();

        {
            // NOTE: There is a brief window between register_catalog and *guard = new_map
            // where the catalog reflects the new sources but source_configs does not yet.
            // Queries that check source_configs (e.g., should_cache_query) are protected
            // by the write lock, but direct catalog access is not.
            let mut guard = self.source_configs.write();
            self.context.register_catalog(&self.catalog_name, catalog);
            *guard = new_map;
        }

        // 4. Invalidate the query cache since source endpoints or credentials may have changed.
        self.cache.invalidate_all();

        Ok(())
    }

    /// Builds a configured DataFusion SessionContext with Strake's optimizer pipeline.
    ///
    /// The optimizer pipeline order matters:
    /// 1. User-provided rules (for custom rewrites)
    /// 2. FederationOptimizerRule (routes subqueries to appropriate sources)
    /// 3. FlattenFederatedNodesRule (Hygiene: ensures nested nodes are flattened)
    /// 4. DefensiveLimitRule (safety net for unbounded queries)
    /// 5. CostBasedValidator (Safety: rejects plans exceeding resource limits)
    fn build_session_context(
        limits: &strake_common::config::QueryLimits,
        catalog_name: &str,
        resource_config: ResourceConfig,
        datafusion_config: HashMap<String, String>,
        extra_optimizer_rules: Vec<
            Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>,
        >,
    ) -> Result<SessionContext> {
        let mut session_config = datafusion::prelude::SessionConfig::new()
            .with_default_catalog_and_schema(catalog_name, "public")
            .with_information_schema(true);

        // Enable predicate pushdown and advanced Parquet features to match high-performance engines
        session_config
            .options_mut()
            .execution
            .parquet
            .pushdown_filters = true;
        session_config
            .options_mut()
            .execution
            .parquet
            .reorder_filters = true;
        session_config.options_mut().execution.parquet.pruning = true;
        session_config
            .options_mut()
            .execution
            .parquet
            .enable_page_index = true;
        session_config
            .options_mut()
            .execution
            .parquet
            .bloom_filter_on_read = true;
        session_config.options_mut().execution.batch_size = 8192;
        let _ = session_config.options_mut().set(
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
            "true",
        );
        let _ = session_config.options_mut().set(
            "datafusion.optimizer.enable_dynamic_filter_pushdown",
            "true",
        );

        // Determine target partitions: Config > Auto-detect > Default (4)
        let target_partitions = if let Some(p) = resource_config.target_partitions {
            if p == 0 {
                anyhow::bail!("target_partitions must be at least 1");
            }
            p
        } else {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        };
        session_config.options_mut().execution.target_partitions = target_partitions;

        for (key, value) in datafusion_config {
            session_config
                .options_mut()
                .set(&key, &value)
                .context(format!("Failed to set config option: {}", key))?;
        }

        let mut rt_builder = RuntimeEnvBuilder::new();

        if let Some(limit_mb) = resource_config.memory_limit_mb {
            let limit_bytes = limit_mb.checked_mul(1024 * 1024).ok_or_else(|| {
                anyhow::anyhow!("Memory limit {} MB is too large (overflow)", limit_mb)
            })?;
            // FairSpillPool spills to disk when memory is exhausted, preventing OOM
            rt_builder = rt_builder.with_memory_pool(Arc::new(FairSpillPool::new(limit_bytes)));
        } else {
            // No limit: relies on OS memory pressure handling
            let pool_limit = if cfg!(target_pointer_width = "64") {
                usize::MAX
            } else {
                // On 32-bit, limit to a conservative 2GB to avoid OOM due to address space fragmentation
                2 * 1024 * 1024 * 1024
            };
            rt_builder = rt_builder.with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_limit)));
        }

        if let Some(spill_path) = resource_config.spill_dir {
            use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
            let mode = DiskManagerMode::Directories(vec![spill_path.into()]);
            rt_builder =
                rt_builder.with_disk_manager_builder(DiskManagerBuilder::default().with_mode(mode));
        } else {
            rt_builder = rt_builder.with_disk_manager_builder(
                datafusion::execution::disk_manager::DiskManagerBuilder::default(),
            );
        }

        let runtime_env = rt_builder.build().context("Failed to build RuntimeEnv")?;

        let context = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env));
        let state = context.state();

        // Build optimizer pipeline: inherit defaults, append custom rules in order
        let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = state.optimizers().to_vec();
        for rule in extra_optimizer_rules {
            rules.push(rule);
        }

        rules.push(Arc::new(FederationOptimizerRule::new()));
        // Ensure nested federated nodes are flattened to prevent unparser failures
        rules.push(Arc::new(
            crate::optimizer::flatten_federated::FlattenFederatedNodesRule::new(),
        ));

        if resource_config.enable_correlated_distinct_pushdown {
            rules.push(Arc::new(CorrelatedDistinctPushdownRule::new()));
        }

        if let Some(limit) = limits.default_limit {
            rules.push(Arc::new(DefensiveLimitRule::new(limit)));
        }

        debug!("Optimizer rules registered:");
        for (i, rule) in rules.iter().enumerate() {
            let name = datafusion::optimizer::optimizer::OptimizerRule::name(rule.as_ref());
            debug!("  {}: {}", i, name);
        }

        // Create physical planner with extension planners registered
        // Build physical optimizer list first
        let cost_validator = Arc::new(CostBasedValidator::new(
            limits.max_output_rows,
            limits.max_scan_bytes,
        ));

        let mut physical_optimizers = state.physical_optimizers().to_vec();

        if resource_config.enable_push_down_filter {
            physical_optimizers.push(Arc::new(crate::query::physical_rules::PushDownFilter::new()));
        }

        if resource_config.enable_broadcast_join {
            physical_optimizers.push(Arc::new(
                crate::query::physical_rules::StrakeBroadcastJoinRule::new(
                    100_000,
                    50 * 1024 * 1024,
                ),
            ));
        }

        if resource_config.enable_single_node_aggregation {
            physical_optimizers.push(Arc::new(
                crate::query::physical_rules::SingleNodeAggregationRule,
            ));
        }

        if resource_config.enable_single_partition_optimizer {
            physical_optimizers.push(Arc::new(
                crate::query::physical_rules::StrakeSinglePartitionOptimizer,
            ));
        }

        physical_optimizers.push(cost_validator);

        // NOTE: SessionStateBuilder::new_from_existing does not preserve a custom QueryPlanner.
        // When using new_from_existing outside this initial build, the planner must be
        // re-registered via .with_query_planner(). See reload_sources() for an example.
        let state = SessionStateBuilder::new_from_existing(state)
            .with_optimizer_rules(rules)
            .with_query_planner(Arc::new(crate::query::planner::QueryPlanner::new()))
            .with_physical_optimizer_rules(physical_optimizers)
            .build();

        debug!("Physical optimizers registered:");
        let physical_optimizers: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] =
            state.physical_optimizers();
        for (i, opt) in physical_optimizers.iter().enumerate() {
            debug!("  {}: {}", i, opt.name());
        }

        Ok(SessionContext::new_with_state(state))
    }

    /// A core helper to register multiple data sources concurrently, returning any
    /// compilation/registration errors mapping source names to the error.
    async fn register_sources_core(
        context: &SessionContext,
        catalog: &str,
        sources: &[SourceConfig],
        registry: &SourceRegistry,
    ) -> Vec<(SourceName, anyhow::Error)> {
        let futures = sources
            .iter()
            .map(|source| registry.register_source(context, catalog, source));
        let results = futures::future::join_all(futures).await;
        results
            .into_iter()
            .enumerate()
            .filter_map(|(i, res)| res.err().map(|e| (sources[i].name.clone(), e)))
            .collect()
    }

    /// Registers data sources and logs error messages on failure.
    async fn register_sources(
        context: &SessionContext,
        catalog: &str,
        sources: &[SourceConfig],
        registry: &SourceRegistry,
    ) -> Result<()> {
        let errors = Self::register_sources_core(context, catalog, sources, registry).await;
        for (name, err) in &errors {
            tracing::error!("Failed to register source '{}': {:#}", name, err);
        }
        Ok(())
    }

    /// Strict registration that fails if any source cannot be registered.
    /// Used during hot-reload where partial state is unacceptable.
    async fn register_sources_strict(
        context: &SessionContext,
        catalog: &str,
        sources: &[SourceConfig],
        registry: &SourceRegistry,
    ) -> Result<()> {
        let errors = Self::register_sources_core(context, catalog, sources, registry).await;
        if let Some((name, first_err)) = errors.first() {
            for (curr_name, err) in &errors {
                tracing::error!(
                    "Strict registration failed for source '{}': {:#}",
                    curr_name,
                    err
                );
            }
            anyhow::bail!(
                "Failed to register source '{}': {:#} ({} of {} sources failed)",
                name,
                first_err,
                errors.len(),
                sources.len()
            );
        }
        Ok(())
    }

    /// Determine if query should be cached based on configuration
    fn should_cache_query(&self, plan: &LogicalPlan) -> bool {
        should_cache_plan(
            plan,
            self.global_cache_config.enabled,
            &self.source_configs.read(),
        )
    }

    /// Execute a SQL query and return all results as record batches.
    pub async fn execute_query(
        &self,
        sql: &str,
        user: Option<strake_common::auth::AuthenticatedUser>,
    ) -> Result<(
        arrow::datatypes::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
        Vec<String>,
    )> {
        let (schema, mut stream, collector) = self.execute_query_stream(sql, user).await?;

        // Safety limit: only collect up to 10k rows for the legacy/REST API
        // This is a temporary measure until the REST API is also fully streaming
        let mut batches = Vec::new();
        let mut row_count: usize = 0;
        let limit = self.query_limits.max_output_rows.unwrap_or(10000);

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res?;
            row_count = row_count
                .checked_add(batch.num_rows())
                .ok_or_else(|| anyhow::anyhow!("Row count overflow during result collection"))?;
            if row_count > limit {
                anyhow::bail!(
                    "Query result exceeded safety limit for materialized execution ({} rows). Please use the streaming API.",
                    limit
                );
            }
            batches.push(batch);
        }

        Ok((schema, batches, collector.take_all()))
    }

    /// Execute a SQL query and return a stream of record batches.
    pub async fn execute_query_stream(
        &self,
        sql: &str,
        user: Option<strake_common::auth::AuthenticatedUser>,
    ) -> Result<(
        arrow::datatypes::SchemaRef,
        SendableRecordBatchStream,
        strake_common::warnings::WarningCollector,
    )> {
        self.active_queries.fetch_add(1, AtomicOrdering::Relaxed);

        let outcome = async {
            let collector = strake_common::warnings::WarningCollector::new();
            let session_manager = crate::query::session::SessionManager::new(self.context.clone());
            let context = session_manager.context_for_user(user.clone(), collector.clone());

            // 1. Planning (Engine Level)
            let state = context.state();
            {
                let catalog = state.catalog_list().catalog("strake");
                tracing::debug!(
                    has_strake_catalog = catalog.is_some(),
                    "Checking catalog registration"
                );
                if let Some(cat) = catalog {
                    let schema = cat.schema("public");
                    tracing::debug!(
                        has_public_schema = schema.is_some(),
                        "Checking schema registration"
                    );
                    if let Some(sch) = schema {
                        let tables = sch.table_names();
                        tracing::debug!(
                            available_tables = ?tables,
                            "Checking table registration"
                        );
                    }
                }
            }

            let plan = state
                .create_logical_plan(sql)
                .await
                .context("Failed to create logical plan")?;

            // 2. Orchestration
            let pipeline = Arc::new(crate::query::pipeline::QueryPipeline::new(context));

            let policies: Vec<Arc<dyn ExecutionPolicy>> = vec![
                Arc::new(CachePolicy::new(self.cache.clone())) as Arc<dyn ExecutionPolicy>,
                Arc::new(BudgetPolicy::new(self.connection_budget.clone()))
                    as Arc<dyn ExecutionPolicy>,
            ];

            let orchestrator =
                crate::query::orchestrator::ExecutionOrchestrator::new(pipeline, policies);

            let timeout_seconds = self.query_limits.query_timeout_seconds.unwrap_or(300);
            let should_cache = self.should_cache_query(&plan);
            let options = crate::query::orchestrator::ExecutionOptions {
                should_cache,
                user,
                timeout: std::time::Duration::from_secs(timeout_seconds),
            };

            orchestrator
                .execute(sql, plan, options, collector.clone())
                .await
                .map(|(schema, stream)| (schema, stream, collector))
        }
        .await;

        match outcome {
            Ok((schema, stream, collector)) => {
                let wrapped_stream = Box::pin(ActiveLimitStream {
                    input: stream,
                    counter: Arc::clone(&self.active_queries),
                });
                Ok((schema, wrapped_stream, collector))
            }
            Err(e) => {
                self.active_queries.fetch_sub(1, AtomicOrdering::Relaxed);
                Err(e)
            }
        }
    }

    /// Execute a SQL query and return a trace of the execution plan.
    pub async fn execute_query_with_trace(&self, sql: &str) -> Result<String> {
        // Validate first
        let _plan = self
            .context
            .state()
            .create_logical_plan(sql)
            .await
            .context("Failed to create logical plan for trace validation")?;

        // Use the trace module to execute and report
        crate::query::trace::execute_and_report(&self.context, sql).await
    }

    /// Returns a detailed ASCII tree visualization of the execution plan.
    ///
    /// Shows federation pushdown indicators, join conditions, filter/projection
    /// details, and timing metrics when available.
    pub async fn explain_tree(&self, sql: &str) -> Result<String> {
        // Create logical plan
        let logical_plan = self
            .context
            .state()
            .create_logical_plan(sql)
            .await
            .context("Failed to create logical plan")?;

        // Optimize logical plan
        let optimized_plan = self
            .context
            .state()
            .optimize(&logical_plan)
            .context("Failed to optimize logical plan")?;

        // Create physical plan
        let physical_plan = match self
            .context
            .state()
            .create_physical_plan(&optimized_plan)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("PHYSICAL PLANNING ERROR DETAIL: {:#}", e);
                return Err(anyhow::anyhow!("Failed to create physical plan: {:#}", e));
            }
        };

        // Format as tree
        Ok(crate::query::plan_tree::format_plan_tree(&physical_plan))
    }
}

/// A stream that decrements the active query counter when dropped.
///
/// NOTE: This is a pass-through wrapper. If this is ever promoted to a full
/// ExecutionPlan node, it must implement `BaselineMetrics` (including `output_bytes`).
struct ActiveLimitStream {
    /// The inner stream to wrap.
    input: SendableRecordBatchStream,
    /// The atomic counter to decrement on drop.
    counter: Arc<AtomicUsize>,
}

impl Stream for ActiveLimitStream {
    type Item = Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ActiveLimitStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

impl Drop for ActiveLimitStream {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

#[cfg(test)]
mod prop_tests;
