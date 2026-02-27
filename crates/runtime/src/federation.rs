//! Core query orchestration engine.
//!
//! The `FederationEngine` is the central entry point for executing distributed queries.
//! It manages:
//!
//! 1. **Session State**: DataFusion `SessionContext` with custom configuration.
//! 2. **Query Planning**: Parsing SQL, logical planning, and optimization.
//! 3. **Resource Management**: Concurrency limits (`Semaphore`) and memory pools.
//! 4. **Caching**: Integration with the `QueryCache` for result reuse.
//!
//! # Query Lifecycle
//!
//! 1. `execute_query(sql)` called.
//! 2. **Authentication**: User context applied to session.
//! 3. **Planning**: SQL -> Logical Plan.
//! 4. **Optimization**:
//!    - `FederationOptimizerRule` routes subqueries to sources.
//!    - `DefensiveLimitRule` ensures fetch limits.
//! 5. **Caching Check**: Compute cache key, heck if cached.
//! 6. **Execution**: Run physical plan if cache miss.
//! 7. **Validation**: `CostBasedValidator` checks result size.
//!
//! # Example
//!
//! ```rust
//! // See `FederationEngine::new` for initialization
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

use datafusion_federation::FederationOptimizerRule;
use tracing::{debug, info};

use crate::query::cache::CacheConfig as InternalCacheConfig;
use crate::query::cache::QueryCache;
use crate::query::cost_validator::CostBasedValidator;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use std::collections::HashMap;
use std::path::PathBuf;
use strake_common::config::{Config, ResourceConfig, SourceConfig};
use strake_connectors::sources::{self, SourceProvider, SourceRegistry};
use strake_sql::optimizer::defensive_trace::DefensiveLimitRule;

use tokio::sync::Semaphore;

pub struct FederationEngine {
    context: SessionContext,
    active_queries: Arc<AtomicUsize>,
    _registry: SourceRegistry,
    pub catalog_name: String,
    connection_budget: Arc<Semaphore>,
    cache: QueryCache,
    /// Per-source configurations for cache overrides
    source_configs: HashMap<String, SourceConfig>,
    /// Global cache configuration (default)
    global_cache_config: strake_common::config::QueryCacheConfig,
    /// Query execution limits
    query_limits: strake_common::config::QueryLimits,
}

pub struct FederationEngineOptions {
    pub config: Config,
    pub catalog_name: String,
    pub query_limits: strake_common::config::QueryLimits,
    pub resource_config: ResourceConfig,
    pub datafusion_config: HashMap<String, String>,
    pub global_budget: usize,
    pub extra_optimizer_rules:
        Vec<Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>>,
    pub extra_sources: Vec<Box<dyn SourceProvider>>,
    pub retry: strake_common::config::RetrySettings,
}

impl FederationEngine {
    pub fn context(&self) -> &SessionContext {
        &self.context
    }

    pub fn active_queries(&self) -> usize {
        self.active_queries.load(Ordering::Relaxed)
    }

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
            _registry: registry,
            catalog_name: options.catalog_name,
            connection_budget: Arc::new(Semaphore::new(options.global_budget)),
            cache,
            source_configs: options
                .config
                .sources
                .iter()
                .map(|s| (s.name.clone(), s.clone()))
                .collect(),
            global_cache_config: options.config.cache.clone(),
            query_limits: options.query_limits,
        })
    }

    pub fn get_source_config(&self, name: &str) -> Option<&SourceConfig> {
        self.source_configs.get(name)
    }

    pub fn list_sources(&self) -> Vec<SourceConfig> {
        self.source_configs.values().cloned().collect()
    }

    /// Builds a configured DataFusion SessionContext with Strake's optimizer pipeline.
    ///
    /// The optimizer pipeline order matters:
    /// 1. User-provided rules (for custom rewrites)
    /// 2. FederationOptimizerRule (routes subqueries to appropriate sources)
    /// 3. DefensiveLimitRule (safety net for unbounded queries)
    /// 4. CostBasedValidator (rejects plans exceeding resource limits)
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

        // Enable predicate pushdown to minimize data transferred from remote sources
        session_config
            .options_mut()
            .execution
            .parquet
            .pushdown_filters = true;
        session_config.options_mut().execution.parquet.pruning = true;

        // DataFusion defaults to 0 (auto-detect), but we want deterministic behavior
        if session_config.options().execution.target_partitions == 0 {
            session_config.options_mut().execution.target_partitions = 4;
        }

        for (key, value) in datafusion_config {
            session_config
                .options_mut()
                .set(&key, &value)
                .context(format!("Failed to set config option: {}", key))?;
        }

        let mut rt_builder = RuntimeEnvBuilder::new();

        if let Some(limit_mb) = resource_config.memory_limit_mb {
            let limit_bytes = limit_mb * 1024 * 1024;
            // FairSpillPool spills to disk when memory is exhausted, preventing OOM
            rt_builder = rt_builder.with_memory_pool(Arc::new(FairSpillPool::new(limit_bytes)));
        } else {
            // No limit: relies on OS memory pressure handling
            rt_builder = rt_builder.with_memory_pool(Arc::new(GreedyMemoryPool::new(usize::MAX)));
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
        let mut optimizer_rules = state.optimizer().rules.clone();
        for rule in extra_optimizer_rules {
            optimizer_rules.push(rule);
        }

        optimizer_rules.push(Arc::new(FederationOptimizerRule::new()));
        // Ensure nested federated nodes are flattened to prevent unparser failures
        optimizer_rules.push(Arc::new(
            crate::optimizer::flatten_federated::FlattenFederatedNodesRule::new(),
        ));

        if let Some(limit) = limits.default_limit {
            optimizer_rules.push(Arc::new(DefensiveLimitRule::new(limit)));
        }

        debug!("Optimizer rules registered:");
        for (i, rule) in optimizer_rules.iter().enumerate() {
            debug!("  {}: {}", i, rule.name());
        }

        // Create physical planner with extension planners registered
        // Build physical optimizer list first
        let cost_validator = Arc::new(CostBasedValidator::new(
            limits.max_output_rows,
            limits.max_scan_bytes,
        ));

        let mut physical_optimizers = state.physical_optimizers().to_vec();
        physical_optimizers.push(cost_validator);

        // IMPORTANT: Build state in a single chain to preserve QueryPlanner registration.
        // Calling SessionStateBuilder::new_from_existing twice would lose the query planner.
        let state = SessionStateBuilder::new_from_existing(state)
            .with_optimizer_rules(optimizer_rules)
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

    async fn register_sources(
        context: &SessionContext,
        catalog: &str,
        sources: &[SourceConfig],
        registry: &SourceRegistry,
    ) -> Result<()> {
        let futures = sources
            .iter()
            .map(|source| registry.register_source(context, catalog, source));

        let results = futures::future::join_all(futures).await;
        for (i, res) in results.into_iter().enumerate() {
            if let Err(e) = res {
                tracing::error!("Failed to register source '{}': {:#}", sources[i].name, e);
                // We continue, allowing the server to start even if some sources are down
            }
        }
        Ok(())
    }

    /// Determine if query should be cached based on configuration
    fn should_cache_query(&self, plan: &LogicalPlan) -> bool {
        // If global cache is disabled, we cannot cache (system not active)
        if !self.global_cache_config.enabled {
            return false;
        }

        let mut explicit_disable = false;

        // Traverse plan to check for source-specific overrides
        // If ANY source explicitly disables caching, we respect that (safety/freshness priority)
        let _ = plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let table_name = scan.table_name.table();
                // Check if known source has cache config
                // NOTE: This uses simple name matching. Robust implementation would resolve table -> source via registry.
                if let Some(source_config) = self.source_configs.get(table_name) {
                    if let Some(cache_override) = &source_config.cache {
                        if !cache_override.enabled {
                            explicit_disable = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });

        !explicit_disable
    }

    pub async fn execute_query(
        &self,
        sql: &str,
        user: Option<strake_common::auth::AuthenticatedUser>,
    ) -> Result<(
        arrow::datatypes::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
        Vec<String>,
    )> {
        self.active_queries.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_with_cache(sql, user).await;
        self.active_queries.fetch_sub(1, Ordering::Relaxed);
        result
    }

    /// Extracted cache middleware logic to satisfy Single Responsibility Principle.
    async fn execute_with_cache(
        &self,
        sql: &str,
        user: Option<strake_common::auth::AuthenticatedUser>,
    ) -> Result<(
        arrow::datatypes::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
        Vec<String>,
    )> {
        let start = Instant::now();

        let state = self.context.state();
        let mut config = state.config().clone();

        if let Some(u) = user.clone() {
            config.options_mut().extensions.insert(u);
        }

        let collector = strake_common::warnings::WarningCollector::new();
        config.options_mut().extensions.insert(collector.clone());

        // Re-construct state to ensure QueryPlanner is present and config is updated.
        let state = SessionStateBuilder::new_from_existing(state)
            .with_config(config)
            .with_query_planner(Arc::new(crate::query::planner::QueryPlanner::new()))
            .build();

        // Create a temporary context for plan creation and execution
        let context = SessionContext::new_with_state(state);

        let _permit = self
            .connection_budget
            .clone()
            .acquire_owned()
            .await
            .context("Failed to acquire connection permit")?;

        let plan = context
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create logical plan: {}", e))?;

        // --- Cache Lookup START ---
        // Resolve effective cache configuration
        let should_cache = self.should_cache_query(&plan);
        let cache_key = crate::query::cache::CacheKey::from_plan(&plan, user.as_ref());

        let mut cached_batches_opt = None;
        if should_cache {
            cached_batches_opt = self.cache.get(&cache_key).await;
        }

        if let Some(cached_batches) = cached_batches_opt {
            let duration = start.elapsed();
            let user_id = user.as_ref().map(|u| u.id.as_str()).unwrap_or("anonymous");

            info!(
                target: "queries",
                user_id = %user_id,
                query = sql,
                duration_ms = duration.as_millis() as u64,
                rows_returned = cached_batches.iter().map(|b: &RecordBatch| b.num_rows()).sum::<usize>(),
                cache_hit = true,
                success = true
            );

            let schema = cached_batches[0].schema();
            let warnings = vec!["x-strake-cache: hit".to_string()];
            return Ok((schema, cached_batches, warnings));
        }
        // --- Cache Lookup END ---

        let timeout_seconds = self.query_limits.query_timeout_seconds.unwrap_or(300);
        let timeout_duration = std::time::Duration::from_secs(timeout_seconds);

        let my_warnings = Arc::new(std::sync::Mutex::new(Vec::new()));
        let result = strake_common::warnings::QUERY_WARNINGS
            .scope(my_warnings.clone(), async {
                tokio::time::timeout(timeout_duration, async {
                    let df = context
                        .execute_logical_plan(plan.clone())
                        .await
                        .context("Failed to execute logical plan")?;

                    let schema: arrow::datatypes::SchemaRef =
                        Arc::new(df.schema().as_arrow().clone());

                    let batches = df.collect().await?;

                    let mut local_warnings = collector.take_all();
                    if let Ok(w) = strake_common::warnings::QUERY_WARNINGS.try_with(|x| x.clone()) {
                        if let Ok(mut lock) = w.lock() {
                            local_warnings.append(&mut *lock);
                        }
                    }

                    Ok::<
                        (
                            arrow::datatypes::SchemaRef,
                            Vec<arrow::record_batch::RecordBatch>,
                            Vec<String>,
                        ),
                        anyhow::Error,
                    >((schema, batches, local_warnings))
                })
                .await
            })
            .await;

        let (schema, batches, mut warnings) = match result {
            Ok(Ok((s, b, w))) => (s, b, w),
            Ok(Err(e)) => {
                tracing::error!("Detailed execution error: {:#}", e);
                return Err(anyhow::anyhow!(e));
            }
            Err(_) => {
                return Err(strake_error::StrakeError::new(
                    strake_error::ErrorCode::QueryCancelled,
                    format!("Query timed out after {} seconds", timeout_seconds),
                )
                .with_hint("Simplify query or increase 'query_timeout_seconds' in config")
                .into());
            }
        };

        // --- Cache Store START ---
        if should_cache {
            let _ = self.cache.put(cache_key, &batches).await;
            warnings.push("x-strake-cache: miss".to_string());
        }
        // --- Cache Store END ---

        let duration = start.elapsed();
        let user_id = user.as_ref().map(|u| u.id.as_str()).unwrap_or("anonymous");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(
            target: "queries",
            user_id = %user_id,
            query = sql,
            duration_ms = duration.as_millis() as u64,
            rows_returned = rows,
            cache_hit = false,
            success = true
        );

        Ok((schema, batches, warnings))
    }

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

        // Create physical plan
        let physical_plan = self
            .context
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .context("Failed to create physical plan")?;

        // Format as tree
        Ok(crate::query::plan_tree::format_plan_tree(&physical_plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strake_common::auth::AuthenticatedUser;
    use strake_common::config::QueryLimits;

    #[tokio::test]
    async fn test_engine_init() -> Result<()> {
        let config = Config {
            sources: vec![],
            cache: Default::default(),
        };
        let limits = QueryLimits::default();
        let engine = FederationEngine::new(FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: limits,
            resource_config: strake_common::config::ResourceConfig::default(),
            datafusion_config: std::collections::HashMap::new(),
            global_budget: 10,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: Default::default(),
        })
        .await?;

        assert_eq!(engine.catalog_name, "strake");
        assert_eq!(engine.active_queries(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_engine_execute_simple() -> Result<()> {
        let config = Config {
            sources: vec![],
            cache: Default::default(),
        };
        let engine = FederationEngine::new(FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: QueryLimits::default(),
            resource_config: strake_common::config::ResourceConfig::default(),
            datafusion_config: std::collections::HashMap::new(),
            global_budget: 10,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: Default::default(),
        })
        .await?;

        let sql = "SELECT 1 as val";
        let (schema, batches, _warnings) = engine.execute_query(sql, None).await?;

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "val");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_user_propagation() -> Result<()> {
        let config = Config {
            sources: vec![],
            cache: Default::default(),
        };
        let engine = FederationEngine::new(FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: QueryLimits::default(),
            resource_config: strake_common::config::ResourceConfig::default(),
            datafusion_config: std::collections::HashMap::new(),
            global_budget: 10,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: Default::default(),
        })
        .await?;

        let user = AuthenticatedUser {
            id: "test_user".to_string(),
            permissions: vec!["admin".to_string()].into(),
            rules: std::collections::HashMap::new(),
        };

        // This just verifies it doesn't crash when user is present
        let sql = "SELECT 1";
        let (_schema, _batches, _warnings) = engine.execute_query(sql, Some(user)).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_engine_trace() -> Result<()> {
        let config = Config {
            sources: vec![],
            cache: Default::default(),
        };
        let engine = FederationEngine::new(FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: QueryLimits::default(),
            resource_config: strake_common::config::ResourceConfig::default(),
            datafusion_config: std::collections::HashMap::new(),
            global_budget: 10,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: Default::default(),
        })
        .await?;

        let sql = "SELECT 1";
        let trace = engine.execute_query_with_trace(sql).await?;
        assert!(trace.contains("STRAKE QUERY REPORT"));
        Ok(())
    }
}
