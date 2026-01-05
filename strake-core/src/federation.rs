use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::catalog::CatalogProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_federation::FederationOptimizerRule;
use tracing::info;

use crate::config::{Config, SourceConfig, ResourceConfig};
use crate::sources::{self, SourceRegistry, SourceProvider};
use crate::query::cost_validator::CostBasedValidator;
use crate::optimizer::defensive_trace::DefensiveLimitRule;
use crate::query::cache::QueryCache;
use crate::query::cache::CacheConfig as InternalCacheConfig;
use std::path::PathBuf;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::memory_pool::{GreedyMemoryPool, FairSpillPool};
use std::collections::HashMap;

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
    global_cache_config: crate::config::QueryCacheConfig,
}

impl FederationEngine {
    pub fn context(&self) -> &SessionContext {
        &self.context
    }

    pub fn active_queries(&self) -> usize {
        self.active_queries.load(Ordering::Relaxed)
    }

    pub async fn new(
        config: Config, 
        catalog_name: String, 
        query_limits: crate::config::QueryLimits,
        resource_config: ResourceConfig,
        datafusion_config: HashMap<String, String>,
        global_budget: usize,
        extra_optimizer_rules: Vec<Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>>,
        extra_sources: Vec<Box<dyn SourceProvider>>,
    ) -> Result<Self> {
        let context = Self::build_session_context(
            &query_limits, 
            &catalog_name, 
            resource_config,
            datafusion_config,
            extra_optimizer_rules
        )?;
        
        // Register our custom catalog
        let catalog = Arc::new(datafusion::catalog::MemoryCatalogProvider::new());
        catalog.register_schema("public", Arc::new(datafusion::catalog::MemorySchemaProvider::new()))?;
        context.register_catalog(&catalog_name, catalog);

        let mut registry = sources::default_registry(crate::config::RetrySettings::default());
        for provider in extra_sources {
            registry.register_provider(provider);
        }
        
        Self::register_sources(&context, &catalog_name, &config.sources, &registry).await?;
        
        let cache_config = InternalCacheConfig {
            enabled: config.cache.enabled,
            directory: PathBuf::from(&config.cache.directory),
            max_size_mb: config.cache.max_size_mb,
            ttl_seconds: config.cache.ttl_seconds,
        };
        let cache = QueryCache::new(cache_config).await?;

        Ok(Self {
            context,
            active_queries: Arc::new(AtomicUsize::new(0)),
            _registry: registry,
            catalog_name,
            connection_budget: Arc::new(Semaphore::new(global_budget)),
            cache,
            source_configs: config.sources.iter()
                .map(|s| (s.name.clone(), s.clone()))
                .collect(),
            global_cache_config: config.cache.clone(),
        })
    }

    pub fn get_source_config(&self, name: &str) -> Option<SourceConfig> {
        self.source_configs.get(name).cloned()
    }

    fn build_session_context(
        limits: &crate::config::QueryLimits, 
        catalog_name: &str,
        resource_config: ResourceConfig,
        datafusion_config: HashMap<String, String>,
        extra_optimizer_rules: Vec<Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>>,
    ) -> Result<SessionContext> {
        let mut session_config = datafusion::prelude::SessionConfig::new()
            .with_default_catalog_and_schema(catalog_name, "public")
            .with_information_schema(true); // Always strictly correct

        // Apply Safety Defaults (Pushdown, Pruning)
        session_config.options_mut().execution.parquet.pushdown_filters = true;
        session_config.options_mut().execution.parquet.pruning = true;
        
        // Single-node concurrency tuning (default to 4 if not set)
        if session_config.options().execution.target_partitions == 0 {
             session_config.options_mut().execution.target_partitions = 4;
        }

        // Apply Generic Config Passthrough
        for (key, value) in datafusion_config {
             session_config.options_mut().set(&key, &value).context(format!("Failed to set config option: {}", key))?;
        }

        // Configure RuntimeEnv (Memory & Disk)
        let mut rt_builder = RuntimeEnvBuilder::new();

        // 1. Memory Limit
        if let Some(limit_mb) = resource_config.memory_limit_mb {
            let limit_bytes = limit_mb * 1024 * 1024;
            // Use FairSpillPool to allow spilling when limit is reached
            rt_builder = rt_builder.with_memory_pool(Arc::new(FairSpillPool::new(limit_bytes)));
        } else {
            // Default: Greedy pool (no limit/spill unless OS OOMs)
             rt_builder = rt_builder.with_memory_pool(Arc::new(GreedyMemoryPool::new(usize::MAX)));
        }
        // 2. Disk Spill/Temp Dir
        if let Some(spill_path) = resource_config.spill_dir {
             use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
             let mode = DiskManagerMode::Directories(vec![spill_path.into()]);
             rt_builder = rt_builder.with_disk_manager_builder(DiskManagerBuilder::default().with_mode(mode));
        } else {
             rt_builder = rt_builder.with_disk_manager_builder(datafusion::execution::disk_manager::DiskManagerBuilder::default());
        }

        let runtime_env = rt_builder.build().context("Failed to build RuntimeEnv")?;

        let context = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env));
        let state = context.state();
        
        let mut optimizer_rules = state.optimizer().rules.clone();
        
        for rule in extra_optimizer_rules {
            optimizer_rules.push(rule);
        }
        optimizer_rules.push(Arc::new(FederationOptimizerRule::new()));

        // Add Defensive Limit Rule if configured
        if let Some(limit) = limits.default_limit {
             optimizer_rules.push(Arc::new(DefensiveLimitRule::new(limit)));
        }
        
        // Register CostBasedValidator as the final physical optimizer rule
        // This runs after all optimizations, checking the final plan stats
        let cost_validator = Arc::new(CostBasedValidator::new(
            limits.max_output_rows,
            limits.max_scan_bytes,
        ));
        
        // We need to add it to physical optimizers, not logical (which is what optimizer_rules is for).
        // Initializing session state with physical optimizers requires a slightly different builder pattern 
        // or we can just append it if we have access.
        // DataFusion SessionState builder `with_physical_optimizer_rules` replaces default rules if we are not careful.
        // But we want to APPEND.
        
        // Strategy: Build state first, getting default rules, then append ours.
        let state = SessionStateBuilder::new_from_existing(state.clone())
            .with_optimizer_rules(optimizer_rules)
            .build();
            
        let mut physical_optimizers = state.physical_optimizers().to_vec();
        physical_optimizers.push(cost_validator);
        
        let state = SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(physical_optimizers)
            .build();
            
        Ok(SessionContext::new_with_state(state))
    }

    async fn register_sources(
        context: &SessionContext, 
        catalog: &str, 
        sources: &[SourceConfig],
        registry: &SourceRegistry,
    ) -> Result<()> {
        let futures = sources.into_iter().map(|source| {
            registry.register_source(context, catalog, source)
        });
        
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
        user: Option<crate::auth::AuthenticatedUser>,
    ) -> Result<(
        arrow::datatypes::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
        Vec<String>,
    )> {
        self.active_queries.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        
        let mut state = self.context.state();
        
        if let Some(u) = user.clone() {
            let mut config = state.config().clone();
            config.options_mut().extensions.insert(u);
            state = SessionStateBuilder::new_from_existing(state)
                .with_config(config)
                .build();
        }

        // Create a temporary context for plan creation and execution
        let context = SessionContext::new_with_state(state);

        let _permit = self.connection_budget.clone().acquire_owned().await
            .context("Failed to acquire connection permit")?;

        let plan = context.state().create_logical_plan(sql).await
            .context("Failed to create logical plan")?;
        
        // --- Cache Lookup START ---
        // Resolve effective cache configuration
        let should_cache = self.should_cache_query(&plan);
        let cache_key = crate::query::cache::CacheKey::from_plan(&plan, user.as_ref())?;
        
        let mut cached_batches_opt = None;
        if should_cache {
            cached_batches_opt = self.cache.get(&cache_key).await;
        }

        if let Some(cached_batches) = cached_batches_opt {
            self.active_queries.fetch_sub(1, Ordering::Relaxed);
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

        let df = context
            .execute_logical_plan(plan.clone())
            .await
            .context("Failed to execute logical plan")?;

        // Check for injected limits (Defensive Limits)
        let mut warnings = vec![];
        let original_has_limit = matches!(plan, datafusion::logical_expr::LogicalPlan::Limit(_));
        if !original_has_limit {
             if let datafusion::logical_expr::LogicalPlan::Limit(l) = df.logical_plan() {
                 // If we now have a limit, and didn't before, precise check:
                 // The DefensiveLimitRule uses the configured default_limit.
                 let mut injected_limit = 0;
                 if let Some(fetch_expr) = &l.fetch {
                     if let datafusion::logical_expr::Expr::Literal(datafusion::common::ScalarValue::Int64(Some(val)), _) = fetch_expr.as_ref() {
                         injected_limit = *val as usize;
                     }
                 }
                 
                 if injected_limit > 0 {
                     warnings.push(format!("x-strake-warning: defensive-limit-applied={}", injected_limit));
                 }
             }
        }

        let schema = df.schema().as_arrow().as_ref().clone();
        let batches = df.collect().await?;
        
        // --- Cache Store START ---
        if should_cache {
            // Store in cache (defensive: errors logged internally)
            let _ = self.cache.put(cache_key, &batches).await;
            warnings.push("x-strake-cache: miss".to_string());
        }
        // --- Cache Store END ---

        self.active_queries.fetch_sub(1, Ordering::Relaxed);
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

        Ok((Arc::new(schema), batches, warnings))
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
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QueryLimits;
    use crate::auth::AuthenticatedUser;

    #[tokio::test]
    async fn test_engine_init() -> Result<()> {
        let config = Config { sources: vec![], cache: Default::default() };
        let limits = QueryLimits::default();
        let engine = FederationEngine::new(
            config,
            "strake".to_string(),
            limits,
            crate::config::ResourceConfig::default(),
            std::collections::HashMap::new(),
            10,
            vec![],
            vec![],
        ).await?;
        
        assert_eq!(engine.catalog_name, "strake");
        assert_eq!(engine.active_queries(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_engine_execute_simple() -> Result<()> {
        let config = Config { sources: vec![], cache: Default::default() };
        let engine = FederationEngine::new(
            config,
            "strake".to_string(),
            QueryLimits::default(),
            crate::config::ResourceConfig::default(),
            std::collections::HashMap::new(),
            10,
            vec![],
            vec![],
        ).await?;
        
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
        let config = Config { sources: vec![], cache: Default::default() };
        let engine = FederationEngine::new(
            config,
            "strake".to_string(),
            QueryLimits::default(),
            crate::config::ResourceConfig::default(),
            std::collections::HashMap::new(),
            10,
            vec![],
            vec![],
        ).await?;
        
        let user = AuthenticatedUser {
            id: "test_user".to_string(),
            permissions: vec!["admin".to_string()],
            rules: std::collections::HashMap::new(),
        };
        
        // This just verifies it doesn't crash when user is present
        let sql = "SELECT 1";
        let (_schema, _batches, _warnings) = engine.execute_query(sql, Some(user)).await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_engine_trace() -> Result<()> {
        let config = Config { sources: vec![], cache: Default::default() };
        let engine = FederationEngine::new(
            config,
            "strake".to_string(),
            QueryLimits::default(),
            crate::config::ResourceConfig::default(),
            std::collections::HashMap::new(),
            10,
            vec![],
            vec![],
        ).await?;
        
        let sql = "SELECT 1";
        let trace = engine.execute_query_with_trace(sql).await?;
        assert!(trace.contains("STRAKE QUERY REPORT"));
        Ok(())
    }
}
