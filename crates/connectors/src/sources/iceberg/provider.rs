//! Iceberg Table Provider Implementation
//!
//! This module handles registration and execution of Iceberg tables via the REST catalog.
//! It supports lazy loading of table metadata and schema initialization at registration time.

use anyhow::Result;
use async_trait::async_trait;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;

use std::sync::{Arc, OnceLock};

use super::auth::{
    AwsIrsaAuth, CompositeAuth, IcebergAuthProvider, OAuthIcebergAuth, S3Credentials,
    StaticTokenAuth,
};
use super::catalog::{CachedRestCatalog, create_rest_catalog};
use super::{IcebergRestConfig, TableVersionSpec};
use strake_common::config::TableConfig;

use super::error::IcebergConnectorError;
use super::telemetry::IcebergTelemetry;
use crate::sources::iceberg::federation::IcebergExecutor;
use crate::sources::sql::wrappers::{wrap_concurrent, wrap_provider};
use datafusion_federation::FederatedTableProviderAdaptor;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use std::any::Any;
use std::time::Instant;
use strake_common::circuit_breaker::{AdaptiveCircuitBreaker, CircuitBreakerConfig};
use strake_common::config::RetrySettings;
use strake_common::retry::retry_async;
use tokio::sync::RwLock;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

/// State machine for lazy table loading:
///
/// ```text
///          ┌─────[channel_closed]─────┐
///          ▼                          │
/// Unloaded ──[get_or_load]──► Loading ──[success]──► Loaded
///          ▲                  │    │
///          │                  │    └──[channel_closed]──► Unloaded (retry)
///          └──────────────────┘
///                             └──────[failure]──────► Failed
/// ```
///
/// Transitions:
/// - Unloaded → Loading: First call to get_or_load()
/// - Loading → Loaded: perform_load() succeeds
/// - Loading → Failed: perform_load() fails and the error is permanent
/// - Loading → Unloaded: The loading task's watch channel was dropped before
///   completion (e.g. the task panicked). A subsequent caller resets to Unloaded
///   and may retry the load.
/// - Failed → *: Never (failures are permanent; no automatic retry)
/// - Loaded → *: Never (terminal state)
#[derive(Debug)]
enum LoadState {
    Unloaded,
    Loading(tokio::sync::watch::Receiver<LoadResult>),
    Loaded(Arc<dyn TableProvider>),
    Failed(Arc<anyhow::Error>),
}

/// Result of table loading operation
#[derive(Clone, Debug)]
enum LoadResult {
    Pending,
    Success,
    Failed(()),
}

use crate::sources::predicate_caching::{
    CacheMode, FileRecordingState, PredicateCachingContext, RecordingExec, inject_factory_into_plan,
};
use moka::future::Cache as MokaCache;
use parquet::file::metadata::ParquetMetaData;
use strake_common::predicate_cache::PredicateCache;

/// Configuration and context for registering an Iceberg REST catalog source.
#[derive(Clone)]
pub struct IcebergRegistration {
    /// DataFusion session context.
    pub ctx: Arc<SessionContext>,
    /// Target catalog name.
    pub catalog_name: String,
    /// Name of the source.
    pub source_name: String,
    /// Parsed Iceberg REST config.
    pub cfg: Arc<IcebergRestConfig>,
    /// Tables configuration to register.
    pub tables: Arc<Vec<TableConfig>>,
    /// Global retry settings.
    pub retry_settings: RetrySettings,
    /// Shared predicate cache.
    pub predicate_cache: Arc<PredicateCache>,
    /// Whether predicate cache is enabled.
    pub predicate_cache_enabled: bool,
}

/// Register Iceberg tables with DataFusion context
pub async fn register_iceberg_rest(reg: IcebergRegistration) -> Result<()> {
    let cb = Arc::new(AdaptiveCircuitBreaker::new(CircuitBreakerConfig::default()));
    let retry_settings = reg.retry_settings;
    let source_name = reg.source_name.clone();

    retry_async(
        format!("iceberg_register({})", source_name),
        retry_settings,
        move || {
            let cb = cb.clone();
            let reg = reg.clone();
            async move { try_register_iceberg_rest(reg, cb).await }
        },
    )
    .await
}

async fn try_register_iceberg_rest(
    reg: IcebergRegistration,
    cb: Arc<AdaptiveCircuitBreaker>,
) -> Result<()> {
    let cfg = &reg.cfg;
    let ctx = &reg.ctx;
    let tables = &reg.tables;
    let catalog_name = reg.catalog_name.as_str();
    let source_name = reg.source_name.as_str();
    let predicate_cache = &reg.predicate_cache;
    let predicate_cache_enabled = reg.predicate_cache_enabled;

    // 1. Setup Auth
    let rest_auth: Option<Box<dyn IcebergAuthProvider>> = if let Some(token) = &cfg.token {
        Some(Box::new(StaticTokenAuth::new(token.clone())))
    } else if let (Some(client_id), Some(client_secret), Some(token_url)) = (
        &cfg.oauth_client_id,
        &cfg.oauth_client_secret,
        &cfg.oauth_token_url,
    ) {
        let scopes = cfg.oauth_scopes.clone().unwrap_or_default();
        Some(Box::new(OAuthIcebergAuth::new(
            client_id.clone(),
            client_secret.clone(),
            token_url.clone(),
            scopes,
        )))
    } else {
        None
    };

    let s3_auth: Box<dyn IcebergAuthProvider> =
        if let (Some(access_key), Some(secret_key)) = (&cfg.s3_access_key, &cfg.s3_secret_key) {
            Box::new(AwsIrsaAuth::with_static_credentials(S3Credentials {
                access_key_id: access_key.clone(),
                secret_access_key: secret_key.clone(),
                session_token: cfg.s3_session_token.clone(),
            }))
        } else {
            Box::new(AwsIrsaAuth::new())
        };
    let auth: Arc<dyn IcebergAuthProvider> = Arc::new(CompositeAuth::new(rest_auth, s3_auth));

    // 2. Create catalog with caching
    let rest_catalog = create_rest_catalog(source_name, cfg, &auth).await?;
    let cache_config = cfg.cache.clone().unwrap_or_default();
    let iceberg_catalog = Arc::new(CachedRestCatalog::new(rest_catalog, cache_config));

    // 2.5 Setup Federation Provider
    let federation_executor = IcebergExecutor::new(iceberg_catalog.clone(), cfg.warehouse.clone());
    let federation_provider = federation_executor.create_federation_provider();

    // 3. Ensure target catalog exists in DataFusion
    let catalog = ctx
        .catalog(catalog_name)
        .ok_or_else(|| anyhow::anyhow!("Catalog '{}' not found", catalog_name))?;

    let schema_name = cfg.namespace.as_deref().unwrap_or(source_name);

    if catalog.schema(schema_name).is_none() {
        tracing::debug!(
            "Schema '{}' not found in catalog '{}', creating memory provider",
            schema_name,
            catalog_name
        );
        catalog.register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))?;
    }

    // Register dedicated catalog using source_name if different from catalog_name
    if catalog_name != source_name {
        if ctx.catalog(source_name).is_none() {
            tracing::info!(
                "Catalog '{}' not found, registering new MemoryCatalogProvider for Iceberg source",
                source_name
            );
            ctx.register_catalog(
                source_name,
                Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
            );
        }
        let source_catalog = ctx.catalog(source_name).unwrap();
        if source_catalog.schema(schema_name).is_none() {
            source_catalog.register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))?;
        }
    }

    let max_concurrency = cfg.max_concurrent_queries.unwrap_or(0);

    // 4. Pre-fetch schema at registration time for planner correctness;
    // actual scan I/O is deferred to get_or_load().
    for table_cfg in tables.iter() {
        let namespace = match &cfg.namespace {
            Some(ns) if !ns.is_empty() => {
                let parts: Vec<String> = ns.split('.').map(|s| s.to_string()).collect();
                NamespaceIdent::from_vec(parts).map_err(|e| {
                    IcebergConnectorError::InvalidConfiguration(format!(
                        "Failed to parse namespace '{}': {}",
                        ns, e
                    ))
                })?
            }
            _ => NamespaceIdent::new("default".to_string()),
        };

        let ident = TableIdent::new(namespace, table_cfg.name.clone());

        // Load table to get schema for pre-seeding (Must Fix: Safety)
        // This makes schema() non-blocking and safe for all runtimes
        let table = iceberg_catalog
            .as_ref()
            .load_table(&ident)
            .await
            .map_err(|e| IcebergConnectorError::CatalogError {
                operation: "load_table".into(),
                retries: 0,
                source: e.into(),
            })?;

        // We use a temporary provider to convert the schema correctly
        // This is cheap as it only processes metadata, no scan
        let dummy_provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(|e| IcebergConnectorError::CatalogError {
                operation: "create_provider".into(),
                retries: 0,
                source: anyhow::anyhow!(e),
            })?;
        let schema = TableProvider::schema(&dummy_provider);

        let lazy_provider = Arc::new(LazyIcebergTableProvider::new(
            iceberg_catalog.clone(),
            ident.clone(),
            cfg.version.clone(),
            schema.clone(),
            predicate_cache.clone(),
            predicate_cache_enabled,
        ));

        // Wrap with federation adaptor to enable pushdown/join splitting
        // Use SQLTableSource for federation logic
        let sql_source = datafusion_federation::sql::SQLTableSource::new_with_schema(
            federation_provider.clone(),
            TableReference::full(catalog_name, schema_name, table_cfg.name.as_str()).into(),
            schema.clone(),
        );

        // Wrap with federation adaptor
        let federated_provider = Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            Arc::new(sql_source),
            lazy_provider.clone(),
        ));

        let enriched_provider = wrap_provider(federated_provider, cb.clone(), false);
        let limited_provider = wrap_concurrent(enriched_provider, max_concurrency);
        let qualified = TableReference::full(catalog_name, schema_name, table_cfg.name.as_str());
        ctx.register_table(qualified, limited_provider)?;

        // Note: we track registration metric immediately as we don't load anymore
        IcebergTelemetry::table_registered(catalog_name, schema_name, &table_cfg.name);

        if catalog_name != source_name {
            let source_sql_source = datafusion_federation::sql::SQLTableSource::new_with_schema(
                federation_provider.clone(),
                TableReference::full(source_name, schema_name, table_cfg.name.as_str()).into(),
                schema,
            );
            let source_federated_provider =
                Arc::new(FederatedTableProviderAdaptor::new_with_provider(
                    Arc::new(source_sql_source),
                    lazy_provider,
                ));
            let source_enriched_provider =
                wrap_provider(source_federated_provider, cb.clone(), false);
            let source_limited_provider =
                wrap_concurrent(source_enriched_provider, max_concurrency);
            let source_qualified =
                TableReference::full(source_name, schema_name, table_cfg.name.as_str());
            ctx.register_table(source_qualified, source_limited_provider)?;
            IcebergTelemetry::table_registered(source_name, schema_name, &table_cfg.name);
        }
    }

    Ok(())
}

/// Lazy wrapper that loads the Iceberg table only when scanned or when schema is requested
#[derive(Debug)]
/// A lazy-loading table provider for Iceberg.
///
/// This provider defers the actual scan execution until explicitly requested,
/// but pre-fetches the schema at registration time to ensure planner correctness.
pub struct LazyIcebergTableProvider {
    catalog: Arc<CachedRestCatalog>,
    ident: TableIdent,
    version: Option<TableVersionSpec>,
    predicate_cache: Arc<PredicateCache>,
    predicate_cache_enabled: bool,
    state: RwLock<LoadState>,
    schema_cache: OnceLock<SchemaRef>,
    snapshot_id: OnceLock<i64>,
    metadata_cache: Arc<MokaCache<String, Arc<ParquetMetaData>>>,
}

impl LazyIcebergTableProvider {
    /// Creates a new `LazyIcebergTableProvider`.
    pub fn new(
        catalog: Arc<CachedRestCatalog>,
        ident: TableIdent,
        version: Option<TableVersionSpec>,
        known_schema: SchemaRef,
        predicate_cache: Arc<PredicateCache>,
        predicate_cache_enabled: bool,
    ) -> Self {
        let schema_cache = OnceLock::new();
        let _ = schema_cache.set(known_schema);

        let metadata_cache = Arc::new(
            MokaCache::builder()
                .max_capacity(1000)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
        );

        Self {
            catalog,
            ident,
            version,
            predicate_cache,
            predicate_cache_enabled,
            state: RwLock::new(LoadState::Unloaded),
            schema_cache,
            snapshot_id: OnceLock::new(),
            metadata_cache,
        }
    }

    async fn get_or_load(&self) -> DFResult<Arc<dyn TableProvider>> {
        // Fast path: check if already loaded
        {
            let state = self.state.read().await;
            match &*state {
                LoadState::Loaded(provider) => return Ok(provider.clone()),
                LoadState::Failed(e) => {
                    return Err(datafusion::error::DataFusionError::Execution(e.to_string()));
                }
                _ => {} // Fall through to slow path
            }
        }

        // Slow path: acquire write lock to transition state
        let mut state = self.state.write().await;

        // Loop to handle retries and spurious wakeups
        loop {
            // Re-check after acquiring write lock
            match &*state {
                LoadState::Loaded(provider) => return Ok(provider.clone()),
                LoadState::Failed(e) => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "Table load previously failed: {}",
                        e
                    )));
                }
                LoadState::Loading(receiver) => {
                    // Another task is loading, wait for it
                    let mut rx = receiver.clone();
                    drop(state); // Release lock while waiting

                    match rx.changed().await {
                        Ok(_) => {
                            // Reload state
                            state = self.state.write().await;
                            continue; // Re-evaluate state
                        }
                        Err(_) => {
                            // Channel closed without result (dropped?)
                            state = self.state.write().await;
                            // If channel closed and still in Loading state, someone dropped the ball.
                            // Reset to Unloaded to allow a retry.
                            if matches!(&*state, LoadState::Loading(_)) {
                                tracing::warn!(
                                    "Loading task dropped its channel, resetting to Unloaded"
                                );
                                *state = LoadState::Unloaded;
                            }
                            continue;
                        }
                    }
                }
                LoadState::Unloaded => {
                    // We are responsible for loading
                    let (tx, rx) = tokio::sync::watch::channel(LoadResult::Pending);
                    *state = LoadState::Loading(rx);
                    drop(state); // Release lock during I/O

                    // Perform actual load
                    let result = self.perform_load().await;

                    // Update state
                    let mut state = self.state.write().await;
                    match result {
                        Ok(provider) => {
                            // Extract and cache snapshot ID if possible
                            // For now we rely on perform_load having set it if it was time-traveling,
                            // but for regular loads we should get it from the table.
                            *state = LoadState::Loaded(provider.clone());
                            let _ = tx.send(LoadResult::Success);
                            return Ok(provider);
                        }
                        Err(e) => {
                            let err_arc = Arc::new(e);
                            *state = LoadState::Failed(err_arc.clone());
                            let _ = tx.send(LoadResult::Failed(()));
                            return Err(datafusion::error::DataFusionError::Execution(
                                err_arc.to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }

    /// Perform the actual table load
    async fn perform_load(&self) -> Result<Arc<dyn TableProvider>> {
        let inner_catalog = self.catalog.clone();

        // Handle time travel if specified
        if let Some(version) = &self.version {
            // Load base table to validate it exists and get metadata for time travel
            let table = inner_catalog
                .as_ref()
                .load_table(&self.ident)
                .await
                .map_err(|e: iceberg::Error| IcebergConnectorError::CatalogError {
                    operation: "load_table".into(),
                    retries: 0,
                    source: e.into(),
                })?;

            let metadata = table.metadata();

            let snapshot_id = match version {
                TableVersionSpec::SnapshotId(id) => metadata
                    .snapshots()
                    .find(|s: &&std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id() == *id)
                    .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                    .ok_or_else(|| IcebergConnectorError::TimeTravelUnavailable {
                        version: version.clone(),
                        available_snapshots: metadata
                            .snapshots()
                            .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                            .collect(),
                    })?,
                TableVersionSpec::Timestamp(ts_millis) => {
                    // Find snapshot closest to timestamp
                    let snapshot = metadata
                        .snapshots()
                        .filter(|s: &&std::sync::Arc<iceberg::spec::Snapshot>| {
                            s.timestamp_ms() <= *ts_millis
                        })
                        .max_by_key(|s: &&std::sync::Arc<iceberg::spec::Snapshot>| s.timestamp_ms())
                        .ok_or_else(|| IcebergConnectorError::TimeTravelUnavailable {
                            version: version.clone(),
                            available_snapshots: metadata
                                .snapshots()
                                .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                                .collect(),
                        })?;
                    snapshot.snapshot_id()
                }
                TableVersionSpec::Tag(tag) => metadata
                    .snapshot_for_ref(tag)
                    .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                    .ok_or_else(|| IcebergConnectorError::TimeTravelUnavailable {
                        version: version.clone(),
                        available_snapshots: metadata
                            .snapshots()
                            .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                            .collect(),
                    })?,
                TableVersionSpec::Branch(branch) => metadata
                    .snapshot_for_ref(branch)
                    .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                    .ok_or_else(|| IcebergConnectorError::TimeTravelUnavailable {
                        version: version.clone(),
                        available_snapshots: metadata
                            .snapshots()
                            .map(|s: &std::sync::Arc<iceberg::spec::Snapshot>| s.snapshot_id())
                            .collect(),
                    })?,
            };

            tracing::info!(
                table = %self.ident,
                version = ?version,
                snapshot_id = snapshot_id,
                "Time travel: creating static table provider"
            );

            let static_provider =
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                    .await
                    .map_err(|e| IcebergConnectorError::CatalogError {
                        operation: "create_provider".into(),
                        retries: 0,
                        source: anyhow::anyhow!(e),
                    })?;
            let _ = self.snapshot_id.set(snapshot_id);
            return Ok(Arc::new(static_provider));
        }

        //Efficiently load only the required table instead of creating a full CatalogProvider
        // which would enumerate all namespaces and tables in the catalog (O(N) vs O(1)).
        let table = inner_catalog
            .as_ref()
            .load_table(&self.ident)
            .await
            .map_err(|e: iceberg::Error| IcebergConnectorError::CatalogError {
                operation: "load_table".into(),
                retries: 0,
                source: e.into(),
            })?;

        if let Some(snapshot) = table.metadata().current_snapshot() {
            let snapshot_id = snapshot.snapshot_id();
            let static_provider =
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                    .await
                    .map_err(|e| IcebergConnectorError::CatalogError {
                        operation: "create_provider".into(),
                        retries: 0,
                        source: anyhow::anyhow!(e),
                    })?;
            let _ = self.snapshot_id.set(snapshot_id);
            Ok(Arc::new(static_provider))
        } else {
            let schema = self
                .schema_cache
                .get()
                .cloned()
                .expect("schema_cache is always initialized in LazyIcebergTableProvider::new");
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]])?;
            Ok(Arc::new(mem_table))
        }
    }
}

#[async_trait]
// FIXME: DynamicFilterSource (#812) not implemented for lazy provider.
// This prevents runtime join filter pushdown until the table is loaded.
impl TableProvider for LazyIcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        // Guaranteed to be present by constructor
        self.schema_cache
            .get()
            .cloned()
            .expect("schema_cache is always initialized in LazyIcebergTableProvider::new")
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        // Fast path: if loaded, use provider stats
        if let Ok(guard) = self.state.try_read() {
            match &*guard {
                LoadState::Loaded(provider) => return provider.statistics(),
                LoadState::Failed(_) => {
                    // Don't retry - return unknown for failed loads
                    return Some(datafusion::common::Statistics::new_unknown(
                        &self.schema_cache.get().cloned().unwrap_or_else(|| {
                            Arc::new(datafusion::arrow::datatypes::Schema::empty())
                        }),
                    ));
                }
                _ => {} // Fall through to load
            }
        }

        // Fall through to unknown stats with known schema

        // If not loaded, return unknown stats with known schema
        if let Some(schema) = self.schema_cache.get() {
            return Some(datafusion::common::Statistics::new_unknown(schema));
        }

        None
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let start = Instant::now();
        IcebergTelemetry::scan_started(
            self.ident.name(),
            filters.len(),
            projection.map(|p| p.len()),
        );

        let provider = self.get_or_load().await?;
        let _ = self.schema_cache.get_or_init(|| provider.schema());
        let result = provider
            .scan(state, projection, filters, limit)
            .await
            .and_then(|plan| {
                if filters.is_empty() || !self.predicate_cache_enabled {
                    return Ok(plan);
                }

                let snapshot_id = match self.snapshot_id.get().copied() {
                    Some(id) => id,
                    None => return Ok(plan),
                };

                let combined_predicate = filters[1..]
                    .iter()
                    .cloned()
                    .fold(filters[0].clone(), |acc, expr| acc.and(expr));

                let df_schema = datafusion::common::DFSchema::try_from_qualified_schema(
                    datafusion::sql::TableReference::bare(Arc::from(self.ident.name())),
                    &provider.schema(),
                )?;
                let physical_predicate =
                    state.create_physical_expr(combined_predicate, &df_schema)?;

                let cache_mode = if self
                    .predicate_cache
                    .has_any_blocks_for_snapshot(snapshot_id)
                {
                    CacheMode::Filtering
                } else {
                    CacheMode::Recording
                };

                let recording_states =
                    Arc::new(dashmap::DashMap::<String, Arc<FileRecordingState>>::new());
                let partition_row_offsets = Arc::new(dashmap::DashMap::new());

                let context = PredicateCachingContext {
                    cache: self.predicate_cache.clone(),
                    snapshot_id,
                    recording_states: recording_states.clone(),
                    partition_row_offsets: partition_row_offsets.clone(),
                    mode: cache_mode,
                    metadata_cache: self.metadata_cache.clone(),
                };

                let instrumented_plan =
                    inject_factory_into_plan(plan, state.runtime_env().clone(), context)?;

                if cache_mode == CacheMode::Recording {
                    Ok(Arc::new(RecordingExec::new(
                        instrumented_plan,
                        physical_predicate,
                        recording_states,
                        partition_row_offsets,
                        self.predicate_cache.clone(),
                        snapshot_id,
                    )) as Arc<dyn ExecutionPlan>)
                } else {
                    Ok(instrumented_plan)
                }
            });

        let elapsed = start.elapsed();
        IcebergTelemetry::scan_planning_completed(self.ident.name(), elapsed.as_millis() as u64);

        result
    }

    /// Determines filter pushdown capability.
    ///
    /// # Note
    /// This method may trigger table loading if the provider has not been
    /// previously accessed. The first call may block while fetching metadata.
    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        // FIXME(DataFusion51): DynamicFilterSource deferred — see issue #812
        // Ensure loaded to delegate pushdown logic (needs schema/partition spec)
        let _ = self.schema();

        if let Ok(guard) = self.state.try_read()
            && let LoadState::Loaded(provider) = &*guard
        {
            return provider.supports_filters_pushdown(filters);
        }

        Ok(vec![datafusion::logical_expr::TableProviderFilterPushDown::Unsupported; filters.len()])
    }
}
