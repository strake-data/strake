use anyhow::Result;
use async_trait::async_trait;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;

use std::sync::{Arc, OnceLock};

use super::auth::{
    AwsIrsaAuth, CompositeAuth, IcebergAuthProvider, OAuthIcebergAuth, StaticTokenAuth,
};
use super::catalog::{create_rest_catalog, CachedRestCatalog};
use super::{IcebergRestConfig, TableVersionSpec};
use strake_common::config::TableConfig;

use super::error::IcebergConnectorError;
use super::telemetry::IcebergTelemetry;
use crate::sources::iceberg::federation::IcebergExecutor;
use crate::sources::sql::common::FetchedMetadata;
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
/// Unloaded ──[get_or_load]──► Loading ──[success]──► Loaded
///                     │                      │
///                     │                      │
///                     └──────[failure]───────┘
///                            │
///                            ▼
///                         Failed
/// ```
///
/// Transitions:
/// - Unloaded → Loading: First call to get_or_load()
/// - Loading → Loaded: perform_load() succeeds
/// - Loading → Failed: perform_load() fails
/// - Failed → Loading: Never (failures are permanent)
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

/// Register Iceberg tables with DataFusion context
pub async fn register_iceberg_rest(
    ctx: &SessionContext,
    catalog_name: &str,
    source_name: &str,
    cfg: &IcebergRestConfig,
    tables: &[TableConfig],
    retry_settings: RetrySettings,
) -> Result<()> {
    let cb = Arc::new(AdaptiveCircuitBreaker::new(CircuitBreakerConfig::default()));

    retry_async(
        &format!("iceberg_register({})", source_name),
        retry_settings,
        || {
            let cb = cb.clone();
            async move {
                try_register_iceberg_rest(ctx, catalog_name, source_name, cfg, tables, cb).await
            }
        },
    )
    .await
}

async fn try_register_iceberg_rest(
    ctx: &SessionContext,
    catalog_name: &str,
    source_name: &str,
    cfg: &IcebergRestConfig,
    tables: &[TableConfig],
    cb: Arc<AdaptiveCircuitBreaker>,
) -> Result<()> {
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

    let s3_auth: Box<dyn IcebergAuthProvider> = Box::new(AwsIrsaAuth::new());
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

    let max_concurrency = cfg.max_concurrent_queries.unwrap_or(0);

    // 4. Register tables lazily (now truly lazy without eager loading)
    for table_cfg in tables {
        let namespace = match &cfg.namespace {
            Some(ns) if !ns.is_empty() => {
                let parts: Vec<String> = ns.split('.').map(|s| s.to_string()).collect();
                NamespaceIdent::from_vec(parts).unwrap_or_else(|e| {
                    tracing::warn!(
                        namespace = %ns,
                        error = %e,
                        "Failed to parse namespace, falling back to 'default'"
                    );
                    NamespaceIdent::new("default".to_string())
                })
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
        let schema = dummy_provider.schema();

        let lazy_provider = LazyIcebergTableProvider::new(
            iceberg_catalog.clone(),
            ident.clone(),
            cfg.version.clone(),
            schema.clone(),
        );

        // Wrap with federation adaptor to enable pushdown/join splitting
        // Use SQLTableSource for federation logic
        let sql_source = datafusion_federation::sql::SQLTableSource::new_with_schema(
            federation_provider.clone(),
            TableReference::full(catalog_name, schema_name, table_cfg.name.as_str()).into(),
            schema,
        );

        // Wrap with federation adaptor
        let federated_provider = Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            Arc::new(sql_source),
            Arc::new(lazy_provider),
        ));

        let enriched_provider =
            wrap_provider(federated_provider, cb.clone(), FetchedMetadata::default());
        let limited_provider = wrap_concurrent(enriched_provider, max_concurrency);
        let qualified = TableReference::full(catalog_name, schema_name, table_cfg.name.as_str());
        ctx.register_table(qualified, limited_provider)?;

        // Note: we track registration metric immediately as we don't load anymore
        IcebergTelemetry::table_registered(catalog_name, schema_name, &table_cfg.name);
    }

    Ok(())
}

/// Lazy wrapper that loads the Iceberg table only when scanned or when schema is requested
#[derive(Debug)]
pub struct LazyIcebergTableProvider {
    catalog: Arc<CachedRestCatalog>,
    ident: TableIdent,
    version: Option<TableVersionSpec>,
    state: RwLock<LoadState>,
    schema_cache: OnceLock<SchemaRef>,
}

impl LazyIcebergTableProvider {
    pub fn new(
        catalog: Arc<CachedRestCatalog>,
        ident: TableIdent,
        version: Option<TableVersionSpec>,
        known_schema: SchemaRef,
    ) -> Self {
        let schema_cache = OnceLock::new();
        let _ = schema_cache.set(known_schema);

        Self {
            catalog,
            ident,
            version,
            state: RwLock::new(LoadState::Unloaded),
            schema_cache,
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

        let static_provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(|e| IcebergConnectorError::CatalogError {
                operation: "create_provider".into(),
                retries: 0,
                source: anyhow::anyhow!(e),
            })?;
        Ok(Arc::new(static_provider))
    }
}

#[async_trait]
impl TableProvider for LazyIcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        // Guaranteed to be present by constructor
        self.schema_cache.get().cloned().unwrap_or_else(|| {
            // Should never happen
            Arc::new(datafusion::arrow::datatypes::Schema::empty())
        })
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

        // Check if loaded without triggering load
        if let Ok(guard) = self.state.try_read() {
            if let LoadState::Loaded(provider) = &*guard {
                return provider.statistics();
            }
        }

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
        let result = provider.scan(state, projection, filters, limit).await;

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
        // Ensure loaded to delegate pushdown logic (needs schema/partition spec)
        let _ = self.schema();

        if let Ok(guard) = self.state.try_read() {
            if let LoadState::Loaded(provider) = &*guard {
                return provider.supports_filters_pushdown(filters);
            }
        }

        Ok(vec![datafusion::logical_expr::TableProviderFilterPushDown::Unsupported; filters.len()])
    }
}
