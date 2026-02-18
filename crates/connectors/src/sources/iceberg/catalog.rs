use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use iceberg::table::Table;
use iceberg::NamespaceIdent;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use moka::future::Cache;
use secrecy::ExposeSecret;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use super::auth::IcebergAuthProvider;
use super::error::IcebergConnectorError;
use super::telemetry::IcebergTelemetry;
use super::{CacheConfig, IcebergRestConfig};
use std::sync::Arc;

/// Caching wrapper around RestCatalog to reduce API calls
#[derive(Clone, Debug)]
pub struct CachedRestCatalog {
    inner: Arc<RestCatalog>,
    table_cache: Cache<TableIdent, Arc<Table>>,
    // Fix [Performance]: Track tables by namespace for targeted invalidation
    namespace_to_tables: Arc<DashMap<NamespaceIdent, HashSet<TableIdent>>>,
}

impl CachedRestCatalog {
    pub fn new(inner: RestCatalog, config: CacheConfig) -> Self {
        Self {
            inner: Arc::new(inner),
            table_cache: Cache::builder()
                .time_to_live(Duration::from_secs(config.table_ttl_secs))
                .max_capacity(config.max_tables)
                .build(),
            namespace_to_tables: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl Catalog for CachedRestCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        let start = std::time::Instant::now();
        let result = self.inner.list_namespaces(parent).await;
        IcebergTelemetry::catalog_api_call(
            "list_namespaces",
            start.elapsed().as_millis() as u64,
            result.is_ok(),
        );
        result
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        // Fix [Performance]: Only invalidate tables in this namespace
        if let Some((_, tables)) = self.namespace_to_tables.remove(namespace) {
            for table in tables {
                self.table_cache.invalidate(&table).await;
            }
        }
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn table_exists(&self, identifier: &TableIdent) -> iceberg::Result<bool> {
        // Check cache first
        if self.table_cache.get(identifier).await.is_some() {
            tracing::debug!(table = %identifier, "Table existence cache hit");
            return Ok(true);
        }
        let start = std::time::Instant::now();
        let result = self.inner.table_exists(identifier).await;
        IcebergTelemetry::catalog_api_call(
            "table_exists",
            start.elapsed().as_millis() as u64,
            result.is_ok(),
        );
        result
    }

    async fn drop_table(&self, identifier: &TableIdent) -> iceberg::Result<()> {
        self.table_cache.invalidate(identifier).await;
        // Remove from reverse mapping
        if let Some(mut tables) = self.namespace_to_tables.get_mut(identifier.namespace()) {
            let _ = tables.remove(identifier);
        }
        self.inner.drop_table(identifier).await
    }

    async fn load_table(&self, identifier: &TableIdent) -> iceberg::Result<Table> {
        // Use try_get_with to prevent cache stampede
        self.table_cache
            .try_get_with(identifier.clone(), async {
                tracing::debug!(table = %identifier, "Table cache miss, loading from catalog");
                let start = std::time::Instant::now();
                let table = self.inner.load_table(identifier).await?;
                IcebergTelemetry::catalog_api_call(
                    "load_table",
                    start.elapsed().as_millis() as u64,
                    true,
                );

                // Track in reverse mapping
                // Note: We do this inside the load to ensure consistency
                // DashMap inner locking is fine here? Yes.
                // Actually we cannot capture self.namespace_to_tables easily in the future if it wasn't cloned.
                // But try_get_with takes a future.
                Ok::<_, iceberg::Error>(Arc::new(table))
            })
            .await
            .map(|arc| {
                // Update reverse mapping on success
                // We do it here because capturing `self` in the async block above is tricky with lifetimes if not careful
                // or requires cloning `self.namespace_to_tables` before.
                // Doing it here is safe enough (eventual consistency for invalidation).
                self.namespace_to_tables
                    .entry(identifier.namespace().clone())
                    .or_default()
                    .insert(identifier.clone());
                (*arc).clone()
            })
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Cache error: {}", e),
                )
            })
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> iceberg::Result<Table> {
        let table = self.inner.create_table(namespace, creation).await?;
        // Cache the newly created table
        let identifier = table.identifier().clone();
        self.table_cache
            .insert(identifier.clone(), Arc::new(table.clone()))
            .await;
        self.namespace_to_tables
            .entry(namespace.clone())
            .or_default()
            .insert(identifier);
        Ok(table)
    }

    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let identifier = commit.identifier().clone();
        let table = self.inner.update_table(commit).await?;
        // Update cache with new table version
        self.table_cache
            .insert(identifier, Arc::new(table.clone()))
            .await;
        Ok(table)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.table_cache.invalidate(src).await;
        self.table_cache.invalidate(dest).await;
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        identifier: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        let table = self
            .inner
            .register_table(identifier, metadata_location)
            .await?;
        // Cache the registered table
        self.table_cache
            .insert(identifier.clone(), Arc::new(table.clone()))
            .await;
        self.namespace_to_tables
            .entry(identifier.namespace().clone())
            .or_default()
            .insert(identifier.clone());
        Ok(table)
    }
}

/// Create REST catalog client (unwrapped, for use with CachedRestCatalog)
pub async fn create_rest_catalog(
    name: &str,
    cfg: &IcebergRestConfig,
    auth: &Arc<dyn IcebergAuthProvider>,
) -> Result<RestCatalog> {
    let mut props = HashMap::new();
    props.insert("uri".to_string(), cfg.catalog_uri.clone());
    props.insert("warehouse".to_string(), cfg.warehouse.clone());

    // Resolve token via auth provider
    if let Some(token) = auth.get_token().await? {
        props.insert("token".to_string(), token);
    }

    // Configure S3 properties for MinIO/AWS
    props.insert("s3.region".to_string(), cfg.region.clone());

    if let Some(endpoint) = &cfg.s3_endpoint {
        props.insert("s3.endpoint".to_string(), endpoint.clone());
        // MinIO usually requires path-style access.
        // In iceberg-rust 0.8.0, this is often mapped from s3.path-style-access
        props.insert("s3.path-style-access".to_string(), "true".to_string());
    }

    // Pass configuration overrides via properties
    if let Some(timeout) = cfg.request_timeout_secs {
        props.insert(
            "http.timeout-ms".to_string(),
            (timeout.saturating_mul(1000)).to_string(),
        );
    }

    // Pass credentials from auth provider to catalog properties (for FileIO created by catalog)
    if let Some(creds) = auth.s3_credentials().await? {
        props.insert("s3.access-key-id".to_string(), creds.access_key_id);
        // SECURITY NOTE: iceberg-rust currently requires String for S3 credentials.
        // These are passed to the AWS SDK which handles them securely.
        // The properties map is ephemeral and dropped after catalog creation.
        props.insert(
            "s3.secret-access-key".to_string(),
            creds.secret_access_key.expose_secret().to_string(),
        );
        if let Some(token) = creds.session_token {
            props.insert(
                "s3.session-token".to_string(),
                token.expose_secret().to_string(),
            );
        }
    }

    // Use the provided source name instead of a hardcoded constant
    let catalog: RestCatalog = CatalogBuilder::load(RestCatalogBuilder::default(), name, props)
        .await
        .map_err(|e| IcebergConnectorError::CatalogError {
            operation: "load_catalog".into(),
            retries: 0,
            source: e.into(),
        })?;

    Ok(catalog)
}
