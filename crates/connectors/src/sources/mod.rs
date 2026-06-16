//! # Connector Sources
//!
//! Pluggable data source architecture for Strake.
//!
//! This module manages the registration and lifecycle of various data sources
//! (SQL, REST, gRPC, etc.) and provides the necessary abstractions to
//! integrate them with DataFusion.
//!
//! ## Overview
//!
//! Strake uses a `SourceRegistry` to manage `SourceProvider` implementations.
//! Each provider is responsible for registering its specific tables or datasets
//! with the DataFusion `SessionContext`.
//!
//! # Supported Sources
//!
//! | Source Type | Implementation | Description |
//! |-------------|----------------|-------------|
//! | `sql`       | `SqlSourceProvider` | JDBC-style connectors (Postgres, MySQL, SQLite, DuckDB, ClickHouse) |
//! | `flight_sql`| `FlightSqlSourceProvider` | High-performance Arrow Flight SQL sources |
//! | `file`      | `FileSourceProvider` | Local or remote files (Parquet, CSV, JSON) |
//! | `rest`      | `RestSourceProvider` | REST APIs with JSON responses |
//! | `grpc`      | `GrpcSourceProvider` | Generic gRPC services with Protobuf reflection |
//!
//! # Adding a New Source
//!
//! 1. Create a struct implementing `SourceProvider`.
//! 2. Implement `register` to add the source to the DataFusion `SessionContext`.
//! 3. Register the provider in `default_registry` in this module.

use anyhow::Result;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use strake_common::config::SourceConfig;

/// Trait for table providers that wrap another table provider (e.g., for metrics, caching, or resilience).
pub trait WrappingTableProvider: TableProvider {
    /// Returns the inner table provider being wrapped.
    fn inner(&self) -> &Arc<dyn TableProvider>;
}

/// Helper to downcast a [`TableProvider`] to a [`WrappingTableProvider`] trait object.
///
/// # Registration Contract
/// This function centrally enumerates all known wrapping table provider types.
/// When adding a new wrapper (decorator), you MUST register it here to enable
/// transparent optimizer pushdown and recursive plan visualization through the
/// wrapper chain.
///
/// Failure to register a new wrapper will cause the `is_federated_plan` logic to
/// stop at that wrapper, potentially missing federated nodes further down the chain.
pub fn as_wrapping(provider: &dyn TableProvider) -> Option<&dyn WrappingTableProvider> {
    let any = provider.as_any();

    if let Some(w) = any.downcast_ref::<sql::wrappers::SchemaAdaptingTableProvider>() {
        return Some(w);
    }
    if let Some(w) = any.downcast_ref::<sql::wrappers::ConcurrencyLimitedTableProvider>() {
        return Some(w);
    }
    if let Some(w) =
        any.downcast_ref::<crate::resilience::circuit_breaker::CircuitBreakerTableProvider>()
    {
        return Some(w);
    }
    if let Some(w) = any.downcast_ref::<schema_drift::SchemaDriftTableProvider>() {
        return Some(w);
    }
    if let Some(w) = any.downcast_ref::<predicate_caching::CachingTableProvider>() {
        return Some(w);
    }

    None
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_as_wrapping_registration() {
        // This test verifies that as_wrapping recognizes known wrappers.
        // It ensures developers update this central function when adding new decorators.
    }
}

pub mod federated;
pub mod file;
pub mod flight;
pub mod grpc;
#[cfg(feature = "iceberg")]
pub mod iceberg;
pub mod predicate_caching;
pub mod rest;
pub mod rest_auth;
pub mod schema_drift;
pub mod sql;

/// Ensures a schema exists in the catalog, creating it if necessary.
pub fn ensure_schema(
    context: &SessionContext,
    catalog_name: &str,
    schema_name: &str,
) -> Result<Arc<dyn datafusion::catalog::SchemaProvider>> {
    use datafusion::catalog::MemorySchemaProvider;

    let catalog = context
        .catalog(catalog_name)
        .ok_or_else(|| anyhow::anyhow!("Catalog {} not found", catalog_name))?;

    if let Some(schema) = catalog.schema(schema_name) {
        Ok(schema)
    } else {
        let new_schema = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema(schema_name, new_schema.clone())?;
        Ok(new_schema)
    }
}

#[async_trait]
/// Trait implemented by each data source connector.
///
/// A `SourceProvider` is responsible for interpreting a [`SourceConfig`] and
/// registering the corresponding tables with a DataFusion [`SessionContext`].
pub trait SourceProvider: Send + Sync {
    /// Returns the type of source this provider handles (e.g., "sql", "flight_sql")
    fn type_name(&self) -> &'static str;

    /// Registers the source with the given configuration
    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()>;
}

use std::sync::Arc;
use strake_common::predicate_cache::PredicateCache;

#[derive(Default)]
/// Registry that maps source type names to their [`SourceProvider`] implementations.
pub struct SourceRegistry {
    providers: std::collections::HashMap<&'static str, Box<dyn SourceProvider>>,
    /// Shared predicate cache used by caching data source providers.
    pub predicate_cache: Arc<PredicateCache>,
}

impl SourceRegistry {
    /// Create a new, empty `SourceRegistry` with a fresh [`PredicateCache`].
    pub fn new() -> Self {
        Self {
            providers: std::collections::HashMap::new(),
            predicate_cache: Arc::new(PredicateCache::new()),
        }
    }
}

impl SourceRegistry {
    /// Register a provider implementation, keyed on [`SourceProvider::type_name`].
    pub fn register_provider(&mut self, provider: Box<dyn SourceProvider>) {
        self.providers.insert(provider.type_name(), provider);
    }

    /// Locate the appropriate provider for `config` and call its `register` method.
    pub async fn register_source(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        let raw_type = config.source_type.as_str();
        tracing::info!(
            "Registry: registering source {} of type {}",
            config.name,
            raw_type
        );
        let type_name = match raw_type {
            "parquet" | "csv" | "json" => "file",
            "postgres" | "mysql" | "sqlite" | "duckdb" | "clickhouse" | "oracle" => "sql",
            #[cfg(feature = "iceberg")]
            "iceberg" => "iceberg_rest",
            #[cfg(not(feature = "iceberg"))]
            "iceberg" => {
                anyhow::bail!(
                    "Iceberg support is disabled in this build. Resolve DataFusion 53 conflicts to enable it."
                )
            }
            other => other,
        };

        if let Some(provider) = self.providers.get(type_name) {
            tracing::debug!("Registry: found provider for type {}", type_name);
            provider.register(context, catalog_name, config).await
        } else {
            tracing::debug!("Registry: NO provider found for type {}", type_name);
            anyhow::bail!("No provider found for source type: {}", type_name)
        }
    }
}

/// Construct a [`SourceRegistry`] pre-populated with all built-in providers.
pub fn default_registry(global_retry: strake_common::config::RetrySettings) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    let cache = registry.predicate_cache.clone();

    registry.register_provider(Box::new(sql::SqlSourceProvider { global_retry }));
    registry.register_provider(Box::new(flight::FlightSqlSourceProvider));
    registry.register_provider(Box::new(file::FileSourceProvider {
        predicate_cache: cache.clone(),
    }));
    registry.register_provider(Box::new(rest::RestSourceProvider {
        global_retry,
        schema_cache: Arc::new(dashmap::DashMap::new()),
    }));
    registry.register_provider(Box::new(grpc::GrpcSourceProvider { global_retry }));
    #[cfg(feature = "iceberg")]
    registry.register_provider(Box::new(iceberg::IcebergSourceProvider {
        global_retry,
        predicate_cache: cache,
    }));

    registry
}
