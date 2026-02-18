//! Data source abstractions and implementations.
//!
//! Strake uses a pluggable source architecture where each data source implements
//! the `SourceProvider` trait. This module manages the registration and lifecycle
//! of these sources.
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
use datafusion::prelude::SessionContext;
use strake_common::config::SourceConfig;

pub mod file;
pub mod flight;
pub mod grpc;
pub mod iceberg;
pub mod rest;
pub mod rest_auth;
pub mod sql;

#[async_trait]
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

#[derive(Default)]
pub struct SourceRegistry {
    providers: std::collections::HashMap<&'static str, Box<dyn SourceProvider>>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SourceRegistry {
    pub fn register_provider(&mut self, provider: Box<dyn SourceProvider>) {
        self.providers.insert(provider.type_name(), provider);
    }

    pub async fn register_source(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        let type_name = match config.source_type.as_str() {
            "parquet" | "csv" | "json" => "file",
            "iceberg" => "iceberg_rest",
            other => other,
        };

        if let Some(provider) = self.providers.get(type_name) {
            provider.register(context, catalog_name, config).await
        } else {
            anyhow::bail!("No provider found for source type: {}", type_name)
        }
    }
}

pub fn default_registry(global_retry: strake_common::config::RetrySettings) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.register_provider(Box::new(sql::SqlSourceProvider { global_retry }));
    registry.register_provider(Box::new(flight::FlightSqlSourceProvider));
    registry.register_provider(Box::new(file::FileSourceProvider));
    registry.register_provider(Box::new(rest::RestSourceProvider { global_retry }));
    registry.register_provider(Box::new(grpc::GrpcSourceProvider { global_retry }));
    registry.register_provider(Box::new(iceberg::IcebergSourceProvider { global_retry }));

    registry
}
