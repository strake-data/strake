use crate::config::SourceConfig;
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

pub mod file;
pub mod flight;
pub mod grpc;
pub mod rest;
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

pub struct SourceRegistry {
    providers: std::collections::HashMap<&'static str, Box<dyn SourceProvider>>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self {
            providers: std::collections::HashMap::new(),
        }
    }

    pub fn register_provider(&mut self, provider: Box<dyn SourceProvider>) {
        self.providers.insert(provider.type_name(), provider);
    }

    pub async fn register_source(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        let type_name = match config.r#type.as_str() {
            "parquet" | "csv" | "json" => "file",
            other => other,
        };

        if let Some(provider) = self.providers.get(type_name) {
            provider.register(context, catalog_name, config).await
        } else {
            anyhow::bail!("No provider found for source type: {}", type_name)
        }
    }
}

pub fn default_registry(global_retry: crate::config::RetrySettings) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.register_provider(Box::new(sql::SqlSourceProvider { global_retry }));
    registry.register_provider(Box::new(flight::FlightSqlSourceProvider));
    registry.register_provider(Box::new(file::FileSourceProvider));
    registry.register_provider(Box::new(rest::RestSourceProvider { global_retry }));
    registry.register_provider(Box::new(grpc::GrpcSourceProvider { global_retry }));

    registry
}
