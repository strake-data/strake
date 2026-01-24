use anyhow::Result;
use futures::future::BoxFuture;
use strake_common::models::SourcesConfig;

pub mod models;
pub mod postgres;
pub mod sqlite;
use models::{ApplyLogEntry, ApplyResult, DomainStatus};

pub trait MetadataStore: Send + Sync {
    /// Initialize the metadata store (e.g., create tables if they don't exist)
    fn init(&self) -> BoxFuture<'_, Result<()>>;

    /// Import/Sync sources configuration to the metadata store
    fn apply_sources<'a>(
        &'a self,
        config: &'a SourcesConfig,
        force: bool,
    ) -> BoxFuture<'a, Result<ApplyResult>>;

    /// Get current version of a domain
    fn get_domain_version<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<i32>>;

    /// Increment domain version with optimistic locking
    fn increment_domain_version<'a>(
        &'a self,
        domain: &'a str,
        expected_version: i32,
    ) -> BoxFuture<'a, Result<i32>>;

    /// Log an apply event for history/audit
    fn log_apply_event<'a>(&'a self, entry: ApplyLogEntry) -> BoxFuture<'a, Result<()>>;

    /// Get history of apply events
    fn get_history<'a>(
        &'a self,
        domain: &'a str,
        limit: i64,
    ) -> BoxFuture<'a, Result<Vec<ApplyLogEntry>>>;

    /// Get the sources configuration as stored in the DB (for diffing)
    fn get_sources<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<SourcesConfig>>;

    /// Get valid configuration YAML for a specific history version
    fn get_history_config<'a>(
        &'a self,
        domain: &'a str,
        version: i32,
    ) -> BoxFuture<'a, Result<String>>;

    /// List all domains and their status
    fn list_domains(&self) -> BoxFuture<'_, Result<Vec<DomainStatus>>>;
}

/// Initialize the metadata store based on configuration
pub async fn init_store(config: &crate::config::CliConfig) -> Result<Box<dyn MetadataStore>> {
    use crate::config::MetadataBackendConfig;
    use postgres::PostgresStore;
    use sqlite::SqliteStore;

    match &config.metadata {
        Some(MetadataBackendConfig::Sqlite { path }) => {
            Ok(Box::new(SqliteStore::new(path.clone())?))
        }
        Some(MetadataBackendConfig::Postgres { url }) => {
            Ok(Box::new(PostgresStore::new(url).await?))
        }
        None => Err(anyhow::anyhow!("No metadata backend configuration found.")),
    }
}
