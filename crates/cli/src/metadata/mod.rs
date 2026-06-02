//! # Metadata Traits
//!
//! Trait definitions for Metadata database backends.
//!
//! ## Overview
//!
//! Defines the generic `MetadataStore` interface that concrete implementations (e.g.,
//! SQLite, Postgres) must conform to. This separation permits transparent driver switching
//! in the CLI via configuration.
//!
//! ## Usage
//!
//! ```ignore
//! // pub trait MetadataStore: Send + Sync { ... }
//! ```
//!
//! ## Performance Characteristics
//!
//! Trait abstraction uses dynamic dispatch (`BoxFuture`) at async API boundaries, which adds minimal boxing overhead.
//!
//! ## Safety
//!
//! Trait objects are strictly `Send + Sync`.
//!
//! ## References
//!
//! - Core Interfaces Doc.

use anyhow::Result;
use futures::future::BoxFuture;

/// The expected length of the API key prefix.
pub const KEY_PREFIX_LEN: usize = 8;

pub mod models;
pub mod postgres;
pub mod sqlite;
use models::ApiKeyInfo;

/// Interface for metadata storage backends that manage domain configurations and history.
pub trait MetadataStore: Send + Sync {
    /// Initialize the metadata store (e.g., create tables if they don't exist)
    fn init(&self) -> BoxFuture<'_, Result<()>>;

    /// Runs outstanding database schema migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if applying the migrations fails.
    fn migrate(&self) -> BoxFuture<'_, Result<()>> {
        // Default implementation delegates to init() which runs all migrations in a database batch block.
        self.init()
    }

    /// Create a new API key in the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying database insertion fails or if a
    /// uniqueness constraint on `key_prefix` is violated.
    fn create_api_key<'a>(
        &'a self,
        name: &'a str,
        description: Option<&'a str>,
        user_id: &'a str,
        key_prefix: &'a str,
        key_hash: &'a str,
        permissions: &'a [String],
    ) -> BoxFuture<'a, Result<()>>;

    /// List all API keys in the store, ordered by creation time descending.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    fn list_api_keys(&self) -> BoxFuture<'_, Result<Vec<ApiKeyInfo>>>;

    /// Revoke an active API key by its prefix.
    ///
    /// Returns `true` if a key was revoked, `false` if no active key matched.
    ///
    /// # Errors
    ///
    /// Returns an error if the update query fails.
    fn revoke_api_key<'a>(&'a self, key_prefix: &'a str) -> BoxFuture<'a, Result<bool>>;
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
