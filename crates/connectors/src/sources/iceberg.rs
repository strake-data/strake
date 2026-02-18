//! Apache Iceberg data source.
//!
//! Supports reading Iceberg tables via REST catalog with S3-backed storage.
//! Leverages partition pruning and snapshot isolation via iceberg-rust.
//!
//! # Configuration Example
//!
//! ```yaml
//! sources:
//!   - name: my_iceberg
//!     type: iceberg_rest
//!     config:
//!       catalog_uri: http://localhost:8181/v1
//!       warehouse: s3://bucket/warehouse
//!       region: us-east-1
//!       # OAuth authentication (preferred)
//!       oauth_client_id: "client-id"
//!       oauth_client_secret: "${OAUTH_SECRET}"
//!       oauth_token_url: https://auth.example.com/token
//!       # Or static token (not recommended for production)
//!       # token: "${STATIC_TOKEN}"
//! ```
use crate::sources::iceberg::error::IcebergConnectorError;
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use crate::sources::SourceProvider;
use strake_common::config::SourceConfig;

pub mod auth;
pub mod catalog;
pub mod error;
pub mod federation;
pub mod provider;
pub mod telemetry;

use provider::register_iceberg_rest;

use secrecy::SecretString;
use std::fmt;

/// Table version specification for time travel queries
#[derive(serde::Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TableVersionSpec {
    /// Query a specific snapshot by ID
    SnapshotId(i64),
    /// Query table state at a specific timestamp (milliseconds since epoch)
    Timestamp(i64),
    /// Query a named tag
    Tag(String),
    /// Query a named branch
    Branch(String),
}

impl TableVersionSpec {
    /// Validate the version specification
    pub fn validate(&self) -> Result<()> {
        match self {
            TableVersionSpec::SnapshotId(id) if *id <= 0 => {
                anyhow::bail!("Snapshot ID must be positive, got {}", id)
            }
            TableVersionSpec::Timestamp(ts) => {
                let dt = chrono::DateTime::from_timestamp_millis(*ts)
                    .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", ts))?;
                if dt > chrono::Utc::now() {
                    anyhow::bail!("Timestamp cannot be in the future: {}", dt);
                }
                Ok(())
            }
            TableVersionSpec::Tag(tag) if tag.is_empty() => {
                anyhow::bail!("Tag name cannot be empty")
            }
            TableVersionSpec::Branch(branch) if branch.is_empty() => {
                anyhow::bail!("Branch name cannot be empty")
            }
            _ => Ok(()),
        }
    }
}

/// Configuration for Iceberg REST catalog source
#[derive(serde::Deserialize, Clone)]
pub struct IcebergRestConfig {
    /// REST catalog URI (e.g., http://localhost:8181/v1)
    pub catalog_uri: String,
    /// Warehouse location (e.g., s3://bucket/warehouse)
    pub warehouse: String,
    /// Namespace for table lookups (optional)
    #[serde(default)]
    pub namespace: Option<String>,
    /// OAuth2 token for authentication (optional, static)
    #[serde(default)]
    pub token: Option<SecretString>,
    /// OAuth2 Client ID for token refresh
    #[serde(default)]
    pub oauth_client_id: Option<String>,
    /// OAuth2 Client Secret for token refresh
    #[serde(default)]
    pub oauth_client_secret: Option<SecretString>,
    /// OAuth2 Token URL
    #[serde(default)]
    pub oauth_token_url: Option<String>,
    /// OAuth2 Scopes
    #[serde(default)]
    pub oauth_scopes: Option<Vec<String>>,
    /// S3 region configuration
    pub region: String,
    /// S3 endpoint for non-AWS deployments (optional)
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    /// Request timeout in seconds (optional)
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,
    /// Maximum number of retries for HTTP requests (default: 3)
    #[serde(default)]
    pub max_retries: Option<u32>,

    /// Cache configuration (optional)
    #[serde(default)]
    pub cache: Option<CacheConfig>,
    /// Table version for time travel queries (optional)
    #[serde(default)]
    pub version: Option<TableVersionSpec>,
    /// Maximum number of concurrent Iceberg scans (default: 0 = unlimited)
    #[serde(default)]
    pub max_concurrent_queries: Option<usize>,
}

/// Configuration for catalog caching
#[derive(serde::Deserialize, Clone, Debug)]
pub struct CacheConfig {
    /// Table cache TTL in seconds (default: 300 = 5 minutes)
    #[serde(default = "default_table_cache_ttl")]
    pub table_ttl_secs: u64,
    /// Maximum number of cached tables (default: 10000)
    #[serde(default = "default_max_cached_tables")]
    pub max_tables: u64,
}

fn default_table_cache_ttl() -> u64 {
    300
}
fn default_max_cached_tables() -> u64 {
    10000
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            table_ttl_secs: default_table_cache_ttl(),
            max_tables: default_max_cached_tables(),
        }
    }
}

impl IcebergRestConfig {
    pub fn validate(&self) -> Result<()> {
        if let Some(max) = self.max_concurrent_queries {
            if max > 10_000 {
                anyhow::bail!("max_concurrent_queries cannot exceed 10000");
            }
        }

        // Validate version if specified
        if let Some(version) = &self.version {
            version.validate()?;
        }

        let has_static = self.token.is_some();
        let has_oauth = self.oauth_client_id.is_some()
            || self.oauth_client_secret.is_some()
            || self.oauth_token_url.is_some();

        if has_static && has_oauth {
            anyhow::bail!("Cannot specify both static token and OAuth credentials");
        }

        if has_oauth {
            if self.oauth_client_id.is_none() {
                anyhow::bail!("OAuth client_id required when using OAuth");
            }
            if self.oauth_client_secret.is_none() {
                anyhow::bail!("OAuth client_secret required when using OAuth");
            }
            if self.oauth_token_url.is_none() {
                anyhow::bail!("OAuth token_url required when using OAuth");
            }
        }

        // Validate warehouse URI scheme
        let warehouse_url = url::Url::parse(&self.warehouse).context("Invalid warehouse URL")?;

        let valid_schemes = ["s3", "gs", "az", "abfss", "file", "http", "https"];
        if !valid_schemes.contains(&warehouse_url.scheme()) {
            anyhow::bail!(
                "Unsupported warehouse scheme '{}'. Must be one of: {:?}",
                warehouse_url.scheme(),
                valid_schemes
            );
        }

        Ok(())
    }
}

impl fmt::Debug for IcebergRestConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergRestConfig")
            .field("catalog_uri", &self.catalog_uri)
            .field("warehouse", &self.warehouse)
            .field("namespace", &self.namespace)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("oauth_client_id", &self.oauth_client_id)
            .field(
                "oauth_client_secret",
                &self.oauth_client_secret.as_ref().map(|_| "***"),
            )
            .field("oauth_token_url", &self.oauth_token_url)
            .field("oauth_scopes", &self.oauth_scopes)
            .field("region", &self.region)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("max_retries", &self.max_retries)
            .field("cache", &self.cache)
            .field("version", &self.version)
            .finish()
    }
}

pub struct IcebergSourceProvider {
    pub global_retry: strake_common::config::RetrySettings,
}

#[async_trait]
impl SourceProvider for IcebergSourceProvider {
    fn type_name(&self) -> &'static str {
        "iceberg_rest"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        let cfg: IcebergRestConfig =
            serde_json::from_value(config.config.clone()).map_err(|e| {
                IcebergConnectorError::InvalidConfiguration(format!(
                    "Failed to parse configuration: {}",
                    e
                ))
            })?;

        cfg.validate()
            .map_err(|e| IcebergConnectorError::InvalidConfiguration(e.to_string()))?;

        // Basic validation
        if !cfg.catalog_uri.starts_with("http") {
            return Err(IcebergConnectorError::InvalidConfiguration(format!(
                "catalog_uri must be a valid HTTP(S) URL, got: {}",
                cfg.catalog_uri
            ))
            .into());
        }

        let effective_retry = self.global_retry;
        register_iceberg_rest(
            context,
            catalog_name,
            &config.name,
            &cfg,
            &config.tables,
            effective_retry,
        )
        .await
    }
}
