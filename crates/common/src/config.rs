pub use crate::models::{ColumnConfig, QueryCacheConfig, SourceConfig, TableConfig};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use validator::Validate;

// Default constants
pub const DEFAULT_PORT: u16 = 50051;
pub const DEFAULT_HEALTH_PORT: u16 = 8080;
pub const DEFAULT_API_PORT: u16 = 8080;
pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:50051";
pub const DEFAULT_HEALTH_ADDR: &str = "0.0.0.0:8080";
pub const DEFAULT_API_URL: &str = "http://localhost:8080/api/v1";
pub const DEFAULT_CATALOG: &str = "strake";
pub const DEFAULT_SERVER_NAME: &str = "Strake Server";
pub const DEFAULT_GLOBAL_CONNECTION_BUDGET: usize = 100;

pub const DEFAULT_LIMIT: usize = 1000;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_BASE_DELAY_MS: u64 = 1000;
pub const DEFAULT_MAX_DELAY_MS: u64 = 60000;

pub const DEFAULT_API_KEY: &str = "dev-key";
pub const DEFAULT_CACHE_TTL: u64 = 300;
pub const DEFAULT_CACHE_CAPACITY: u64 = 10000;

pub const DEFAULT_TELEMETRY_ENABLED: bool = false;
pub const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub cache: QueryCacheConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct QueryLimits {
    #[serde(default)]
    pub max_output_rows: Option<usize>,
    #[serde(default)]
    pub max_scan_bytes: Option<usize>,
    #[serde(default = "default_limit")]
    pub default_limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
pub struct RetrySettings {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
}

fn default_limit() -> Option<usize> {
    Some(DEFAULT_LIMIT)
}

fn default_max_attempts() -> u32 {
    DEFAULT_MAX_ATTEMPTS
}
fn default_base_delay_ms() -> u64 {
    DEFAULT_BASE_DELAY_MS
}
fn default_max_delay_ms() -> u64 {
    DEFAULT_MAX_DELAY_MS
}

#[derive(Debug, Deserialize, Default, Clone, Validate)]
pub struct AppConfig {
    #[serde(default)]
    #[validate(nested)]
    pub server: ServerSettings,
    #[serde(default)]
    pub query_limits: QueryLimits,
    #[serde(default)]
    pub retry: RetrySettings,
    #[serde(default)]
    pub resources: ResourceConfig,
    #[serde(default)]
    pub cache: QueryCacheConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Deserialize, Clone, Validate)]
pub struct TelemetryConfig {
    #[serde(default = "default_telemetry_enabled")]
    pub enabled: bool,

    #[serde(default = "default_otlp_endpoint")]
    #[validate(url)]
    pub endpoint: String,

    #[serde(default = "default_service_name_config")]
    pub service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: default_telemetry_enabled(),
            endpoint: default_otlp_endpoint(),
            service_name: default_service_name_config(),
        }
    }
}

fn default_telemetry_enabled() -> bool {
    DEFAULT_TELEMETRY_ENABLED
}

fn default_otlp_endpoint() -> String {
    DEFAULT_OTLP_ENDPOINT.to_string()
}

fn default_service_name_config() -> String {
    DEFAULT_SERVER_NAME.to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct McpConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_mcp_port")]
    pub port: u16,
    #[serde(default = "default_sidecar_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_sidecar_retry_delay_ms")]
    pub retry_delay_ms: u64,
    #[serde(default = "default_sidecar_startup_delay_ms")]
    pub startup_delay_ms: u64,
    #[serde(default = "default_sidecar_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
    #[serde(default)]
    pub python_bin: Option<String>,
    #[serde(default)]
    pub health_check_url: Option<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_mcp_port(),
            max_retries: default_sidecar_max_retries(),
            retry_delay_ms: default_sidecar_retry_delay_ms(),
            startup_delay_ms: default_sidecar_startup_delay_ms(),
            shutdown_timeout_ms: default_sidecar_shutdown_timeout_ms(),
            python_bin: None,
            health_check_url: None,
        }
    }
}

fn default_mcp_port() -> u16 {
    8001
}

fn default_sidecar_max_retries() -> u32 {
    5
}

fn default_sidecar_retry_delay_ms() -> u64 {
    1000
}

fn default_sidecar_startup_delay_ms() -> u64 {
    500
}

fn default_sidecar_shutdown_timeout_ms() -> u64 {
    5000
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct ResourceConfig {
    pub memory_limit_mb: Option<usize>,
    pub spill_dir: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone, Validate)]
pub struct ServerSettings {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    #[serde(default = "default_health_addr")]
    pub health_addr: String,

    #[serde(default = "default_catalog")]
    pub catalog: String,

    #[serde(default = "default_api_url")]
    #[validate(custom(function = "validate_api_url"))]
    pub api_url: String,

    #[serde(default = "default_global_budget")]
    pub global_connection_budget: usize,

    #[serde(default = "default_database_url")]
    pub database_url: String,

    #[serde(default)]
    pub tls: TlsSettings,

    #[serde(default)]
    pub auth: AuthSettings,

    #[serde(default)]
    pub oidc: Option<OidcConfig>,

    #[serde(default)]
    pub datafusion_config: HashMap<String, String>,

    #[serde(default = "default_server_name")]
    pub name: String,
}

fn default_listen_addr() -> String {
    DEFAULT_LISTEN_ADDR.to_string()
}

fn default_health_addr() -> String {
    DEFAULT_HEALTH_ADDR.to_string()
}

#[allow(dead_code)]
fn default_pool_size() -> usize {
    10
}

fn default_global_budget() -> usize {
    DEFAULT_GLOBAL_CONNECTION_BUDGET
}

fn default_database_url() -> String {
    String::new()
}

fn default_catalog() -> String {
    DEFAULT_CATALOG.to_string()
}

fn default_api_url() -> String {
    DEFAULT_API_URL.to_string()
}

fn default_server_name() -> String {
    DEFAULT_SERVER_NAME.to_string()
}

fn validate_api_url(url: &str) -> Result<(), validator::ValidationError> {
    if url.is_empty() {
        return Ok(()); // Allow empty - will use default
    }

    // Use the url crate to parse and validate
    match url::Url::parse(url) {
        Ok(_) => Ok(()),
        Err(_) => Err(validator::ValidationError::new("invalid_url")),
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct TlsSettings {
    pub enabled: bool,
    #[serde(default)]
    pub cert: String,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct OidcConfig {
    pub issuer_url: String,
    pub audience: Vec<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AuthSettings {
    pub enabled: bool,
    #[serde(default = "default_api_key")]
    pub api_key: String,
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
    #[serde(default = "default_cache_capacity")]
    pub cache_max_capacity: u64,
}

fn default_api_key() -> String {
    DEFAULT_API_KEY.to_string()
}

fn default_cache_ttl() -> u64 {
    DEFAULT_CACHE_TTL
}

fn default_cache_capacity() -> u64 {
    DEFAULT_CACHE_CAPACITY
}

// Config implementation
impl AppConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let builder = config::Config::builder();

        let builder = if std::path::Path::new(path).exists() {
            builder.add_source(config::File::with_name(path))
        } else {
            builder
        };

        // Add environment variables
        // Map STRAKE_SERVER__LISTEN_ADDR to server.listen_addr, etc.
        let builder = builder.add_source(
            config::Environment::with_prefix("STRAKE")
                .separator("__")
                .try_parsing(true),
        );

        let cfg = builder.build().context("Failed to build configuration")?;

        let app_config: AppConfig = cfg
            .try_deserialize()
            .context("Failed to deserialize configuration")?;

        // Validate
        app_config
            .validate()
            .map_err(|e| anyhow::anyhow!("Configuration validation failed: {:?}", e))?;

        Ok(app_config)
    }
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let builder = config::Config::builder();

        let builder = if std::path::Path::new(path).exists() {
            builder.add_source(config::File::with_name(path))
        } else {
            builder
        };

        // Generally Source Configs are loaded from a specific file, not ENV vars for every list item usually.
        // But we can check if there are overrides.

        let cfg = builder
            .build()
            .context("Failed to build sources configuration")?;

        let config: Config = cfg
            .try_deserialize()
            .context("Failed to deserialize sources configuration")?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_config_validation() {
        let config = AppConfig::default();
        // Should validate OK with defaults (assuming defaults are valid)
        // api_url default is http://... which is valid URL
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_telemetry_config_validation() {
        let config = TelemetryConfig {
            endpoint: "not_a_url".to_string(),
            ..Default::default()
        };
        // Validation should fail via Validate derive
        assert!(config.validate().is_err());
    }
}
