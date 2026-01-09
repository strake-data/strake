use std::fs;

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

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

pub const DEFAULT_CACHE_ENABLED: bool = false;
pub const DEFAULT_CACHE_DIR: &str = "/tmp/strake-cache";
pub const DEFAULT_CACHE_MAX_SIZE_MB: u64 = 10240; // 10GB
pub const DEFAULT_CACHE_TTL_SECONDS: u64 = 3600; // 1 hour

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub cache: QueryCacheConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub r#type: String,
    pub default_limit: Option<usize>,
    /// Per-source cache override (optional)
    #[serde(default)]
    pub cache: Option<QueryCacheConfig>,
    #[serde(flatten)]
    pub config: serde_yaml::Value,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    pub schema: Option<String>,
    pub partition_column: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<ColumnConfig>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub length: Option<u32>,
    pub primary_key: Option<bool>,
    pub not_null: Option<bool>,
    pub unique: Option<bool>,
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

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AppConfig {
    #[serde(default)]
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

#[derive(Debug, Deserialize, Default, Clone)]
pub struct ServerSettings {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_health_addr")]
    pub health_addr: String,
    #[serde(default = "default_catalog")]
    pub catalog: String,
    #[serde(default = "default_api_url")]
    pub api_url: String,
    #[serde(default = "default_global_budget")]
    pub global_connection_budget: usize,
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

fn default_catalog() -> String {
    DEFAULT_CATALOG.to_string()
}

fn default_api_url() -> String {
    DEFAULT_API_URL.to_string()
}

fn default_server_name() -> String {
    DEFAULT_SERVER_NAME.to_string()
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

// Query result cache configuration
#[derive(Debug, Deserialize, Clone)]
pub struct QueryCacheConfig {
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cache_directory")]
    pub directory: String,
    #[serde(default = "default_cache_max_size_mb")]
    pub max_size_mb: u64,
    #[serde(default = "default_cache_ttl_seconds")]
    pub ttl_seconds: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            directory: default_cache_directory(),
            max_size_mb: default_cache_max_size_mb(),
            ttl_seconds: default_cache_ttl_seconds(),
        }
    }
}

fn default_cache_enabled() -> bool {
    DEFAULT_CACHE_ENABLED
}

fn default_cache_directory() -> String {
    DEFAULT_CACHE_DIR.to_string()
}

fn default_cache_max_size_mb() -> u64 {
    DEFAULT_CACHE_MAX_SIZE_MB
}

fn default_cache_ttl_seconds() -> u64 {
    DEFAULT_CACHE_TTL_SECONDS
}

impl AppConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let mut config: AppConfig = if std::path::Path::new(path).exists() {
            let content = fs::read_to_string(path)
                .context(format!("Failed to read config file at {}", path))?;
            serde_yaml::from_str(&content)
                .context(format!("Failed to parse app config file at {}", path))?
        } else {
            // Allow starting without config file if defaults/env vars are enough
            AppConfig::default()
        };

        // Environment variable overrides for server settings
        if let Ok(addr) = std::env::var("STRAKE_SERVER__LISTEN_ADDR") {
            config.server.listen_addr = addr;
        }
        if let Ok(addr) = std::env::var("STRAKE_SERVER__HEALTH_ADDR") {
            config.server.health_addr = addr;
        }
        if let Ok(url) = std::env::var("STRAKE_API_URL") {
            config.server.api_url = url;
        }
        if let Ok(catalog) = std::env::var("STRAKE_SERVER__CATALOG") {
            config.server.catalog = catalog;
        }
        if let Ok(budget) = std::env::var("STRAKE_SERVER__GLOBAL_CONNECTION_BUDGET") {
            if let Ok(val) = budget.parse() {
                config.server.global_connection_budget = val;
            }
        }
        if let Ok(enabled) = std::env::var("STRAKE_AUTH__ENABLED") {
            if let Ok(val) = enabled.parse() {
                config.server.auth.enabled = val;
            }
        }
        if let Ok(key) = std::env::var("STRAKE_AUTH__API_KEY") {
            config.server.auth.api_key = key;
        }
        if let Ok(attempts) = std::env::var("STRAKE_RETRY__MAX_ATTEMPTS") {
            if let Ok(val) = attempts.parse() {
                config.retry.max_attempts = val;
            }
        }
        if let Ok(rows) = std::env::var("STRAKE_QUERY_LIMITS__MAX_OUTPUT_ROWS") {
            if let Ok(val) = rows.parse() {
                config.query_limits.max_output_rows = Some(val);
            }
        }
        if let Ok(enabled) = std::env::var("STRAKE_MCP__ENABLED") {
            if let Ok(val) = enabled.parse() {
                config.mcp.enabled = val;
            }
        }
        if let Ok(port) = std::env::var("STRAKE_MCP__PORT") {
            if let Ok(val) = port.parse() {
                config.mcp.port = val;
            }
        }
        if let Ok(val) = std::env::var("STRAKE_MCP__MAX_RETRIES") {
            if let Ok(v) = val.parse() {
                config.mcp.max_retries = v;
            }
        }
        if let Ok(val) = std::env::var("STRAKE_MCP__RETRY_DELAY_MS") {
            if let Ok(v) = val.parse() {
                config.mcp.retry_delay_ms = v;
            }
        }
        if let Ok(val) = std::env::var("STRAKE_MCP__STARTUP_DELAY_MS") {
            if let Ok(v) = val.parse() {
                config.mcp.startup_delay_ms = v;
            }
        }
        if let Ok(val) = std::env::var("STRAKE_MCP__SHUTDOWN_TIMEOUT_MS") {
            if let Ok(v) = val.parse() {
                config.mcp.shutdown_timeout_ms = v;
            }
        }
        if let Ok(val) = std::env::var("STRAKE_MCP__PYTHON_BIN") {
            config.mcp.python_bin = Some(val);
        }
        if let Ok(val) = std::env::var("STRAKE_MCP__HEALTH_CHECK_URL") {
            config.mcp.health_check_url = Some(val);
        }

        Ok(config)
    }
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).context(format!("Failed to read config file at {}", path))?;
        let config = serde_yaml::from_str(&content).context("Failed to parse config file")?;
        Ok(config)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_config_parsing() {
        let yaml = r#"
server:
  listen_addr: "0.0.0.0:50053"
  health_addr: "0.0.0.0:8088"
  catalog: "strake"
  global_connection_budget: 100
  tls:
    enabled: false
  auth:
    enabled: false
query_limits:
  max_output_rows: 10000
retry:
  max_attempts: 3
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server.listen_addr, "0.0.0.0:50053");
        assert_eq!(config.server.catalog, "strake");
    }

    #[test]
    fn test_env_var_overrides() {
        // Set env vars
        std::env::set_var("STRAKE_SERVER__LISTEN_ADDR", "1.2.3.4:9999");
        std::env::set_var("STRAKE_SERVER__CATALOG", "test_catalog");
        std::env::set_var("STRAKE_SERVER__GLOBAL_CONNECTION_BUDGET", "500");
        std::env::set_var("STRAKE_AUTH__ENABLED", "true");
        std::env::set_var("STRAKE_AUTH__API_KEY", "env-key");
        std::env::set_var("STRAKE_RETRY__MAX_ATTEMPTS", "10");

        // Load config (non-existent file, should use defaults + overrides)
        let config = AppConfig::from_file("non_existent_config.yaml").unwrap();

        assert_eq!(config.server.listen_addr, "1.2.3.4:9999");
        assert_eq!(config.server.catalog, "test_catalog");
        assert_eq!(config.server.global_connection_budget, 500);
        assert!(config.server.auth.enabled);
        assert_eq!(config.server.auth.api_key, "env-key");
        assert_eq!(config.retry.max_attempts, 10);

        // Cleanup
        std::env::remove_var("STRAKE_SERVER__LISTEN_ADDR");
        std::env::remove_var("STRAKE_SERVER__CATALOG");
        std::env::remove_var("STRAKE_SERVER__GLOBAL_CONNECTION_BUDGET");
        std::env::remove_var("STRAKE_AUTH__ENABLED");
        std::env::remove_var("STRAKE_AUTH__API_KEY");
        std::env::remove_var("STRAKE_RETRY__MAX_ATTEMPTS");
    }
}
