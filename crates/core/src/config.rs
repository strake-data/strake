use std::fs;

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

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
    Some(1000)
}

fn default_max_attempts() -> u32 {
    5
}
fn default_base_delay_ms() -> u64 {
    1000
}
fn default_max_delay_ms() -> u64 {
    60000
}

#[derive(Debug, Deserialize, Default)]
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
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct ResourceConfig {
    pub memory_limit_mb: Option<usize>,
    pub spill_dir: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
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
    "0.0.0.0:50051".to_string()
}

fn default_health_addr() -> String {
    "0.0.0.0:8080".to_string()
}

#[allow(dead_code)]
fn default_pool_size() -> usize {
    10
}

fn default_global_budget() -> usize {
    100
}

fn default_catalog() -> String {
    "strake".to_string()
}

fn default_api_url() -> String {
    "http://localhost:8080/api/v1".to_string()
}

fn default_server_name() -> String {
    "Strake Server".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct TlsSettings {
    pub enabled: bool,
    #[serde(default)]
    pub cert: String,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct OidcConfig {
    pub issuer_url: String,
    pub audience: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
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
    "dev-key".to_string()
}

fn default_cache_ttl() -> u64 {
    300
}

fn default_cache_capacity() -> u64 {
    10000
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
    false
}

fn default_cache_directory() -> String {
    "/tmp/strake-cache".to_string()
}

fn default_cache_max_size_mb() -> u64 {
    10240 // 10GB
}

fn default_cache_ttl_seconds() -> u64 {
    3600 // 1 hour
}

impl AppConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).context(format!("Failed to read config file at {}", path))?;
        let mut config: AppConfig = serde_yaml::from_str(&content)
            .context(format!("Failed to parse app config file at {}", path))?;

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
}
