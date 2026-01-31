use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use validator::Validate;

// Custom Serde logic for SecretString
fn serialize_secret<S>(secret: &Option<SecretString>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match secret {
        Some(_) => serializer.serialize_str("[REDACTED]"),
        None => serializer.serialize_none(),
    }
}

fn deserialize_secret<'de, D>(deserializer: D) -> Result<Option<SecretString>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    Ok(s.map(SecretString::from))
}

// --- Config Constants (Defaults) ---

fn default_schema() -> String {
    "public".to_string()
}

fn default_cache_enabled() -> bool {
    false
}

fn default_cache_directory() -> String {
    "/tmp/strake-cache".to_string()
}

fn default_cache_max_size_mb() -> u64 {
    10240
}

fn default_cache_ttl_seconds() -> u64 {
    3600
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourcesConfig {
    pub domain: Option<String>,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct SourceConfig {
    #[validate(length(min = 1))]
    pub name: String,

    #[serde(rename = "type")]
    #[validate(length(min = 1))]
    pub source_type: String, // e.g., postgres, mysql, etc.

    pub url: Option<String>,
    pub username: Option<String>,

    #[serde(default)]
    pub max_concurrent_queries: Option<usize>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_secret",
        deserialize_with = "deserialize_secret"
    )]
    pub password: Option<SecretString>,

    pub default_limit: Option<usize>,

    #[serde(default)]
    #[validate(nested)]
    pub cache: Option<QueryCacheConfig>,

    #[serde(default)]
    #[validate(nested)]
    pub tables: Vec<TableConfig>,

    // Flatten other loose config
    #[serde(flatten)]
    pub config: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct TableConfig {
    #[validate(length(min = 1))]
    pub name: String,

    #[serde(default = "default_schema")]
    pub schema: String,

    pub partition_column: Option<String>,

    #[serde(default)]
    #[validate(nested)]
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub length: Option<u32>,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub not_null: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Validate)]
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

// Data Contracts
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractsConfig {
    pub contracts: Vec<Contract>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub table: String,
    #[serde(default)]
    pub strict: bool,
    pub columns: Vec<ContractColumn>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub nullable: Option<bool>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Constraint {
    #[serde(rename = "type")]
    pub constraint_type: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidationRequest {
    pub sources_yaml: String,
    pub contracts_yaml: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidationResponse {
    pub valid: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableDiscovery {
    pub name: String,
    pub schema: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryResponse {
    pub status: String,
    pub data: Option<serde_json::Value>,
    pub message: Option<String>,
}
