//! # Data Models
//!
//! Core data structures for Strake configurations and source definitions.
//!
//! This module defines the [`crate::models::SourceConfig`] used to register external data sources,
//! and the [`crate::models::SourcesConfig`] which aggregates multiple sources.
//!
//! ## Overview
//!
//! Strake uses a declarative model for data source registration. [`SourcesConfig`]
//! is the root container for one or more [`SourceConfig`] entries, each defining
//! connection parameters, table mappings, and caching policies.
//!
//! ## Usage
//!
//! ```rust
//! use strake_common::models::{SourceConfig, SourceType, SourceName};
//! let mut config = SourceConfig::default();
//! config.name = SourceName::from("my_db");
//! config.source_type = SourceType::Postgres;
//! config.url = Some("postgres://localhost:5432/db".to_string());
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Serialization**: Configs use `SecretString` for sensitive fields to ensure
//!   they are redacted during serialization, preventing accidental leakage in logs.
//! - **Equality**: Configs derive `PartialEq` for efficient structural comparison.
//!
//! ## Safety
//!
//! All models are pure-Rust data structures. `SecretString` provides a safety
//! boundary for authentication credentials by clearing memory on drop and
//! custom serialization.
//!
//! ## Errors
//!
//! Models implement the `validator::Validate` trait. Validation errors are
//! typically handled during configuration loading in [`crate::config::AppConfig`].
//!

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use validator::Validate;

/// Type of data source.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SourceType {
    /// Postgres database.
    #[default]
    Postgres,
    /// MySQL database.
    Mysql,
    /// SQLite database.
    Sqlite,
    /// DuckDB database.
    Duckdb,
    /// ClickHouse database.
    Clickhouse,
    /// Apache Iceberg table.
    Iceberg,
    /// Parquet files.
    Parquet,
    /// CSV files.
    Csv,
    /// JSON files.
    Json,
    /// Arrow Flight SQL source.
    FlightSql,
    /// REST API source.
    Rest,
    /// gRPC service source.
    Grpc,
    /// Custom or unknown source type.
    Other(String),
}

impl SourceType {
    /// Returns the canonical string representation of the source type.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Sqlite => "sqlite",
            Self::Duckdb => "duckdb",
            Self::Clickhouse => "clickhouse",
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Iceberg => "iceberg",
            Self::FlightSql => "flight_sql",
            Self::Rest => "rest",
            Self::Grpc => "grpc",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl std::str::FromStr for SourceType {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "postgres" => Self::Postgres,
            "mysql" => Self::Mysql,
            "sqlite" => Self::Sqlite,
            "duckdb" => Self::Duckdb,
            "clickhouse" => Self::Clickhouse,
            "iceberg" => Self::Iceberg,
            "parquet" => Self::Parquet,
            "csv" => Self::Csv,
            "json" => Self::Json,
            "flight_sql" => Self::FlightSql,
            "rest" => Self::Rest,
            "grpc" => Self::Grpc,
            _ => Self::Other(s.to_string()),
        })
    }
}

impl serde::Serialize for SourceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for SourceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.to_lowercase().as_str() {
            "postgres" => Self::Postgres,
            "mysql" => Self::Mysql,
            "sqlite" => Self::Sqlite,
            "duckdb" => Self::Duckdb,
            "clickhouse" => Self::Clickhouse,
            "iceberg" => Self::Iceberg,
            "parquet" => Self::Parquet,
            "csv" => Self::Csv,
            "json" => Self::Json,
            "flight_sql" => Self::FlightSql,
            "rest" => Self::Rest,
            "grpc" => Self::Grpc,
            _ => Self::Other(s),
        })
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres => write!(f, "postgres"),
            Self::Mysql => write!(f, "mysql"),
            Self::Sqlite => write!(f, "sqlite"),
            Self::Duckdb => write!(f, "duckdb"),
            Self::Clickhouse => write!(f, "clickhouse"),
            Self::Iceberg => write!(f, "iceberg"),
            Self::Parquet => write!(f, "parquet"),
            Self::Csv => write!(f, "csv"),
            Self::Json => write!(f, "json"),
            Self::FlightSql => write!(f, "flight_sql"),
            Self::Rest => write!(f, "rest"),
            Self::Grpc => write!(f, "grpc"),
            Self::Other(s) => write!(f, "{}", s),
        }
    }
}

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

fn default_predicate_cache() -> bool {
    false
}

/// A collection of data source configurations.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct SourcesConfig {
    /// Optional domain name for grouped data sources.
    pub domain: Option<DomainName>,
    /// List of data source configurations in this collection.
    pub sources: Vec<SourceConfig>,
}

/// Configuration for a single data source.
#[derive(Debug, Serialize, Deserialize, Clone, Validate, Default)]
#[non_exhaustive]
pub struct SourceConfig {
    /// Unique name for the data source.
    #[validate(nested)]
    pub name: SourceName,

    /// Type of the data source (e.g., postgres, mysql, parquet).
    #[serde(rename = "type")]
    pub source_type: SourceType,

    /// Connection URL or file path.
    pub url: Option<String>,
    /// Optional username for authentication.
    pub username: Option<String>,

    /// Maximum number of concurrent queries allowed for this source.
    #[serde(default)]
    pub max_concurrent_queries: Option<usize>,

    /// Optional password for authentication (redacted in serialization).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_secret",
        deserialize_with = "deserialize_secret"
    )]
    pub password: Option<SecretString>,

    /// Default row limit for queries.
    pub default_limit: Option<usize>,

    /// Query cache configuration.
    #[serde(default)]
    #[validate(nested)]
    pub cache: Option<QueryCacheConfig>,

    /// List of tables to register from this source.
    #[serde(default)]
    #[validate(nested)]
    pub tables: Vec<TableConfig>,

    /// Whether to enable predicate caching for this source.
    #[serde(default = "default_predicate_cache")]
    pub predicate_cache: bool,

    /// Additional arbitrary configuration.
    #[serde(flatten)]
    pub config: serde_json::Value,
}

/// Configuration for a single table within a data source.
#[derive(Debug, Serialize, Deserialize, Clone, Validate, Default, PartialEq)]
#[non_exhaustive]
pub struct TableConfig {
    /// Name of the table.
    #[validate(length(min = 1))]
    pub name: String,

    /// Schema where the table resides (defaults to "public" for DBs).
    #[serde(default = "default_schema")]
    pub schema: String,

    /// Optional column used for partitioning.
    pub partition_column: Option<String>,
    /// Optional table description.
    pub description: Option<String>,
    /// Optional physical path (for file-based sources).
    pub path: Option<String>,
    /// Optional snapshot ID (for versioned sources like Iceberg).
    pub snapshot_id: Option<i64>,

    /// List of column definitions.
    #[serde(default, alias = "columns")]
    #[validate(nested)]
    pub column_definitions: Vec<ColumnConfig>,
}

/// Configuration for a single column.
#[derive(Debug, Serialize, Deserialize, Clone, Validate, Default, PartialEq)]
#[non_exhaustive]
pub struct ColumnConfig {
    /// Name of the column.
    pub name: String,
    /// Data type of the column.
    #[serde(rename = "type")]
    pub data_type: String,
    /// Optional maximum length (for strings).
    pub length: Option<u32>,
    /// Optional precision (for decimals).
    pub precision: Option<u8>,
    /// Optional scale (for decimals).
    pub scale: Option<u8>,
    /// Whether the column is part of the primary key.
    #[serde(default)]
    pub primary_key: bool,
    /// Whether the column must have unique values.
    #[serde(default)]
    pub unique: bool,
    /// Whether the column is non-nullable.
    #[serde(default)]
    pub not_null: bool,
    /// Optional column description.
    pub description: Option<String>,
}

impl PartialEq for SourcesConfig {
    fn eq(&self, other: &Self) -> bool {
        self.domain == other.domain && self.sources == other.sources
    }
}

impl PartialEq for SourceConfig {
    fn eq(&self, other: &Self) -> bool {
        use secrecy::ExposeSecret;

        self.name == other.name
            && self.source_type == other.source_type
            && self.url == other.url
            && self.username == other.username
            && self.max_concurrent_queries == other.max_concurrent_queries
            && self.default_limit == other.default_limit
            && self.cache == other.cache
            && self.tables == other.tables
            && self.predicate_cache == other.predicate_cache
            && self.config == other.config
            && match (&self.password, &other.password) {
                (Some(p1), Some(p2)) => p1.expose_secret() == p2.expose_secret(),
                (None, None) => true,
                _ => false,
            }
    }
}

/// Configuration for the query result cache.
#[derive(Debug, Serialize, Deserialize, Clone, Validate, PartialEq)]
#[non_exhaustive]
pub struct QueryCacheConfig {
    /// Whether the query cache is enabled.
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    /// Directory where cache files are stored.
    #[serde(default = "default_cache_directory")]
    pub directory: String,
    /// Maximum size of the cache in megabytes.
    #[serde(default = "default_cache_max_size_mb")]
    pub max_size_mb: u64,
    /// Time-to-live for cache entries in seconds.
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
/// A collection of data quality contracts.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct ContractsConfig {
    /// List of contracts for various tables.
    pub contracts: Vec<Contract>,
}

/// A data quality contract for a table.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct Contract {
    /// Name of the table the contract applies to.
    pub table: String,
    /// Whether to strictly enforce the contract (fail on any violation).
    #[serde(default)]
    pub strict: bool,
    /// List of column-level contracts.
    pub columns: Vec<ContractColumn>,
}

/// Contract specification for a single column.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct ContractColumn {
    /// Name of the column.
    pub name: String,
    /// Expected data type of the column.
    #[serde(rename = "type")]
    pub data_type: String,
    /// Whether the column allows NULL values.
    #[serde(default)]
    pub nullable: Option<bool>,
    /// List of constraints applied to the column.
    #[serde(default)]
    pub constraints: Vec<Constraint>,
}

/// A single data quality constraint.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct Constraint {
    /// Type of constraint (e.g., "gt", "lt", "regex").
    #[serde(rename = "type")]
    pub constraint_type: String,
    /// Value used for the constraint comparison.
    pub value: serde_json::Value,
}

/// A request to validate a configuration against a set of contracts.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct ValidationRequest {
    /// YAML string containing the data source configurations.
    pub sources_yaml: String,
    /// Optional YAML string containing the data quality contracts.
    pub contracts_yaml: Option<String>,
}

/// Response from a configuration validation request.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct ValidationResponse {
    /// Whether the configuration is valid.
    pub valid: bool,
    /// List of validation error messages.
    pub errors: Vec<String>,
}

/// Information about a discovered table.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct TableDiscovery {
    /// Name of the table.
    pub name: String,
    /// Schema where the table resides.
    pub schema: String,
}

/// A request to execute a SQL query.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct QueryRequest {
    /// The SQL query string to execute.
    pub sql: String,
}

/// Response from a SQL query execution.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[non_exhaustive]
pub struct QueryResponse {
    /// Overall status of the query (e.g., "success", "error").
    pub status: String,
    /// The resulting data as optional JSON.
    pub data: Option<serde_json::Value>,
    /// Optional error or warning message.
    pub message: Option<String>,
}
/// Newtype for a domain name.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord, Validate,
)]
#[non_exhaustive]
#[serde(transparent)]
pub struct DomainName {
    /// The inner domain name string.
    #[validate(length(min = 1))]
    pub name: String,
}

impl std::str::FromStr for DomainName {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self {
            name: s.to_string(),
        })
    }
}

impl From<String> for DomainName {
    fn from(name: String) -> Self {
        Self { name }
    }
}

impl From<&str> for DomainName {
    fn from(s: &str) -> Self {
        Self {
            name: s.to_string(),
        }
    }
}

impl std::fmt::Display for DomainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl AsRef<str> for DomainName {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

/// Newtype for an actor name.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord, Validate,
)]
#[non_exhaustive]
#[serde(transparent)]
pub struct ActorName {
    /// The inner actor name string.
    #[validate(length(min = 1))]
    pub name: String,
}

impl std::str::FromStr for ActorName {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self {
            name: s.to_string(),
        })
    }
}

impl From<String> for ActorName {
    fn from(name: String) -> Self {
        Self { name }
    }
}

impl From<&str> for ActorName {
    fn from(s: &str) -> Self {
        Self {
            name: s.to_string(),
        }
    }
}

impl std::fmt::Display for ActorName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl AsRef<str> for ActorName {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

/// Newtype for a source name.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default, PartialOrd, Ord, Validate,
)]
#[non_exhaustive]
#[serde(transparent)]
pub struct SourceName {
    /// The inner source name string.
    #[validate(length(min = 1))]
    pub name: String,
}

impl std::str::FromStr for SourceName {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self {
            name: s.to_string(),
        })
    }
}

impl From<String> for SourceName {
    fn from(name: String) -> Self {
        Self { name }
    }
}

impl From<&str> for SourceName {
    fn from(s: &str) -> Self {
        Self {
            name: s.to_string(),
        }
    }
}

impl std::fmt::Display for SourceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl AsRef<str> for SourceName {
    fn as_ref(&self) -> &str {
        &self.name
    }
}
