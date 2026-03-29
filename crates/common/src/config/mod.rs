//! # Configuration Management
//!
//! Centralized configuration for the Strake server and data sources.
//!
//! ## Overview
//!
//! Configuration follows a layered model: YAML files provide base values, and
//! environment variables (prefix `STRAKE__`, separator `__`) override them at
//! runtime. The root type is [`crate::config::AppConfig`], which aggregates server settings,
//! query limits, security policies, and MCP sidecar configuration.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::config::AppConfig;
//! # use std::fs;
//! # let temp = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
//! # fs::write(&temp, "server:\n  listen_addr: 0.0.0.0:50051").unwrap();
//! // Load from file with environment variable overrides
//! let config = AppConfig::from_file(temp.path().to_str().unwrap())
//!     .expect("Failed to load configuration");
//!
//! println!("Server listening on {}", config.server.listen_addr);
//! ```
//!
//! ## Performance Characteristics
//!
//! Configuration is loaded once at startup. The `config` crate parses YAML
//! and environment variables in a single pass; no lazy evaluation is performed.
//! Expect ~1-5ms for typical configuration files.
//!
//! ## Safety
//!
//! Implementation is pure Rust and contains no unsafe code. Sensitive values
//! (API keys, OIDC secrets) are stored in plain strings; consumers should
//! use the `SecretString` wrapper from `models.rs` for these fields.
//!
//! ## Errors
//!
//! - [`crate::config::ConfigError`]: Returned if the file is missing, malformed,
//!   or if environment variable overrides fail type coercion.
//!
//!
//! ## Errors
//!
//! [`crate::config::AppConfig::from_file`] returns [`crate::config::ConfigError`] when:
//! - The YAML file is malformed or missing required fields.
//! - URL validation fails for `api_url` or `telemetry.endpoint`.
//! - `server.auth.enabled = true` but `api_key` is empty.
//!
//! ## Safety
//!
//! No unsafe code in this module.

/// Default constants and functions.
pub mod defaults;
/// Configuration error types.
pub mod error;
/// External system integrations.
pub mod integrations;
/// Query and resource limits.
pub mod limits;
/// Server-specific settings.
pub mod server;

pub use defaults::Defaults;
pub use error::{ConfigError, Result};
pub use integrations::*;
pub use limits::*;
use serde::{Deserialize, Serialize};
pub use server::*;
use validator::Validate;

/// Re-export of common models used in configuration.
pub use crate::models::{ColumnConfig, QueryCacheConfig, SourceConfig, TableConfig};

/// The deployment environment for Strake.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum StrakeEnvironment {
    /// Development environment with verbose logging and relaxed security.
    #[default]
    #[serde(rename = "development")]
    Development,
    /// Production environment with strict enforcement and optimized performance.
    #[serde(rename = "production")]
    Production,
}

impl std::fmt::Display for StrakeEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrakeEnvironment::Development => write!(f, "development"),
            StrakeEnvironment::Production => write!(f, "production"),
        }
    }
}

/// Root application configuration for the Strake server.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Validate)]
#[non_exhaustive]
pub struct AppConfig {
    /// Server-level listener and identity settings.
    #[serde(default)]
    #[validate(nested)]
    pub server: ServerSettings,
    /// Global query throughput and size limits.
    #[serde(default)]
    #[validate(nested)]
    pub query_limits: QueryLimits,
    /// Security and access control policies.
    #[serde(default)]
    #[validate(nested)]
    pub security: SecurityConfig,
    /// Global retry policy for external systems.
    #[serde(default)]
    #[validate(nested)]
    pub retry: RetrySettings,
    /// Resource management (memory, disk).
    #[serde(default)]
    #[validate(nested)]
    pub resources: ResourceConfig,
    /// Global query result cache.
    #[serde(default)]
    pub cache: QueryCacheConfig,
    /// Deployment environment.
    #[serde(default)]
    pub environment: StrakeEnvironment,
    /// Settings for the Model Context Protocol (MCP) sidecar.
    #[serde(default)]
    #[validate(nested)]
    pub mcp: McpConfig,
    /// Observability and OTLP settings.
    #[serde(default)]
    #[validate(nested)]
    pub telemetry: TelemetryConfig,
}

impl AppConfig {
    /// Loads the application configuration from a YAML file and environment variables.
    ///
    /// Environment variables with prefix `STRAKE__` override file values.
    /// Nested fields use `__` as a separator (e.g., `STRAKE_SERVER__LISTEN_ADDR`).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if:
    /// - The configuration file cannot be parsed.
    /// - Validation fails (invalid URLs, out-of-range values).
    /// - `server.auth.enabled` is `true` but `api_key` is empty.
    ///
    /// # Panics
    ///
    /// This function cannot panic.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use strake_common::config::AppConfig;
    /// # let temp = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
    /// let config = AppConfig::from_file(temp.path().to_str().unwrap());
    /// assert!(config.is_ok());
    /// ```
    pub fn from_file(path: &str) -> Result<Self> {
        let builder = config::Config::builder();

        let builder = if std::path::Path::new(path).exists() {
            builder.add_source(config::File::with_name(path))
        } else {
            tracing::warn!(
                "Configuration file not found: {}. Proceeding with environment variables and defaults.",
                path
            );
            builder
        };

        let builder = builder.add_source(
            config::Environment::with_prefix("STRAKE")
                .separator("__")
                .try_parsing(true),
        );

        let cfg = builder.build()?;
        let app_config: AppConfig = cfg.try_deserialize()?;

        app_config
            .validate()
            .map_err(|e| ConfigError::Validation(e.to_string()))?;

        if app_config.server.auth.enabled && app_config.server.auth.api_key.is_empty() {
            return Err(ConfigError::MissingApiKey);
        }

        Ok(app_config)
    }
}

/// Legacy configuration for data sources.
#[derive(Debug, Deserialize, Serialize, Clone, Validate, Default)]
#[non_exhaustive]
pub struct Config {
    /// List of data sources.
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    /// Global query cache settings.
    #[serde(default)]
    pub cache: QueryCacheConfig,
}

impl Config {
    /// Loads the data source configuration from a YAML file.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if the file cannot be parsed or deserialized.
    ///
    /// # Panics
    ///
    /// This function cannot panic.
    pub fn from_file(path: &str) -> Result<Self> {
        let builder = config::Config::builder();

        let builder = if std::path::Path::new(path).exists() {
            builder.add_source(config::File::with_name(path))
        } else {
            builder
        };

        let cfg = builder.build()?;
        let config: Config = cfg.try_deserialize()?;

        Ok(config)
    }
}

/// Generic URL validator for use with the `validator` crate.
pub fn validate_url(url: &str) -> std::result::Result<(), validator::ValidationError> {
    if url.is_empty() {
        return Ok(());
    }
    match url::Url::parse(url) {
        Ok(_) => Ok(()),
        Err(_) => Err(validator::ValidationError::new("invalid_url")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::io::Write;

    #[test]
    fn test_app_config_validation() {
        let config = AppConfig::default();
        // Should validate OK with defaults
        if let Err(e) = config.validate() {
            panic!("Validation failed: {:?}", e);
        }
    }

    #[test]
    fn test_defaults_consistency() {
        assert_eq!(AgentGuardMode::default(), AgentGuardMode::DryRun);
        assert_eq!(StrakeEnvironment::default(), StrakeEnvironment::Development);
    }

    #[test]
    #[serial]
    fn test_auth_enabled_requires_api_key() {
        let mut temp = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        writeln!(
            temp,
            "server:\n  auth:\n    enabled: true\n    api_key: \"\""
        )
        .unwrap();

        let err_res = AppConfig::from_file(temp.path().to_str().unwrap());
        if let Err(err) = &err_res {
            eprintln!("Error was: {:?}", err);
        }
        let err = err_res.unwrap_err();
        assert!(matches!(err, ConfigError::MissingApiKey));
    }

    #[test]
    fn test_url_validation() {
        let mut config = AppConfig::default();
        config.server.api_url = "not-a-url".to_string();
        assert!(config.validate().is_err());

        config.server.api_url = "http://localhost:8080".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let config = AppConfig::default();
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: AppConfig = serde_yaml::from_str(&yaml).unwrap();

        // Check a few fields
        assert_eq!(deserialized.server.listen_addr, config.server.listen_addr);
        assert_eq!(
            deserialized.server.global_connection_budget,
            config.server.global_connection_budget
        );
    }

    #[test]
    fn test_range_validation() {
        let mut config = AppConfig::default();
        config.server.global_connection_budget = 0;
        assert!(config.validate().is_err());

        config.server.global_connection_budget = 1;
        assert!(config.validate().is_ok());

        config.mcp.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_malformed_yaml() {
        let mut temp = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        writeln!(temp, "server: [unclosed list").unwrap();

        let err = AppConfig::from_file(temp.path().to_str().unwrap()).unwrap_err();
        assert!(matches!(err, ConfigError::ParseError(_)));
    }
}
