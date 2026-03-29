//! # Server Configuration
//!
//! Server-specific settings for listener addresses, identity, and security policies.
//!
//! ## Overview
//!
//! This module defines [`ServerSettings`], which configures the primary gRPC
//! listener, health check endpoint, and global identity (catalog name). It also
//! manages authentication (OIDC) and audit logging policies.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::config::server::ServerSettings;
//! let settings = ServerSettings::default();
//! assert_eq!(settings.listen_addr, "0.0.0.0:50051");
//! ```
//!
//! ## Performance Characteristics
//!
//! Configuration is static and loaded once. Environment variable lookups
//! are fast and performed during the initial configuration merge.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! Handled during overall configuration loading in [`crate::config::AppConfig`].
//!
use crate::config::defaults::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;

/// Core server settings including listener addresses and identity.
#[derive(Debug, Deserialize, Serialize, Clone, Validate)]
#[non_exhaustive]
pub struct ServerSettings {
    /// Address to listen on for the main gRPC API.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Address to listen on for health checks and internal metrics.
    #[serde(default = "default_health_addr")]
    pub health_addr: String,

    /// Default catalog name for DataFusion.
    #[serde(default = "default_catalog")]
    pub catalog: String,

    /// Base URL for the Strake API.
    #[serde(default = "default_api_url")]
    #[validate(custom(function = "crate::config::validate_url"))]
    pub api_url: String,

    /// Maximum number of concurrent connections across all sources.
    #[serde(default = "default_global_budget")]
    #[validate(range(min = 1))]
    pub global_connection_budget: usize,

    /// Internal database URL for metadata storage.
    #[serde(default)]
    pub database_url: String,

    /// TLS configuration for the server listeners.
    #[serde(default)]
    pub tls: TlsSettings,

    /// Authentication settings and API keys.
    #[serde(default)]
    pub auth: AuthSettings,

    /// Optional OpenID Connect (OIDC) configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc: Option<OidcConfig>,

    /// Arbitrary configuration key-values for the DataFusion SessionContext.
    #[serde(default)]
    pub datafusion_config: HashMap<String, String>,

    /// Human-readable name for this Strake instance.
    #[serde(default = "default_server_name")]
    pub name: String,

    /// Audit logging settings.
    #[serde(default)]
    pub audit: AuditSettings,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            health_addr: default_health_addr(),
            catalog: default_catalog(),
            api_url: default_api_url(),
            global_connection_budget: default_global_budget(),
            database_url: String::new(),
            tls: TlsSettings::default(),
            auth: AuthSettings::default(),
            oidc: None,
            datafusion_config: HashMap::new(),
            name: default_server_name(),
            audit: AuditSettings::default(),
        }
    }
}

impl ServerSettings {
    /// Returns the parsed API URL, or an error if invalid.
    pub fn parsed_api_url(&self) -> Result<url::Url, url::ParseError> {
        url::Url::parse(&self.api_url)
    }
}

/// TLS configuration for server listeners.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Validate)]
#[non_exhaustive]
pub struct TlsSettings {
    /// Whether TLS is enabled.
    #[serde(default = "Default::default")]
    pub enabled: bool,
    /// Path to the TLS certificate file.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cert: String,
    /// Path to the TLS private key file.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
}

/// Configuration for OpenID Connect (OIDC) authentication.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Validate)]
#[non_exhaustive]
pub struct OidcConfig {
    /// The issuer URL for the OIDC provider.
    pub issuer_url: String,
    /// The allowed audience for OIDC tokens.
    #[serde(default)]
    pub audience: Vec<String>,
}

impl OidcConfig {
    /// Returns the parsed issuer URL, or an error if invalid.
    pub fn parsed_issuer_url(&self) -> Result<url::Url, url::ParseError> {
        url::Url::parse(&self.issuer_url)
    }
}

/// Settings for API key and local cache authentication.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Validate)]
#[non_exhaustive]
pub struct AuthSettings {
    /// Whether authentication is enabled.
    #[serde(default = "Default::default")]
    pub enabled: bool,
    /// The API key required for access.
    #[serde(default = "default_api_key")]
    pub api_key: String,
    /// Time-to-live for cached authentication results in seconds.
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
    /// Maximum number of entries in the authentication cache.
    #[serde(default = "default_cache_capacity")]
    pub cache_max_capacity: u64,
}

/// Failure modes for audit logging.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditFailureMode {
    /// Only alert/log when audit logging fails.
    #[default]
    #[serde(rename = "alert")]
    Alert,
    /// Shut down the server if audit logging fails.
    #[serde(rename = "shutdown")]
    Shutdown,
}

/// Settings for audit logging of sensitive operations.
#[derive(Debug, Deserialize, Serialize, Clone, Validate, Default)]
#[non_exhaustive]
pub struct AuditSettings {
    /// Whether audit logging is enabled.
    #[serde(default = "Default::default")]
    pub enabled: bool,
    /// Behavior when an audit log entry cannot be written.
    #[serde(default)]
    pub failure_mode: AuditFailureMode,
}
