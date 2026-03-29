//! # Integration Configuration
//!
//! Settings for external system integrations like MCP sidecars and telemetry endpoints.
//!
//! ## Overview
//!
//! This module defines [`McpConfig`] for managing Model Context Protocol sidecars
//! and [`TelemetryConfig`] for OpenTelemetry OTLP endpoint configuration.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::config::integrations::McpConfig;
//! let config = McpConfig::default();
//! assert!(!config.enabled);
//! ```
//!
//! ## Performance Characteristics
//!
//! Integration settings are used primarily at startup to initialize exporters
//! and spawn sidecar processes.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! Handled during overall configuration loading in [`crate::config::AppConfig`].
//!
use super::StrakeEnvironment;
use crate::config::defaults::*;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Configuration for the Model Context Protocol (MCP) sidecar.
#[derive(Debug, Deserialize, Serialize, Clone, Validate)]
#[non_exhaustive]
pub struct McpConfig {
    /// Deployment environment for the sidecar.
    #[serde(default)]
    pub environment: StrakeEnvironment,
    /// Whether the MCP sidecar is enabled.
    #[serde(default = "Default::default")]
    pub enabled: bool,
    /// Port for the sidecar to listen on.
    #[serde(default = "default_mcp_port")]
    #[validate(range(min = 1))]
    pub port: u16,
    /// Maximum number of retries for sidecar operations.
    #[serde(default = "default_max_attempts")]
    pub max_retries: u32,
    /// Delay in milliseconds between retries.
    #[serde(default = "default_base_delay_ms")]
    pub retry_delay_ms: u64,
    /// Expected startup time for the sidecar in milliseconds.
    #[serde(default = "default_mcp_sidecar_startup_delay_ms")]
    pub startup_delay_ms: u64,
    /// Timeout for sidecar shutdown in milliseconds.
    #[serde(default = "default_mcp_sidecar_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
    /// Path to the Python binary for running the sidecar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_bin: Option<String>,
    /// Interval between sidecar health checks.
    #[serde(default = "default_mcp_health_check_interval_ms")]
    #[validate(range(min = 1000, max = 300000))] // 1s to 5min
    pub health_check_interval_ms: u64,
    /// Maximum number of rows the sidecar should output in a single response.
    #[serde(default = "default_mcp_max_output_rows")]
    pub max_output_rows: usize,
    /// Cooldown period in seconds after a sidecar failure.
    #[serde(default = "default_mcp_cooldown_secs")]
    pub cooldown_secs: u64,
    /// URL for sidecar health checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(custom(function = "crate::config::validate_url"))]
    pub health_check_url: Option<String>,
    /// Whether to use Firecracker microVMs for sidecar isolation.
    #[serde(default = "default_use_firecracker")]
    pub use_firecracker: bool,
}

impl McpConfig {
    /// Returns the parsed health check URL, or `None` if not configured or invalid.
    pub fn parsed_health_check_url(&self) -> Option<url::Url> {
        self.health_check_url
            .as_ref()
            .and_then(|s| url::Url::parse(s).ok())
    }
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            environment: StrakeEnvironment::default(),
            enabled: false,
            port: default_mcp_port(),
            max_retries: default_max_attempts(),
            retry_delay_ms: default_base_delay_ms(),
            startup_delay_ms: default_mcp_sidecar_startup_delay_ms(),
            shutdown_timeout_ms: default_mcp_sidecar_shutdown_timeout_ms(),
            python_bin: None,
            health_check_interval_ms: default_mcp_health_check_interval_ms(),
            max_output_rows: default_mcp_max_output_rows(),
            cooldown_secs: default_mcp_cooldown_secs(),
            health_check_url: None,
            use_firecracker: default_use_firecracker(),
        }
    }
}

/// Configuration for OTLP telemetry.
#[derive(Debug, Deserialize, Serialize, Clone, Validate)]
#[non_exhaustive]
pub struct TelemetryConfig {
    /// Whether telemetry is enabled.
    #[serde(default = "default_telemetry_enabled")]
    pub enabled: bool,

    /// OTLP collector endpoint URL (e.g., http://localhost:4317).
    #[serde(default = "default_otlp_endpoint")]
    #[validate(custom(function = "crate::config::validate_url"))]
    pub endpoint: String,

    /// Service name for identifying this instance in traces and metrics.
    #[serde(default = "default_server_name")]
    pub service_name: String,
}

impl TelemetryConfig {
    /// Returns the parsed telemetry endpoint URL, or an error if invalid.
    pub fn parsed_endpoint_url(&self) -> Result<url::Url, url::ParseError> {
        url::Url::parse(&self.endpoint)
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: default_telemetry_enabled(),
            endpoint: default_otlp_endpoint(),
            service_name: default_server_name(),
        }
    }
}
