//! # Configuration Defaults
//!
//! Centralized default values and fallback functions for all Strake configuration fields.
//!
//! ## Overview
//!
//! This module provides the [`Defaults`] struct containing constant field values
//! and a suite of `default_*` functions used by `serde(default)` across the
//! configuration modules.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::config::defaults::Defaults;
//! assert_eq!(Defaults::PORT, 50051);
//! ```
//!
//! ## Performance Characteristics
//!
//! Constant and function-based defaults have zero runtime overhead beyond
//! the initial configuration parsing.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! This module does not return errors.
//!

/// Centralized default values for the Strake configuration.
pub struct Defaults;

impl Defaults {
    // --- Network ---
    /// Default port for the gRPC server.
    pub const PORT: u16 = 50051;
    /// Default port for the health check endpoint.
    pub const HEALTH_PORT: u16 = 8080;
    /// Default port for the gRPC API endpoint.
    pub const API_PORT: u16 = 8080;
    /// Default address to listen for gRPC requests.
    pub const LISTEN_ADDR: &'static str = "0.0.0.0:50051";
    /// Default address to listen for health checks.
    pub const HEALTH_ADDR: &'static str = "0.0.0.0:8080";
    /// Default URL for the Strake API.
    pub const API_URL: &'static str = "http://localhost:8080/api/v1";

    // --- Core ---
    /// Default catalog name for DataFusion.
    pub const CATALOG: &'static str = "strake";
    /// Default server instance name.
    pub const SERVER_NAME: &'static str = "Strake Server";
    /// Default global budget for concurrent connections.
    pub const GLOBAL_CONNECTION_BUDGET: usize = 100;

    // --- Limits ---
    /// Default row limit for queries.
    pub const LIMIT: usize = 1000;
    /// Default maximum number of retry attempts.
    pub const MAX_ATTEMPTS: u32 = 5;
    /// Default base delay for exponential backoff in milliseconds.
    pub const BASE_DELAY_MS: u64 = 1000;
    /// Default maximum delay for exponential backoff in milliseconds.
    pub const MAX_DELAY_MS: u64 = 60000;

    // --- Auth ---
    /// Default API key (empty).
    pub const API_KEY: &'static str = "";
    /// Default time-to-live for cached authentication.
    pub const CACHE_TTL_SECS: u64 = 300;
    /// Default capacity for the authentication cache.
    pub const CACHE_MAX_CAPACITY: u64 = 10000;

    // --- Telemetry ---
    /// Whether telemetry is enabled by default.
    pub const TELEMETRY_ENABLED: bool = false;
    /// Default OTLP collector endpoint.
    pub const OTLP_ENDPOINT: &'static str = "http://localhost:4317";

    // --- MCP ---
    /// Default port for the MCP sidecar.
    pub const MCP_PORT: u16 = 8001;
    /// Whether to use Firecracker by default.
    pub const USE_FIRECRACKER: bool = false;
    /// Default max output rows for MCP.
    pub const MCP_MAX_OUTPUT_ROWS: usize = 1000;
}

// Serde default functions
pub(crate) fn default_listen_addr() -> String {
    Defaults::LISTEN_ADDR.to_string()
}
pub(crate) fn default_health_addr() -> String {
    Defaults::HEALTH_ADDR.to_string()
}
pub(crate) fn default_catalog() -> String {
    Defaults::CATALOG.to_string()
}
pub(crate) fn default_api_url() -> String {
    Defaults::API_URL.to_string()
}
pub(crate) fn default_server_name() -> String {
    Defaults::SERVER_NAME.to_string()
}
pub(crate) fn default_global_budget() -> usize {
    Defaults::GLOBAL_CONNECTION_BUDGET
}
pub(crate) fn default_limit() -> Option<usize> {
    Some(Defaults::LIMIT)
}
pub(crate) fn default_max_attempts() -> u32 {
    Defaults::MAX_ATTEMPTS
}
pub(crate) fn default_base_delay_ms() -> u64 {
    Defaults::BASE_DELAY_MS
}
pub(crate) fn default_max_delay_ms() -> u64 {
    Defaults::MAX_DELAY_MS
}
pub(crate) fn default_api_key() -> String {
    Defaults::API_KEY.to_string()
}
pub(crate) fn default_cache_ttl() -> u64 {
    Defaults::CACHE_TTL_SECS
}
pub(crate) fn default_cache_capacity() -> u64 {
    Defaults::CACHE_MAX_CAPACITY
}
pub(crate) fn default_telemetry_enabled() -> bool {
    Defaults::TELEMETRY_ENABLED
}
pub(crate) fn default_otlp_endpoint() -> String {
    Defaults::OTLP_ENDPOINT.to_string()
}
pub(crate) fn default_mcp_port() -> u16 {
    Defaults::MCP_PORT
}
pub(crate) fn default_use_firecracker() -> bool {
    Defaults::USE_FIRECRACKER
}
pub(crate) fn default_mcp_max_output_rows() -> usize {
    Defaults::MCP_MAX_OUTPUT_ROWS
}
pub(crate) fn default_mcp_sidecar_startup_delay_ms() -> u64 {
    500
}
pub(crate) fn default_mcp_sidecar_shutdown_timeout_ms() -> u64 {
    5000
}
pub(crate) fn default_mcp_health_check_interval_ms() -> u64 {
    10000
}
pub(crate) fn default_mcp_cooldown_secs() -> u64 {
    30
}
