//! # Query Limits and Security Configuration
//!
//! Settings for query resource constraints, retry policies, and Agent Guard modes.
//!
//! ## Overview
//!
//! Defines [`QueryLimits`] to prevent resource exhaustion, [`RetrySettings`]
//! for resilient backend communication, and [`SecurityConfig`] for managing
//! the `AgentGuardMode` (Enforce, DryRun, Disabled).
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::config::limits::RetrySettings;
//! let retry = RetrySettings::default();
//! assert_eq!(retry.max_attempts, 5);
//! ```
//!
//! ## Performance Characteristics
//!
//! Limits are checked synchronously during query planning and execution.
//! Retry settings govern the timing of asynchronous retry loops.
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
use validator::Validate;

/// Configuration for security-related features.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, Validate)]
#[non_exhaustive]
pub struct SecurityConfig {
    /// The current mode for the Agent Guard.
    #[serde(default)]
    pub agent_guard_mode: AgentGuardMode,
}

/// Mode for the Agent Guard security layer.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AgentGuardMode {
    /// Security checks are disabled.
    #[serde(rename = "disabled")]
    Disabled,
    /// Security checks are performed but not enforced (only logged).
    #[serde(rename = "dry_run")]
    #[default]
    DryRun,
    /// Security checks are strictly enforced.
    #[serde(rename = "enforce")]
    Enforce,
}

impl std::fmt::Display for AgentGuardMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentGuardMode::Disabled => write!(f, "disabled"),
            AgentGuardMode::DryRun => write!(f, "dry_run"),
            AgentGuardMode::Enforce => write!(f, "enforce"),
        }
    }
}

/// Limits applied to query execution.
#[derive(Debug, Deserialize, Serialize, Clone, Validate)]
#[non_exhaustive]
pub struct QueryLimits {
    /// Maximum number of rows allowed in the final output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_rows: Option<usize>,
    /// Maximum number of bytes allowed to be scanned from source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_scan_bytes: Option<usize>,
    /// Default row limit applied if none is specified in the query.
    /// `None` indicates no default limit (unlimited scan).
    #[serde(default = "default_limit")]
    pub default_limit: Option<usize>,
    /// Maximum duration allowed for query execution in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_timeout_seconds: Option<u64>,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_output_rows: None,
            max_scan_bytes: None,
            default_limit: default_limit(),
            query_timeout_seconds: None,
        }
    }
}

/// Configuration for retrying fallible operations.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Validate)]
#[non_exhaustive]
pub struct RetrySettings {
    /// Maximum number of attempts.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base delay in milliseconds.
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,
    /// Maximum delay in milliseconds.
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            base_delay_ms: default_base_delay_ms(),
            max_delay_ms: default_max_delay_ms(),
        }
    }
}

/// Resource usage limits for the Strake process.
#[derive(Debug, Deserialize, Serialize, Default, Clone, Validate)]
#[non_exhaustive]
pub struct ResourceConfig {
    /// Maximum memory (in MB) the engine is allowed to use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<usize>,
    /// Directory used for spilling intermediate query results to disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spill_dir: Option<String>,
}
