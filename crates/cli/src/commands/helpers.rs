//! Shared helper functions and types for CLI commands.
//!
//! # Components
//! - **Utilities**: `expand_secrets` (env var substitution), `get_client` (authenticated HTTP), `parse_yaml` (helpers).
//! - **Result Types**: Serializable structs used by commands for machine-readable (JSON/YAML) output.
//!   Common types include `ValidateResult`, `ApplyResult`, `DiffResult`, etc.

use crate::config::CliConfig;
use crate::models;
use anyhow::{Context, Result};
use regex::Regex;
use serde::Serialize;
use std::env;
use std::fs;

/// Expand environment variable placeholders in content.
pub fn expand_secrets(content: &str) -> String {
    let re = Regex::new(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}").unwrap();
    re.replace_all(content, |caps: &regex::Captures| {
        let var_name = &caps[1];
        env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name))
    })
    .to_string()
}

/// Build an authenticated HTTP client using the provided configuration.
pub fn get_client(config: &CliConfig) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = &config.token {
        if let Ok(value) = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token)) {
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
    }
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .context("Failed to build HTTP client")
}

/// Parse a YAML configuration file with secret expansion.
pub fn parse_yaml(path: &str) -> Result<models::SourcesConfig> {
    let raw_content =
        fs::read_to_string(path).context(format!("Failed to read config file: {}", path))?;
    let content = expand_secrets(&raw_content);
    serde_yaml::from_str(&content).context("Failed to parse YAML structure")
}

// ===== Result Types =====

#[derive(Serialize)]
pub struct ValidateResult {
    pub valid: bool,
    pub errors: Vec<String>,
}

#[derive(Serialize)]
pub struct DiffChange {
    #[serde(rename = "type")]
    pub change_type: String,
    pub category: String,
    pub name: String,
    pub details: Option<String>,
}

#[derive(Serialize)]
pub struct DiffResult {
    pub changes: Vec<DiffChange>,
}

#[derive(Serialize)]
pub struct ApplyResult {
    pub domain: String,
    pub version: i32,
    pub added: Vec<String>,
    pub deleted: Vec<String>,
    pub dry_run: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff: Option<DiffResult>,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub source: String,
    pub domain: String,
    pub tables: Vec<strake_common::models::TableDiscovery>,
}

#[derive(Serialize)]
pub struct AddResult {
    pub source: String,
    pub table: String,
    pub domain: String,
    pub output_file: String,
}

#[derive(Serialize)]
pub struct TestConnectionResult {
    pub source: String,
    pub valid: bool,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct TestConnectionSummary {
    pub results: Vec<TestConnectionResult>,
}

#[derive(Serialize)]
pub struct DomainEntry {
    pub name: String,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
pub struct DomainHistoryEntry {
    pub version: i32,
    pub user_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub added: usize,
    pub deleted: usize,
}
