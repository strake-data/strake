//! # Shared Helpers
//!
//! Shared helper functions and types for CLI commands.
//!
//! ## Overview
//!
//! - **Utilities**: `expand_secrets` (env var substitution), `get_client` (authenticated HTTP), `parse_yaml` (helpers).
//! - **Result Types**: Serializable structs used by commands for machine-readable (JSON/YAML) output.
//!   Common types include `ValidateResult`, `ApplyResult`, `DiffResult`, etc.
//!
//! ## Usage
//!
//! ```rust
//! // use crate::commands::helpers::parse_yaml;
//! ```
//!
//! ## Performance Characteristics
//!
//! `expand_secrets` uses a global `LazyLock` regex to prevent recompilation penalties.
//!
//! ## Safety
//!
//! Standard safe Rust.
//!
//! ## References
//!
//! - Helpers architecture specs.

use crate::config::CliConfig;
use crate::models;
use crate::secrets::{ResolverContext, SecretResolver};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Expand secret placeholders in content using the provided context.
pub fn expand_secrets(content: &str, ctx: &ResolverContext) -> String {
    // Note: This returns a String because YAML parsing requires a String.
    // However, the resolver ensures that it only exposes secrets into this String
    // for the minimum duration required to build the result.
    match SecretResolver::resolve(content, ctx) {
        Ok(secret) => {
            use secrecy::ExposeSecret;
            secret.expose_secret().to_string()
        }
        Err(_) => content.to_string(), // Fallback to raw if resolution fails (e.g. for partial strings)
    }
}

/// Build an authenticated HTTP client using the provided configuration.
pub fn get_client(config: &CliConfig) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = &config.token
        && let Ok(value) = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
    {
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .context("Failed to build HTTP client")
}

/// Parse a YAML configuration file with secret expansion.
pub async fn parse_yaml(path: &str, ctx: &ResolverContext) -> Result<models::SourcesConfig> {
    let raw_content = tokio::fs::read_to_string(path)
        .await
        .context(format!("Failed to read config file: {}", path))?;
    let content = expand_secrets(&raw_content, ctx);
    serde_yaml::from_str(&content).context("Failed to parse YAML structure")
}

#[derive(Debug, Clone)]
pub struct ManifestPaths {
    pub sources: PathBuf,
    pub contracts: PathBuf,
    pub policies: PathBuf,
    pub default_domain: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ProjectManifestConfig {
    #[serde(default)]
    sources_file: Option<String>,
    #[serde(default)]
    contracts_file: Option<String>,
    #[serde(default)]
    policies_file: Option<String>,
    #[serde(default)]
    default_domain: Option<String>,
}

pub async fn resolve_manifest_paths(explicit_sources: Option<&str>) -> Result<ManifestPaths> {
    let config = load_project_manifest_config().await?;

    let sources = explicit_sources
        .map(PathBuf::from)
        .or_else(|| config.sources_file.as_ref().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("sources.yaml"));

    let base_dir = sources
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let contracts = config
        .contracts_file
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| base_dir.join("contracts.yaml"));

    let policies = config
        .policies_file
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| base_dir.join("policies.yaml"));

    Ok(ManifestPaths {
        sources,
        contracts,
        policies,
        default_domain: config.default_domain,
    })
}

async fn load_project_manifest_config() -> Result<ProjectManifestConfig> {
    let path = PathBuf::from("strake.yaml");
    if tokio::fs::metadata(&path).await.is_err() {
        return Ok(ProjectManifestConfig::default());
    }

    let raw = tokio::fs::read_to_string(&path)
        .await
        .context(format!("Failed to read project config: {}", path.display()))?;
    let config = serde_yaml::from_str::<ProjectManifestConfig>(&raw)
        .context("Failed to parse project manifest settings from strake.yaml")?;
    Ok(config)
}

// ===== Result Types =====

#[derive(Serialize)]
pub struct ValidateResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(transparent)]
pub struct DomainName(pub String);

impl std::fmt::Display for DomainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ChangeType {
    #[serde(rename = "ADD")]
    Added,
    #[serde(rename = "MODIFY")]
    Modified,
    #[serde(rename = "DELETE")]
    Deleted,
}

impl std::fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ChangeType::Added => "ADD",
            ChangeType::Modified => "MODIFY",
            ChangeType::Deleted => "DELETE",
        };
        write!(f, "{}", s)
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DiffChange {
    pub change_type: ChangeType,
    pub path: String,
    pub previous: Option<String>,
    pub current: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct DiffResult {
    pub domain: DomainName,
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
