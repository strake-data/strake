//! # Apply Command
//!
//! The `apply` command creates a new version of the domain configuration in the
//! metadata store based on the local `sources.yaml`. It is the primary mechanism
//! in the GitOps workflow.
//!
//! ## Overview
//!
//! This module coordinates optimistic locking, atomic database updates, audit logging,
//! and server notification upon successful configuration application. It can optionally
//! run in dry-run mode.
//!
//! ## Usage
//!
//! ```rust
//! // apply(&store, options, &config).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Config parsing is done synchronously before dispatching async tasks. Hashes the entire
//! YAML config to store audit proofs.
//!
//! ## Safety
//!
//! No unsafe blocks.
//!
//! ## References
//!
//! - [Apply Command RFC](https://strake.io/docs)

use super::diff::{diff_internal, print_diff_human};
use super::helpers::ApplyResult;
use super::validate::validate;
use crate::config::CliConfig;
use crate::{
    metadata::{models::ApplyLogEntry, MetadataStore},
    output::{self, OutputFormat},
};
use anyhow::{Context, Result};
use owo_colors::OwoColorize;
use serde_json::json;

pub struct ApplyOptions {
    pub file_path: String,
    pub force: bool,
    pub dry_run: bool,
    pub expected_version: Option<i32>,
    pub format: OutputFormat,
    pub notify_url: Option<String>,
}

pub async fn apply(
    store: &dyn MetadataStore,
    options: ApplyOptions,
    config: &CliConfig,
) -> Result<()> {
    let format = options.format;
    let dry_run = options.dry_run;
    let expected_version = options.expected_version;
    if !options.format.is_machine_readable() {
        println!(
            "{} {} {}",
            "[Config:".dimmed(),
            options.file_path.yellow(),
            "] Applying configuration...".bold().cyan()
        );
    }
    let raw_yaml = tokio::fs::read_to_string(&options.file_path)
        .await
        .context(format!("Failed to read config file: {}", options.file_path))?;

    if let Ok(raw_source_config) = serde_yaml::from_str::<crate::models::SourcesConfig>(&raw_yaml) {
        for source in &raw_source_config.sources {
            if let Some(password) = &source.password {
                use secrecy::ExposeSecret;
                let sec = password.expose_secret();
                if !sec.starts_with("${") {
                    tracing::warn!("Plaintext password stored in database for source '{}'. Use environment variable substitution (e.g. ${{PASSWORD}}) for secure operation.", source.name);
                }
            }
        }
    }

    // Expand secrets and parse
    let expanded_yaml = super::helpers::expand_secrets(&raw_yaml);
    let source_config: crate::models::SourcesConfig =
        serde_yaml::from_str(&expanded_yaml).context("Failed to parse YAML structure")?;

    let domain = source_config.domain.as_deref().unwrap_or("default");

    if dry_run {
        if !format.is_machine_readable() {
            println!("\n--- DRY RUN MODE ---");
            println!("Target Domain: {}", domain);
        }

        // Validate (Machine mode silent, human mode prints)
        validate(&options.file_path, false, options.format, config).await?;

        if !options.format.is_machine_readable() {
            println!();
        }

        let diff_result = diff_internal(store, &options.file_path).await?;

        if options.format.is_machine_readable() {
            let result = ApplyResult {
                domain: domain.to_string(),
                version: store.get_domain_version(domain).await.unwrap_or(0),
                added: vec![],
                deleted: vec![],
                dry_run: true,
                diff: Some(super::helpers::DiffResult {
                    domain: super::helpers::DomainName(domain.to_string()),
                    changes: diff_result.changes,
                }),
            };
            output::print_success(options.format, result)?;
        } else {
            print_diff_human(&diff_result);
            println!("\nNo changes applied (dry-run mode).");
        }
        return Ok(());
    }

    // Ensure schema exists before importing (do not run during dry-run)
    store.init().await?;

    // 1. Optimistic Locking
    let current_version_to_update = match expected_version {
        Some(v) => v,
        None => store.get_domain_version(domain).await?,
    };

    // 2. Increment version (Locking)
    let new_version: i32 = store
        .increment_domain_version(domain, current_version_to_update)
        .await
        .context(
            "Failed to increment domain version. Another user may have modified the domain.",
        )?;

    // 3. Import
    let apply_res = store.apply_sources(&source_config, options.force).await?;

    // 4. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());
    // raw_yaml is already read at the start of the function

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(raw_yaml.as_bytes());
    let config_hash = format!("{:x}", hasher.finalize());

    store
        .log_apply_event(ApplyLogEntry {
            domain: domain.to_string(),
            version: new_version,
            user_id,
            sources_added: serde_json::to_value(&apply_res.sources_added).unwrap_or(json!([])),
            sources_deleted: serde_json::to_value(&apply_res.sources_deleted).unwrap_or(json!([])),
            tables_modified: json!([]), // Not detailed yet
            config_hash,
            config_yaml: raw_yaml,
            timestamp: None,
        })
        .await?;

    if options.format.is_machine_readable() {
        output::print_success(
            options.format,
            ApplyResult {
                domain: domain.to_string(),
                version: new_version,
                added: apply_res.sources_added,
                deleted: apply_res.sources_deleted,
                dry_run: false,
                diff: None,
            },
        )?;
    } else {
        println!(
            "{} configuration applied successfully to domain '{}' (New version: {}).",
            "✔".green(),
            domain.bold(),
            format!("v{}", new_version).yellow()
        );
    }

    // 5. Notify Server (if configured)
    if let Some(url) = options.notify_url {
        if !options.format.is_machine_readable() {
            println!("{} Notifying server at {}...", "ℹ".blue(), url);
        }
        let client = reqwest::Client::new();
        match client.post(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if !format.is_machine_readable() {
                        println!("{} Server notification successful.", "✔".green());
                    }
                } else {
                    eprintln!("{} Server returned error: {}", "✖".red(), resp.status());
                }
            }
            Err(e) => {
                eprintln!("{} Failed to notify server: {}", "✖".red(), e);
            }
        }
    }

    Ok(())
}
