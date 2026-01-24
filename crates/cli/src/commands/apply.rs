//! Apply command for applying configuration to the metadata store.
//!
//! # Overview
//! The `apply` command creates a new version of the domain configuration in the
//! metadata store based on the local `sources.yaml`. It is the primary mechanism
//! in the GitOps workflow.
//!
//! # Key Features
//! - **Optimistic Locking**: Ensures no concurrent modifications via `expected_version` or db checks.
//! - **Atomic Updates**: All changes for a domain version are applied transactionally.
//! - **Audit Logging**: Records who applied the change, invalidates caches, and stores the full config snapshot.
//! - **Dry Run**: Preview changes (via `diff`) without applying them.

use super::diff::{diff_internal, print_diff_human};
use super::helpers::{parse_yaml, ApplyResult};
use super::validate::validate;
use crate::config::CliConfig;
use crate::{
    metadata::{models::ApplyLogEntry, MetadataStore},
    output::{self, OutputFormat},
};
use anyhow::{Context, Result};
use owo_colors::OwoColorize;
use serde_json::json;
use std::fs;

pub async fn apply(
    store: &dyn MetadataStore,
    file_path: &str,
    force: bool,
    dry_run: bool,
    expected_version: Option<i32>,
    format: OutputFormat,
    config: &CliConfig,
) -> Result<()> {
    if !format.is_machine_readable() {
        println!(
            "{} {} {}",
            "[Config:".dimmed(),
            file_path.yellow(),
            "] Applying configuration...".bold().cyan()
        );
    }

    let source_config = parse_yaml(file_path)?;
    let domain = source_config.domain.as_deref().unwrap_or("default");

    store.init().await?; // Ensure schema exists before any logic or diffing

    if dry_run {
        if !format.is_machine_readable() {
            println!("\n--- DRY RUN MODE ---");
            println!("Target Domain: {}", domain);
        }

        // Validate (Machine mode silent, human mode prints)
        validate(file_path, false, format, config).await?;

        if !format.is_machine_readable() {
            println!();
        }

        let diff_result = diff_internal(store, file_path).await?;

        if format.is_machine_readable() {
            let result = ApplyResult {
                domain: domain.to_string(),
                version: store.get_domain_version(domain).await.unwrap_or(0),
                added: vec![],
                deleted: vec![],
                dry_run: true,
                diff: Some(super::helpers::DiffResult {
                    changes: diff_result.changes,
                }),
            };
            output::print_success(format, result)?;
        } else {
            print_diff_human(&diff_result);
            println!("\nNo changes applied (dry-run mode).");
        }
        return Ok(());
    }

    // store.init() already called above before dry_run check

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
    let apply_res = store.apply_sources(&source_config, force).await?;

    // 4. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());
    let raw_yaml = fs::read_to_string(file_path)?;
    let config_hash = format!("{:x}", md5::compute(&raw_yaml)); // Simple hash

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

    if format.is_machine_readable() {
        output::print_success(
            format,
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
    Ok(())
}
