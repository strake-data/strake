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

use super::apply_models::{
    ApplyReceipt, ApplyStatus, ChangeSummary, ErrorDetail, RejectionDetail, ResourceChanges,
    VersionTransition,
};
use super::diff_logic::{diff_internal, print_diff_human};
use super::validate::validate;
use crate::config::CliConfig;
use crate::models::tables_equal;
use crate::{
    exit_codes,
    metadata::{MetadataStore, models::ApplyLogEntry},
    output::{self, OutputFormat},
    secrets::ResolverContext,
};
use anyhow::{Context, Result};
use chrono::Utc;
use owo_colors::OwoColorize;
use serde_json::json;
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;
use strake_common::models::{SourceConfig, SourcesConfig, TableConfig};

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
    ctx: &ResolverContext,
) -> Result<i32> {
    let started_at = Instant::now();
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

    // Expand secrets and parse
    let expanded_yaml = super::helpers::expand_secrets(&raw_yaml, ctx);
    let source_config: crate::models::SourcesConfig =
        serde_yaml::from_str(&expanded_yaml).context("Failed to parse YAML structure")?;

    let domain = source_config.domain.as_deref().unwrap_or("default");
    let actor = resolve_actor();
    let previous_config = store
        .get_sources(domain)
        .await
        .unwrap_or_else(|_| SourcesConfig {
            domain: Some(domain.to_string()),
            sources: Vec::new(),
        });

    if dry_run {
        if !format.is_machine_readable() {
            println!("\n--- DRY RUN MODE ---");
            println!("Target Domain: {}", domain);
        }

        // Validate (Machine mode silent, human mode prints)
        let validation_exit = validate(
            &options.file_path,
            false,
            false,
            options.format,
            config,
            ctx,
        )
        .await?;
        if validation_exit == exit_codes::EXIT_ERROR {
            return Ok(exit_codes::EXIT_ERROR);
        }

        if !options.format.is_machine_readable() {
            println!();
        }

        let diff_result = diff_internal(store, &options.file_path, ctx).await?;

        if options.format.is_machine_readable() {
            let current_version = store.get_domain_version(domain).await.unwrap_or(0);
            let receipt = ApplyReceipt {
                receipt_version: 1,
                applied_at: Utc::now(),
                actor,
                domain: domain.to_string(),
                version: VersionTransition {
                    previous: Some(current_version),
                    current: current_version,
                },
                duration_ms: started_at.elapsed().as_millis(),
                status: ApplyStatus::DryRun,
                changes: compute_changes(&previous_config, &source_config),
                warnings: Vec::new(),
                drift_detected: false,
                rejection: None,
                errors: Vec::new(),
            };
            output::print_output(options.format, receipt)?;
        } else {
            print_diff_human(&diff_result);
            println!("\nNo changes applied (dry-run mode).");
        }
        return Ok(exit_codes::EXIT_DRY_RUN);
    }

    // Ensure schema exists before importing (do not run during dry-run)
    store.init().await?;

    // 1. Optimistic Locking
    let current_version_to_update = match expected_version {
        Some(v) => v,
        None => store.get_domain_version(domain).await?,
    };

    // 2. Increment version (Locking)
    let new_version: i32 = match store
        .increment_domain_version(domain, current_version_to_update)
        .await
    {
        Ok(version) => version,
        Err(err) => {
            if format.is_machine_readable() {
                let actual_version = store
                    .get_domain_version(domain)
                    .await
                    .unwrap_or(current_version_to_update);
                let receipt = ApplyReceipt {
                    receipt_version: 1,
                    applied_at: Utc::now(),
                    actor,
                    domain: domain.to_string(),
                    version: VersionTransition {
                        previous: Some(current_version_to_update),
                        current: actual_version,
                    },
                    duration_ms: started_at.elapsed().as_millis(),
                    status: ApplyStatus::Rejected,
                    changes: ResourceChanges::default(),
                    warnings: Vec::new(),
                    drift_detected: false,
                    rejection: Some(RejectionDetail {
                        reason: "version_conflict".to_string(),
                        expected_version: current_version_to_update,
                        actual_version,
                        detail: "Domain was modified by another actor between plan and apply."
                            .to_string(),
                    }),
                    errors: Vec::new(),
                };
                output::print_output(format, receipt)?;
                return Ok(exit_codes::EXIT_ERROR);
            }
            return Err(err).context(
                "Failed to increment domain version. Another user may have modified the domain.",
            );
        }
    };

    // 3. Import
    let apply_res = match store.apply_sources(&source_config, options.force).await {
        Ok(result) => result,
        Err(err) => {
            if format.is_machine_readable() {
                let receipt = ApplyReceipt {
                    receipt_version: 1,
                    applied_at: Utc::now(),
                    actor,
                    domain: domain.to_string(),
                    version: VersionTransition {
                        previous: Some(current_version_to_update),
                        current: current_version_to_update,
                    },
                    duration_ms: started_at.elapsed().as_millis(),
                    status: ApplyStatus::Failed,
                    changes: ResourceChanges::default(),
                    warnings: Vec::new(),
                    drift_detected: false,
                    rejection: None,
                    errors: vec![ErrorDetail {
                        code: "STRAKE-3000".to_string(),
                        source: None,
                        detail: err.to_string(),
                    }],
                };
                output::print_output(format, receipt)?;
                return Ok(exit_codes::EXIT_ERROR);
            }
            return Err(err);
        }
    };

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

    let receipt = ApplyReceipt {
        receipt_version: 1,
        applied_at: Utc::now(),
        actor,
        domain: domain.to_string(),
        version: VersionTransition {
            previous: Some(current_version_to_update),
            current: new_version,
        },
        duration_ms: started_at.elapsed().as_millis(),
        status: ApplyStatus::Applied,
        changes: compute_changes(&previous_config, &source_config),
        warnings: Vec::new(),
        drift_detected: false,
        rejection: None,
        errors: Vec::new(),
    };

    if options.format.is_machine_readable() {
        output::print_output(options.format, &receipt)?;
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
            eprintln!("{} Notifying server at {}...", "ℹ".blue(), url);
        }
        let client = reqwest::Client::new();
        match client.post(&url).json(&receipt).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if !format.is_machine_readable() {
                        eprintln!("{} Server notification successful.", "✔".green());
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

    Ok(exit_codes::EXIT_OK)
}

fn resolve_actor() -> String {
    std::env::var("STRAKE_ACTOR")
        .ok()
        .or_else(|| std::env::var("STRAKE_PROFILE").ok())
        .unwrap_or_else(|| "unknown".to_string())
}

#[must_use]
fn compute_changes(previous: &SourcesConfig, current: &SourcesConfig) -> ResourceChanges {
    let previous_sources: HashMap<&str, &SourceConfig> = previous
        .sources
        .iter()
        .map(|source| (source.name.as_str(), source))
        .collect();
    let current_sources: HashMap<&str, &SourceConfig> = current
        .sources
        .iter()
        .map(|source| (source.name.as_str(), source))
        .collect();

    let previous_names: BTreeSet<&str> = previous_sources.keys().copied().collect();
    let current_names: BTreeSet<&str> = current_sources.keys().copied().collect();

    let source_changes = ChangeSummary {
        added: current_names
            .difference(&previous_names)
            .map(|name| (*name).to_string())
            .collect(),
        removed: previous_names
            .difference(&current_names)
            .map(|name| (*name).to_string())
            .collect(),
        modified: current_names
            .intersection(&previous_names)
            .filter_map(|name| {
                let current_source = current_sources.get(name)?;
                let previous_source = previous_sources.get(name)?;
                if strake_common::models::sources_equal(current_source, previous_source) {
                    None
                } else {
                    Some((*name).to_string())
                }
            })
            .collect(),
    };

    let previous_tables = flatten_tables(previous);
    let current_tables = flatten_tables(current);
    let previous_table_names: BTreeSet<String> = previous_tables.keys().cloned().collect();
    let current_table_names: BTreeSet<String> = current_tables.keys().cloned().collect();

    let table_changes = ChangeSummary {
        added: current_table_names
            .difference(&previous_table_names)
            .map(|name| name.to_string())
            .collect(),
        removed: previous_table_names
            .difference(&current_table_names)
            .map(|name| name.to_string())
            .collect(),
        modified: current_table_names
            .intersection(&previous_table_names)
            .filter_map(|name| {
                let current_table = current_tables.get(name.as_str())?;
                let previous_table = previous_tables.get(name.as_str())?;
                if tables_equal(current_table, previous_table) {
                    None
                } else {
                    Some(name.to_string())
                }
            })
            .collect(),
    };

    ResourceChanges {
        sources: source_changes,
        tables: table_changes,
        contracts: ChangeSummary::default(),
        policies: ChangeSummary::default(),
    }
}

fn flatten_tables(config: &SourcesConfig) -> HashMap<String, &TableConfig> {
    let mut tables = HashMap::new();
    for source in &config.sources {
        for table in &source.tables {
            tables.insert(format!("{}.{}", source.name, table.name), table);
        }
    }
    tables
}

#[cfg(test)]
mod tests {
    use super::compute_changes;
    use strake_common::models::{ColumnConfig, SourceConfig, SourcesConfig, TableConfig};

    #[test]
    fn compute_changes_captures_added_and_removed_resources() {
        let previous = SourcesConfig {
            domain: Some("finance".to_string()),
            sources: vec![SourceConfig {
                name: "warehouse".to_string(),
                source_type: "postgres".to_string(),
                url: Some("postgres://old".to_string()),
                username: None,
                password: None,
                max_concurrent_queries: None,
                default_limit: None,
                cache: None,
                tables: vec![TableConfig {
                    name: "orders".to_string(),
                    schema: "public".to_string(),
                    partition_column: None,
                    columns: vec![],
                }],
                config: serde_json::Value::Null,
            }],
        };
        let current = SourcesConfig {
            domain: Some("finance".to_string()),
            sources: vec![SourceConfig {
                name: "reporting".to_string(),
                source_type: "postgres".to_string(),
                url: Some("postgres://new".to_string()),
                username: None,
                password: None,
                max_concurrent_queries: None,
                default_limit: None,
                cache: None,
                tables: vec![TableConfig {
                    name: "revenue".to_string(),
                    schema: "public".to_string(),
                    partition_column: None,
                    columns: vec![],
                }],
                config: serde_json::Value::Null,
            }],
        };

        let changes = compute_changes(&previous, &current);
        assert_eq!(changes.sources.added, vec!["reporting"]);
        assert_eq!(changes.sources.removed, vec!["warehouse"]);
        assert_eq!(changes.tables.added, vec!["reporting.revenue"]);
        assert_eq!(changes.tables.removed, vec!["warehouse.orders"]);
    }

    #[test]
    fn compute_changes_marks_modified_tables() {
        let previous = SourcesConfig {
            domain: Some("finance".to_string()),
            sources: vec![SourceConfig {
                name: "warehouse".to_string(),
                source_type: "postgres".to_string(),
                url: Some("postgres://warehouse".to_string()),
                username: None,
                password: None,
                max_concurrent_queries: None,
                default_limit: None,
                cache: None,
                tables: vec![TableConfig {
                    name: "orders".to_string(),
                    schema: "public".to_string(),
                    partition_column: None,
                    columns: vec![ColumnConfig {
                        name: "amount".to_string(),
                        data_type: "INT".to_string(),
                        length: None,
                        primary_key: false,
                        unique: false,
                        not_null: false,
                        description: None,
                    }],
                }],
                config: serde_json::Value::Null,
            }],
        };
        let current = SourcesConfig {
            domain: Some("finance".to_string()),
            sources: vec![SourceConfig {
                name: "warehouse".to_string(),
                source_type: "postgres".to_string(),
                url: Some("postgres://warehouse".to_string()),
                username: None,
                password: None,
                max_concurrent_queries: None,
                default_limit: None,
                cache: None,
                tables: vec![TableConfig {
                    name: "orders".to_string(),
                    schema: "public".to_string(),
                    partition_column: None,
                    columns: vec![ColumnConfig {
                        name: "amount".to_string(),
                        data_type: "NUMERIC".to_string(),
                        length: None,
                        primary_key: false,
                        unique: false,
                        not_null: false,
                        description: None,
                    }],
                }],
                config: serde_json::Value::Null,
            }],
        };

        let changes = compute_changes(&previous, &current);
        assert_eq!(changes.sources.modified, vec!["warehouse"]);
        assert_eq!(changes.tables.modified, vec!["warehouse.orders"]);
    }
}
