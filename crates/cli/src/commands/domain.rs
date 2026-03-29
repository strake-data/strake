//! # Domain Management
//!
//! Domain-related commands: rollback, list_domains, show_domain_history.
//!
//! ## Overview
//!
//! Domains in Strake represent isolated namespaces for configurations (e.g., `prod`, `staging`).
//! These commands allow managing the lifecycle and history of domain configurations.
//!
//! ## Usage
//!
//! ```ignore
//! // rollback(&store, domain, version, force, format).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Fetches history states using offset/limit pagination to avoid memory bloat.
//!
//! ## Safety
//!
//! Standard safe Rust. Uses transactional boundaries.
//!
//! ## References
//!
//! - Domain Management Proposal

use super::helpers::{ApplyResult, DomainEntry, DomainHistoryEntry};
use crate::secrets::ResolverContext;
use crate::{
    exit_codes,
    metadata::{MetadataStore, models::ApplyLogEntry},
    models::{self, DomainName},
    output::{self, OutputFormat},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use owo_colors::OwoColorize;

pub async fn rollback(
    store: &dyn MetadataStore,
    domain: &DomainName,
    to_version: i32,
    force: bool,
    format: OutputFormat,
    _ctx: &ResolverContext,
) -> Result<i32> {
    if !format.is_machine_readable() {
        println!(
            "{} Rolling back domain '{}' to version {}...",
            "⟲".yellow(),
            domain.bold(),
            format!("v{}", to_version).yellow()
        );
    }

    // 1. Fetch old config (explicit type to help inference)
    let config_yaml: String = store.get_history_config(domain, to_version).await?;
    let config: models::SourcesConfig = serde_yaml::from_str(&config_yaml)?;

    // 2. Optimistic Locking
    let current_version = store.get_domain_version(domain).await?;
    // 3. Increment version
    let new_version = store
        .increment_domain_version(domain, current_version)
        .await?;

    // 4. Import
    let apply_res = store.apply_sources(&config, force).await?;

    // 5. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(config_yaml.as_bytes());
    let config_hash = format!("{:x}", hasher.finalize());

    store
        .log_apply_event(ApplyLogEntry {
            domain: domain.clone(),
            version: new_version,
            user_id: user_id.into(),
            sources_added: apply_res.sources_added.clone(),
            sources_deleted: apply_res.sources_deleted.clone(),
            tables_modified: Vec::new(),
            config_hash,
            config_yaml,
            timestamp: None,
        })
        .await?;

    if format.is_machine_readable() {
        output::print_success(
            format,
            ApplyResult {
                domain: domain.to_string(),
                version: new_version,
                added: apply_res
                    .sources_added
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                deleted: apply_res
                    .sources_deleted
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                dry_run: false,
                diff: None,
            },
        )?;
    } else {
        println!(
            "{} Domain '{}' rolled back successfully to version {} (New version: {}).",
            "✔".green(),
            domain.bold(),
            format!("v{}", to_version).yellow(),
            format!("v{}", new_version).yellow()
        );
    }
    Ok(exit_codes::EXIT_OK)
}

pub async fn list_domains(
    store: &dyn MetadataStore,
    format: OutputFormat,
    _ctx: &ResolverContext,
) -> Result<i32> {
    let domains = store.list_domains().await?;

    if format.is_machine_readable() {
        let mut results = Vec::new();
        for d in domains {
            results.push(DomainEntry {
                name: d.name.to_string(),
                version: d.version,
                created_at: d.created_at.unwrap_or_else(Utc::now),
            });
        }
        output::print_success(format, &results)?;
        return Ok(exit_codes::EXIT_OK);
    }

    println!(
        "{:<20} {:<10} {:<20}",
        "DOMAIN".bold(),
        "VERSION".bold(),
        "CREATED AT".bold()
    );
    println!("{}", "-".repeat(50).dimmed());
    for d in domains {
        println!(
            "{:<20} v{:<9} {}",
            d.name.bold().cyan(),
            d.version.yellow(),
            d.created_at
                .map(|ts: DateTime<Utc>| ts.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "N/A".to_string())
                .dimmed()
        );
    }
    Ok(exit_codes::EXIT_OK)
}

pub async fn show_domain_history(
    store: &dyn MetadataStore,
    domain: DomainName,
    format: OutputFormat,
    _ctx: &ResolverContext,
) -> Result<i32> {
    let history = store.get_history(&domain, 10).await?;

    if format.is_machine_readable() {
        let mut results = Vec::new();
        for entry in history {
            results.push(DomainHistoryEntry {
                version: entry.version,
                user_id: entry.user_id.to_string(),
                timestamp: entry.timestamp.unwrap_or_else(chrono::Utc::now),
                added: entry.sources_added.len(),
                deleted: entry.sources_deleted.len(),
            });
        }
        output::print_success(format, &results)?;
        return Ok(exit_codes::EXIT_OK);
    }

    println!(
        "{} '{}':",
        "Apply History for Domain".bold().cyan(),
        domain.bold()
    );
    println!(
        "{:<10} {:<15} {:<20} {:<20}",
        "VERSION".bold(),
        "USER".bold(),
        "TIMESTAMP".bold(),
        "CHANGES".bold()
    );
    println!("{}", "-".repeat(70).dimmed());

    for entry in history {
        let added_list = entry.sources_added.len();
        let deleted_list = entry.sources_deleted.len();
        let ts_str = entry
            .timestamp
            .map(|ts: DateTime<Utc>| ts.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "N/A".to_string());

        println!(
            "{:<18} {:<15} {:<20} {} / {}",
            format!("v{}", entry.version).yellow().bold(),
            entry.user_id.as_ref().dimmed(),
            ts_str.dimmed(),
            format!("+{}", added_list).green(),
            format!("-{}", deleted_list).red()
        );
    }
    Ok(exit_codes::EXIT_OK)
}
