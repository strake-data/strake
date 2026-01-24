//! Domain-related commands: rollback, list_domains, show_domain_history.
//!
//! # Overview
//! Domains in Strake represent isolated namespaces for configurations (e.g., `prod`, `staging`).
//! These commands allow managing the lifecycle and history of domain configurations.
//!
//! # Commands
//! - **list_domains**: Shows all active domains and their current versions.
//! - **show_domain_history**: Displays the audit log of applied configurations for a domain.
//! - **rollback**: Reverts a domain to a previous version by re-applying the historical configuration.

use super::helpers::{ApplyResult, DomainEntry, DomainHistoryEntry};
use crate::{
    metadata::{models::ApplyLogEntry, MetadataStore},
    models,
    output::{self, OutputFormat},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use owo_colors::OwoColorize;
use serde_json::json;

pub async fn rollback(
    store: &dyn MetadataStore,
    domain: &str,
    to_version: i32,
    format: OutputFormat,
) -> Result<()> {
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
    let apply_res = store.apply_sources(&config, true).await?;

    // 5. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());
    let config_hash = format!("{:x}", md5::compute(&config_yaml));

    store
        .log_apply_event(ApplyLogEntry {
            domain: domain.to_string(),
            version: new_version,
            user_id,
            sources_added: serde_json::to_value(&apply_res.sources_added).unwrap_or(json!([])),
            sources_deleted: serde_json::to_value(&apply_res.sources_deleted).unwrap_or(json!([])),
            tables_modified: json!([]),
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
                added: apply_res.sources_added,
                deleted: apply_res.sources_deleted,
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
    Ok(())
}

pub async fn list_domains(store: &dyn MetadataStore, format: OutputFormat) -> Result<()> {
    let domains = store.list_domains().await?;

    if format.is_machine_readable() {
        let mut results = Vec::new();
        for d in domains {
            results.push(DomainEntry {
                name: d.name,
                version: d.version,
                created_at: d.created_at.unwrap_or_else(chrono::Utc::now),
            });
        }
        output::print_success(format, &results)?;
        return Ok(());
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
    Ok(())
}

pub async fn show_domain_history(
    store: &dyn MetadataStore,
    domain: String,
    format: OutputFormat,
) -> Result<()> {
    let history = store.get_history(&domain, 10).await?;

    if format.is_machine_readable() {
        let mut results = Vec::new();
        for entry in history {
            // Explicit types for closure parameters to fix inference
            let added_count = entry
                .sources_added
                .as_array()
                .map(|a: &Vec<serde_json::Value>| a.len())
                .unwrap_or(0);
            let deleted_count = entry
                .sources_deleted
                .as_array()
                .map(|d: &Vec<serde_json::Value>| d.len())
                .unwrap_or(0);

            results.push(DomainHistoryEntry {
                version: entry.version,
                user_id: entry.user_id,
                timestamp: entry.timestamp.unwrap_or_else(chrono::Utc::now),
                added: added_count,
                deleted: deleted_count,
            });
        }
        output::print_success(format, &results)?;
        return Ok(());
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
        let added_list = entry
            .sources_added
            .as_array()
            .map(|a: &Vec<serde_json::Value>| a.len())
            .unwrap_or(0);
        let deleted_list = entry
            .sources_deleted
            .as_array()
            .map(|d: &Vec<serde_json::Value>| d.len())
            .unwrap_or(0);
        let ts_str = entry
            .timestamp
            .map(|ts: DateTime<Utc>| ts.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "N/A".to_string());

        println!(
            "{:<18} {:<15} {:<20} {} / {}",
            format!("v{}", entry.version).yellow().bold(),
            entry.user_id.dimmed(),
            ts_str.dimmed(),
            format!("+{}", added_list).green(),
            format!("-{}", deleted_list).red()
        );
    }
    Ok(())
}
