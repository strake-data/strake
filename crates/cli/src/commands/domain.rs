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
    db, models,
    output::{self, OutputFormat},
};
use anyhow::Result;
use owo_colors::OwoColorize;
use tokio_postgres::Client;

pub async fn rollback(
    client: &Client,
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

    // 1. Fetch old config
    let config_yaml = db::get_history_config(client, domain, to_version).await?;
    let config: models::SourcesConfig = serde_yaml::from_str(&config_yaml)?;

    // 2. Optimistic Locking
    let current_version = db::get_domain_version(client, domain).await?;
    let new_version = db::increment_domain_version(client, domain, current_version).await?;

    // 3. Import
    let (added, deleted) = db::import_sources(client, &config, true).await?;

    // 4. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());
    let config_hash = format!("{:x}", md5::compute(&config_yaml));

    db::log_apply_event(
        client,
        db::ApplyLogEntry {
            domain,
            version: new_version,
            user_id: &user_id,
            added: &added,
            deleted: &deleted,
            config_hash: &config_hash,
            config_yaml: &config_yaml,
        },
    )
    .await?;

    if format.is_machine_readable() {
        output::print_success(
            format,
            ApplyResult {
                domain: domain.to_string(),
                version: new_version,
                added,
                deleted,
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

pub async fn list_domains(client: &Client, format: OutputFormat) -> Result<()> {
    let rows = client
        .query(
            "SELECT name, version, created_at FROM domains ORDER BY name",
            &[],
        )
        .await?;

    if format.is_machine_readable() {
        let mut results = Vec::new();
        for row in rows {
            results.push(DomainEntry {
                name: row.get(0),
                version: row.get(1),
                created_at: row.get(2),
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
    for row in rows {
        let name: String = row.get(0);
        let version: i32 = row.get(1);
        let created_at: chrono::DateTime<chrono::Utc> = row.get(2);
        println!(
            "{:<20} v{:<9} {}",
            name.bold().cyan(),
            version.yellow(),
            created_at.format("%Y-%m-%d %H:%M:%S").dimmed()
        );
    }
    Ok(())
}

pub async fn show_domain_history(
    client: &Client,
    domain: String,
    format: OutputFormat,
) -> Result<()> {
    let rows = client.query(
        "SELECT version, user_id, timestamp, sources_added, sources_deleted FROM apply_history WHERE domain_name = $1 ORDER BY version DESC LIMIT 10",
        &[&domain]
    ).await?;

    if format.is_machine_readable() {
        let mut results = Vec::new();
        for row in rows {
            let added: serde_json::Value = row.get(3);
            let deleted: serde_json::Value = row.get(4);
            let added_count = added.as_array().map(|a| a.len()).unwrap_or(0);
            let deleted_count = deleted.as_array().map(|d| d.len()).unwrap_or(0);

            results.push(DomainHistoryEntry {
                version: row.get(0),
                user_id: row.get(1),
                timestamp: row.get(2),
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

    for row in rows {
        let version: i32 = row.get(0);
        let user: String = row.get(1);
        let ts: chrono::DateTime<chrono::Utc> = row.get(2);
        let added: serde_json::Value = row.get(3);
        let deleted: serde_json::Value = row.get(4);

        let added_list = added.as_array().map(|a| a.len()).unwrap_or(0);
        let deleted_list = deleted.as_array().map(|d| d.len()).unwrap_or(0);

        println!(
            "{:<18} {:<15} {:<20} {} / {}",
            format!("v{}", version).yellow().bold(),
            user.dimmed(),
            ts.format("%Y-%m-%d %H:%M").dimmed(),
            format!("+{}", added_list).green(),
            format!("-{}", deleted_list).red()
        );
    }
    Ok(())
}
