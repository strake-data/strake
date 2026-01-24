//! Diff command and helpers for comparing local config with metadata store.
//!
//! # Overview
//! The `diff` command provides a dry-run preview of what changes would be applied
//! to the metadata store state if `strake apply` were run.
//!
//! # Comparison Logic
//! It compares the local `sources.yaml` against the current state in the database
//! for the specified domain. It detects:
//! - **Additions**: New sources, tables, or columns.
//! - **Modifications**: Changes to URLs, types, or partition columns.
//! - **Deletions**: Items present in the DB but missing from local config.

use super::helpers::{parse_yaml, DiffChange, DiffResult};
use crate::models;
use crate::{
    db,
    output::{self, OutputFormat},
};
use anyhow::Result;
use owo_colors::OwoColorize;
use tokio_postgres::Client;

pub async fn diff(client: &Client, file_path: &str, format: OutputFormat) -> Result<()> {
    let result = diff_internal(client, file_path).await?;
    if format.is_machine_readable() {
        output::print_success(format, &result)?;
    } else {
        print_diff_human(&result);
    }
    Ok(())
}

pub(crate) fn print_diff_human(result: &DiffResult) {
    if result.changes.is_empty() {
        println!(
            "{}",
            "No changes detected (schema matches metadata store).".dimmed()
        );
        return;
    }

    println!("\nDetected {} change(s).", result.changes.len().yellow());
    for change in &result.changes {
        let symbol = match change.change_type.as_str() {
            "ADD" => "+".green().to_string(),
            "DELETE" => "-".red().to_string(),
            "MODIFY" => "~".yellow().to_string(),
            _ => "?".dimmed().to_string(),
        };

        match change.category.as_str() {
            "source" => {
                let details = change.details.as_deref().unwrap_or("");
                println!(
                    "{} {} source: {} {}",
                    symbol,
                    change.change_type,
                    change.name.bold(),
                    details
                );
            }
            "table" => {
                let details = change.details.as_deref().unwrap_or("");
                println!(
                    "  {} {} table: {} {}",
                    symbol,
                    change.change_type,
                    change.name.bold(),
                    details
                );
            }
            "column" => {
                let details = change.details.as_deref().unwrap_or("");
                println!(
                    "    {} {} column: {} {}",
                    symbol,
                    change.change_type,
                    change.name.bold(),
                    details
                );
            }
            _ => {}
        }
    }
}

pub(crate) async fn diff_internal(client: &Client, file_path: &str) -> Result<DiffResult> {
    let local_config = parse_yaml(file_path)?;
    let domain = local_config.domain.as_deref();
    let db_config = db::get_all_sources(client, domain).await?;

    let mut changes = Vec::new();

    // Compare sources
    for local_source in &local_config.sources {
        match db_config
            .sources
            .iter()
            .find(|s| s.name == local_source.name)
        {
            None => {
                changes.push(DiffChange {
                    change_type: "ADD".to_string(),
                    category: "source".to_string(),
                    name: local_source.name.clone(),
                    details: None,
                });
            }
            Some(db_source) => {
                changes.extend(diff_sources(local_source, db_source));
            }
        }
    }

    // Find deletions
    for db_source in &db_config.sources {
        if !local_config
            .sources
            .iter()
            .any(|s| s.name == db_source.name)
        {
            changes.push(DiffChange {
                change_type: "DELETE".to_string(),
                category: "source".to_string(),
                name: db_source.name.clone(),
                details: None,
            });
        }
    }

    Ok(DiffResult { changes })
}

pub(crate) fn diff_sources(
    local: &models::SourceConfig,
    db: &models::SourceConfig,
) -> Vec<DiffChange> {
    let mut changes = Vec::new();

    if local.source_type != db.source_type {
        changes.push(DiffChange {
            change_type: "MODIFY".to_string(),
            category: "source".to_string(),
            name: local.name.clone(),
            details: Some(format!("type: {} -> {}", db.source_type, local.source_type)),
        });
    }

    if local.url != db.url {
        changes.push(DiffChange {
            change_type: "MODIFY".to_string(),
            category: "source".to_string(),
            name: local.name.clone(),
            details: Some(format!("url: {:?} -> {:?}", db.url, local.url)),
        });
    }

    // Tables
    for local_table in &local.tables {
        match db
            .tables
            .iter()
            .find(|t| t.name == local_table.name && t.schema == local_table.schema)
        {
            None => {
                changes.push(DiffChange {
                    change_type: "ADD".to_string(),
                    category: "table".to_string(),
                    name: format!("{}.{}", local_table.schema, local_table.name),
                    details: None,
                });
            }
            Some(db_table) => {
                changes.extend(diff_tables(local_table, db_table));
            }
        }
    }

    for db_table in &db.tables {
        if !local
            .tables
            .iter()
            .any(|t| t.name == db_table.name && t.schema == db_table.schema)
        {
            changes.push(DiffChange {
                change_type: "DELETE".to_string(),
                category: "table".to_string(),
                name: format!("{}.{}", db_table.schema, db_table.name),
                details: None,
            });
        }
    }

    changes
}

pub(crate) fn diff_tables(
    local: &models::TableConfig,
    db: &models::TableConfig,
) -> Vec<DiffChange> {
    let mut changes = Vec::new();

    if local.partition_column != db.partition_column {
        changes.push(DiffChange {
            change_type: "MODIFY".to_string(),
            category: "table".to_string(),
            name: format!("{}.{}", local.schema, local.name),
            details: Some(format!(
                "partition_column: {:?} -> {:?}",
                db.partition_column, local.partition_column
            )),
        });
    }

    // Columns
    for local_col in &local.columns {
        match db.columns.iter().find(|c| c.name == local_col.name) {
            None => {
                changes.push(DiffChange {
                    change_type: "ADD".to_string(),
                    category: "column".to_string(),
                    name: local_col.name.clone(),
                    details: None,
                });
            }
            Some(db_col) => {
                if local_col.data_type != db_col.data_type {
                    changes.push(DiffChange {
                        change_type: "MODIFY".to_string(),
                        category: "column".to_string(),
                        name: local_col.name.clone(),
                        details: Some(format!(
                            "type: {} -> {}",
                            db_col.data_type, local_col.data_type
                        )),
                    });
                }
            }
        }
    }

    for db_col in &db.columns {
        if !local.columns.iter().any(|c| c.name == db_col.name) {
            changes.push(DiffChange {
                change_type: "DELETE".to_string(),
                category: "column".to_string(),
                name: db_col.name.clone(),
                details: None,
            });
        }
    }

    changes
}
