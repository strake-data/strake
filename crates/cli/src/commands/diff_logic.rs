//! # Diff Command
//!
//! Provides a dry-run preview of changes applied to the overarching metadata store.
//!
//! ## Overview
//!
//! It compares the local `sources.yaml` against the current state in the database
//! for the specified domain. It detects structural additions, modifications, and deletions.
//!
//! ## Usage
//!
//! ```ignore
//! // diff(&store, file_path, format).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Loads full source and database state into memory for synchronous nested loop comparison.
//! Expects configuration sizes in the MBs max.
//!
//! ## Safety
//!
//! No `unsafe` in this module.
//!
//! ## References
//!
//! - Relevant GitHub Diff Issue

use super::helpers::{DiffChange, DiffResult, parse_yaml};
use crate::models;
use crate::{
    exit_codes,
    metadata::MetadataStore,
    output::{self, OutputFormat},
    secrets::ResolverContext,
};
use anyhow::Result;
use owo_colors::OwoColorize;
use std::fs;

#[derive(Debug, Clone)]
pub struct DiffOptions {
    pub file: String,
    pub impact: bool,
    pub format: OutputFormat,
}

/// Compares the local configuration against the metadata store and prints the diff.
pub async fn diff(
    store: &dyn MetadataStore,
    opts: DiffOptions,
    ctx: &ResolverContext,
) -> Result<i32> {
    let result = diff_internal(store, &opts.file, ctx).await?;

    if opts.impact {
        let sources = parse_yaml(&opts.file, ctx).await?;
        // Load contracts.yaml
        let contracts_file = std::path::Path::new(&opts.file)
            .parent()
            .unwrap()
            .join("contracts.yaml");
        let contracts: strake_common::models::ContractsConfig = if contracts_file.exists() {
            let content = fs::read_to_string(&contracts_file)?;
            serde_yaml::from_str(&content)?
        } else {
            let mut c = strake_common::models::ContractsConfig::default();
            c.contracts = vec![];
            c
        };

        let graph = crate::impact::ReferenceGraph::build(&sources, &contracts);
        let impact_record = graph.impact_of(&result.changes, &sources);

        if !impact_record.affected.is_empty() {
            println!("\n{}", "Semantic Impact Analysis:".bold().yellow());
            for aff in &impact_record.affected {
                println!(
                    "  {} {} '{}' (Severity: {:?})",
                    "⚠".yellow(),
                    match aff.kind {
                        crate::impact::AffectedKind::Contract => "Contract",
                        crate::impact::AffectedKind::Policy => "Policy",
                    },
                    aff.entity,
                    aff.severity
                );
            }
        }
    }

    if opts.format.is_machine_readable() {
        output::print_success(opts.format, &result)?;
    } else {
        print_diff_human(&result);
    }

    if result.changes.is_empty() {
        Ok(exit_codes::EXIT_OK)
    } else {
        Ok(exit_codes::EXIT_WARNINGS)
    }
}

pub(crate) fn print_diff_human(result: &DiffResult) {
    if result.changes.is_empty() {
        println!("{}", "No changes detected.".green());
        return;
    }

    println!("{}", "Proposed Changes:".bold().cyan());
    for change in &result.changes {
        let symbol = match change.change_type {
            super::helpers::ChangeType::Added => "+".green().to_string(),
            super::helpers::ChangeType::Deleted => "-".red().to_string(),
            super::helpers::ChangeType::Modified => "~".yellow().to_string(),
        };

        println!(
            "{} {} {}",
            symbol,
            change.change_type.to_string().bold(),
            change.path.bold()
        );

        if let Some(prev) = &change.previous
            && let Some(curr) = &change.current
        {
            println!("    {} -> {}", prev.dimmed(), curr.dimmed());
        }
    }
}

pub(crate) async fn diff_internal(
    store: &dyn MetadataStore,
    file_path: &str,
    ctx: &ResolverContext,
) -> Result<DiffResult> {
    let local_config = parse_yaml(file_path, ctx).await?;
    let domain_name = local_config
        .domain
        .clone()
        .unwrap_or_else(|| strake_common::models::DomainName::from("default"));
    let db_config = store.get_sources(&domain_name).await.unwrap_or_default();

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
                    change_type: super::helpers::ChangeType::Added,
                    path: format!("sources[{}]", local_source.name),
                    previous: None,
                    current: None,
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
                change_type: super::helpers::ChangeType::Deleted,
                path: format!("sources[{}]", db_source.name),
                previous: None,
                current: None,
            });
        }
    }

    Ok(DiffResult {
        domain: domain_name,
        changes,
    })
}

pub(crate) fn diff_sources(
    local: &models::SourceConfig,
    db: &models::SourceConfig,
) -> Vec<DiffChange> {
    let mut changes = Vec::new();

    if local.source_type != db.source_type {
        changes.push(DiffChange {
            change_type: super::helpers::ChangeType::Modified,
            path: format!("sources[{}].type", local.name),
            previous: Some(db.source_type.to_string()),
            current: Some(local.source_type.to_string()),
        });
    }

    if local.url != db.url {
        changes.push(DiffChange {
            change_type: super::helpers::ChangeType::Modified,
            path: format!("sources[{}].url", local.name),
            previous: db.url.clone(),
            current: local.url.clone(),
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
                    change_type: super::helpers::ChangeType::Added,
                    path: format!(
                        "sources[{}].tables[{}.{}]",
                        local.name.as_ref(),
                        local_table.schema,
                        local_table.name
                    ),
                    previous: None,
                    current: None,
                });
            }
            Some(db_table) => {
                changes.extend(diff_tables(local.name.as_ref(), local_table, db_table));
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
                change_type: super::helpers::ChangeType::Deleted,
                path: format!(
                    "sources[{}].tables[{}.{}]",
                    local.name, db_table.schema, db_table.name
                ),
                previous: None,
                current: None,
            });
        }
    }

    changes
}

pub(crate) fn diff_tables(
    source_name: &str,
    local: &models::TableConfig,
    db: &models::TableConfig,
) -> Vec<DiffChange> {
    let mut changes = Vec::new();

    if local.partition_column != db.partition_column {
        changes.push(DiffChange {
            change_type: super::helpers::ChangeType::Modified,
            path: format!(
                "sources[{}].tables[{}.{}].partition_column",
                source_name, local.schema, local.name
            ),
            previous: db.partition_column.clone(),
            current: local.partition_column.clone(),
        });
    }

    // Columns
    for local_col in &local.column_definitions {
        match db
            .column_definitions
            .iter()
            .find(|c| c.name == local_col.name)
        {
            None => {
                changes.push(DiffChange {
                    change_type: super::helpers::ChangeType::Added,
                    path: format!(
                        "sources[{}].tables[{}.{}].column_definitions[{}]",
                        source_name, local.schema, local.name, local_col.name
                    ),
                    previous: None,
                    current: None,
                });
            }
            Some(db_col) => {
                if local_col.data_type != db_col.data_type {
                    changes.push(DiffChange {
                        change_type: super::helpers::ChangeType::Modified,
                        path: format!(
                            "sources[{}].tables[{}.{}].column_definitions[{}].type",
                            source_name, local.schema, local.name, local_col.name
                        ),
                        previous: Some(db_col.data_type.clone()),
                        current: Some(local_col.data_type.clone()),
                    });
                }
            }
        }
    }

    for db_col in &db.column_definitions {
        if !local
            .column_definitions
            .iter()
            .any(|c| c.name == db_col.name)
        {
            changes.push(DiffChange {
                change_type: super::helpers::ChangeType::Deleted,
                path: format!(
                    "sources[{}].tables[{}.{}].column_definitions[{}]",
                    source_name, local.schema, local.name, db_col.name
                ),
                previous: None,
                current: None,
            });
        }
    }

    changes
}
