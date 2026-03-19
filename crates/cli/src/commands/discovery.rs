//! # Discovery Commands
//!
//! This module provides commands for discovering, searching, and importing data sources.
//!
//! ## Overview
//!
//! These commands allow users to interact with upstream data sources through the
//! Strake API to discover existing tables and automatically add them to their
//! local project configuration.
//!
//! ## Security
//!
//! All discovery operations require a valid Strake API token.

use super::helpers::{AddResult, SearchResult, get_client, parse_yaml};
use crate::config::CliConfig;
use crate::exit_codes;
use crate::output::{self, OutputFormat};
use crate::secrets::ResolverContext;
use anyhow::{Context, Result, anyhow};
use owo_colors::OwoColorize;
use std::fs;

pub async fn search(
    source: &str,
    file_path: &str,
    domain: Option<&str>,
    format: OutputFormat,
    config: &CliConfig,
    _ctx: &ResolverContext,
) -> Result<i32> {
    let domain = domain.unwrap_or("default");

    if !format.is_machine_readable() {
        println!(
            "{} {} {} source '{}' [Domain: {}]...",
            "[Config:".dimmed(),
            file_path.yellow(),
            "] Searching for tables in".bold().cyan(),
            source.bold(),
            domain.bold()
        );
    }

    let client = get_client(config)?;
    let api_url = &config.api_url;

    let url = format!("{}/introspect/{}/{}", api_url, domain, source);
    let response = client
        .get(&url)
        .send()
        .await
        .context("Failed to connect to Strake API. Ensure the server is running.")?;

    if !response.status().is_success() {
        return Err(anyhow!("Search failed: {}", response.text().await?));
    }

    let tables: Vec<strake_common::models::TableDiscovery> = response.json().await?;

    if format.is_machine_readable() {
        output::print_success(
            format,
            SearchResult {
                source: source.to_string(),
                domain: domain.to_string(),
                tables,
            },
        )?;
        return Ok(exit_codes::EXIT_OK);
    }

    println!("\n{}", "DISCOVERED TABLES:".bold().underline());
    println!("{:<20} {:<20}", "SCHEMA".bold(), "NAME".bold());
    println!("{}", "-".repeat(40).dimmed());
    for table in tables {
        println!("{:<20} {:<20}", table.schema, table.name);
    }

    println!(
        "\nUse 'strake-cli add {} <schema>.<name>' to import a specific table.",
        source
    );
    Ok(exit_codes::EXIT_OK)
}

/// Imports specific tables from a source into the local project.
///
/// This command fetches the schema for each table and appends it to the project's
/// `sources.yaml` file. It merges with existing source definitions if present.
pub async fn add(
    source: &str,
    table_full_name: &str,
    domain: Option<&str>,
    output_path: &str,
    format: OutputFormat,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    let domain = domain.unwrap_or("default");

    if !format.is_machine_readable() {
        println!(
            "{} {} {} table '{}' from source '{}' into domain '{}'...",
            "[Config:".dimmed(),
            output_path.yellow(),
            "] Importing".bold().cyan(),
            table_full_name.bold(),
            source.bold(),
            domain.bold()
        );
    }

    let client = get_client(config)?;
    let api_url = &config.api_url;

    let url = format!("{}/introspect/{}/{}/tables", api_url, domain, source);
    let response = client
        .post(&url)
        .json(&vec![table_full_name.to_string()])
        .send()
        .await
        .context("Failed to connect to Strake API.")?;

    if !response.status().is_success() {
        return Err(anyhow!("Import failed: {}", response.text().await?));
    }

    let imported_config: strake_common::models::SourcesConfig = response.json().await?;

    // Append to local sources.yaml
    let mut current_config = if std::path::Path::new(output_path).exists() {
        parse_yaml(output_path, ctx).await?
    } else {
        strake_common::models::SourcesConfig {
            domain: Some(domain.into()),
            sources: vec![],
        }
    };

    // simplified merge logic: find source, merge tables
    if let Some(imported_source) = imported_config.sources.first() {
        if let Some(target_source) = current_config.sources.iter_mut().find(|s| s.name == source) {
            // Update URL if missing or if imported one exists
            if target_source.url.is_none() && imported_source.url.is_some() {
                target_source.url = imported_source.url.clone();
            }

            for t in &imported_source.tables {
                if let Some(existing_table) = target_source
                    .tables
                    .iter_mut()
                    .find(|existing| existing.name == t.name && existing.schema == t.schema)
                {
                    // Update columns if existing table has none
                    if existing_table.columns.is_empty() {
                        existing_table.columns = t.columns.clone();
                    }
                } else {
                    target_source.tables.push(t.clone());
                }
            }
        } else {
            // Source doesn't exist in config, add it entirely
            current_config.sources.push(imported_source.clone());
        }
    }

    let yaml = serde_yaml::to_string(&current_config)?;
    fs::write(output_path, yaml)?;

    if format.is_machine_readable() {
        output::print_success(
            format,
            AddResult {
                source: source.to_string(),
                table: table_full_name.to_string(),
                domain: domain.to_string(),
                output_file: output_path.to_string(),
            },
        )?;
    } else {
        println!(
            "{} Successfully added '{}' to {}.",
            "✔".green(),
            table_full_name.bold(),
            output_path.yellow()
        );
    }
    Ok(exit_codes::EXIT_OK)
}
