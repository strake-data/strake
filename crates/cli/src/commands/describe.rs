//! Describe and test_connection commands.
//!
//! # Overview
//! Useful for debugging and understanding the current state.
//!
//! # Commands
//! - **describe**: Shows a detailed view of the *current configuration* stored in the metadata DB.
//!   This is the "source of truth" for the server.
//! - **test_connection**: Reads the *local* `sources.yaml` and attempts to connect to every defined
//!   source to verify credentials and network reachability. Useful before running `apply`.

use super::helpers::{TestConnectionResult, TestConnectionSummary};
use super::validate::validate_source;
use crate::config::CliConfig;
use crate::{
    exit_codes,
    metadata::MetadataStore,
    output::{self, OutputFormat},
    secrets::ResolverContext,
};
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use strake_common::models::DomainName;

pub async fn describe(
    store: &dyn MetadataStore,
    file_path: &str,
    domain: Option<&str>,
    format: OutputFormat,
    _ctx: &ResolverContext,
) -> Result<i32> {
    let domain_name = DomainName::from(domain.unwrap_or("default"));

    if !format.is_machine_readable() {
        println!(
            "{} {} {} domain '{}':",
            "[Config:".dimmed(),
            file_path.yellow(),
            "] Current Configuration in Metadata Store for"
                .bold()
                .cyan(),
            domain_name.bold()
        );
    }
    let config = store.get_sources(&domain_name).await?;

    if format.is_machine_readable() {
        output::print_success(format, &config)?;
        return Ok(exit_codes::EXIT_OK);
    }

    if config.sources.is_empty() {
        println!("No sources configured.");
        return Ok(exit_codes::EXIT_OK);
    }

    for source in config.sources {
        println!(
            "{} {} (Type: {})",
            "Source:".bold().blue(),
            source.name.bold(),
            source.source_type.dimmed()
        );
        if let Some(url) = &source.url {
            println!("  {} {}", "URL:".dimmed(), url);
        }
        for table in source.tables {
            println!(
                "  {} {}.{}",
                "Table:".bold().cyan(),
                table.schema.dimmed(),
                table.name.bold()
            );
            if let Some(pc) = &table.partition_column {
                println!(
                    "    {} {}",
                    "Partition Column:".dimmed(),
                    pc.as_str().yellow()
                );
            }
            println!("    {}", "Columns:".dimmed());
            for col in table.column_definitions {
                let mut attribs = Vec::new();
                if col.primary_key {
                    attribs.push("PK".red().to_string());
                }
                if col.unique {
                    attribs.push("UNIQUE".yellow().to_string());
                }
                if col.not_null {
                    attribs.push("NOT NULL".dimmed().to_string());
                }

                let attrib_str = if attribs.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", attribs.join(", "))
                };

                println!(
                    "      {} {}: {}{}",
                    "•".cyan(),
                    col.name.bold(),
                    col.data_type,
                    attrib_str
                );
            }
        }
        println!();
    }
    Ok(exit_codes::EXIT_OK)
}

pub async fn test_connection(
    file_path: &str,
    format: OutputFormat,
    _config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    if !format.is_machine_readable() {
        println!(
            "{} {} {}",
            "[Config:".dimmed(),
            file_path.yellow(),
            "] Testing connections...".bold().cyan()
        );
    }
    let config = super::helpers::parse_yaml(file_path, ctx).await?;

    let mut results = Vec::new();

    for source in config.sources {
        if !format.is_machine_readable() {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .context("Failed to compile progress bar template")?,
            );
            pb.set_message(format!("Testing source '{}'...", source.name));
            pb.enable_steady_tick(std::time::Duration::from_millis(100));

            match validate_source(&source).await {
                Ok(_) => {
                    pb.finish_with_message(format!("{} source '{}': OK", "✔".green(), source.name));
                    results.push(TestConnectionResult {
                        source: source.name.clone(),
                        valid: true,
                        error: None,
                    });
                }
                Err(e) => {
                    pb.finish_with_message(format!(
                        "{} source '{}': FAILED - {}",
                        "✘".red(),
                        source.name,
                        e
                    ));
                    results.push(TestConnectionResult {
                        source: source.name.clone(),
                        valid: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        } else {
            match validate_source(&source).await {
                Ok(_) => {
                    results.push(TestConnectionResult {
                        source: source.name.clone(),
                        valid: true,
                        error: None,
                    });
                }
                Err(e) => {
                    results.push(TestConnectionResult {
                        source: source.name.clone(),
                        valid: false,
                        error: Some(e.to_string()),
                    });
                }
            }
        }
    }

    if format.is_machine_readable() {
        let has_failures = results.iter().any(|r| !r.valid);
        let summary = TestConnectionSummary { results };

        if has_failures {
            output::print_error(
                format,
                "One or more connection tests failed",
                exit_codes::EXIT_ERROR,
                Some(summary),
            )?;
            return Ok(exit_codes::EXIT_ERROR);
        } else {
            output::print_success(format, summary)?;
        }
    }

    Ok(exit_codes::EXIT_OK)
}
