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

use super::{
    ai::{AiProviderRegistry, AnthropicProvider, GeminiProvider},
    helpers::{SearchResult, get_client, parse_yaml},
};
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

#[derive(Debug, Clone)]
pub struct AddOptions {
    pub source: String,
    pub table: Option<String>,
    pub file: String,
    pub full: bool,
    pub ai_descriptions: bool,
    pub to_contracts: bool,
    pub merge: bool,
    pub overwrite: bool,
    pub pattern: Option<String>,
    pub all: bool,
    pub stdin: bool,
    pub yes: bool,
    pub dry_run: bool,
    pub format: OutputFormat,
}

/// Imports specific tables from a source into the local project.
pub async fn add(options: AddOptions, config: &CliConfig, ctx: &ResolverContext) -> Result<i32> {
    if options.all || options.pattern.is_some() || options.stdin {
        return bulk_add(options, config, ctx).await;
    }

    let table_full_name = options.table.as_ref().context("Table name is required")?;
    let parts: Vec<&str> = table_full_name.split('.').collect();
    if parts.len() != 2 {
        return Err(anyhow!(
            "Invalid table name format. Expected 'schema.table'"
        ));
    }
    let table_ref = strake_connectors::introspect::TableRef {
        schema: parts[0].to_string(),
        table: parts[1].to_string(),
    };

    if !options.format.is_machine_readable() {
        println!(
            "{} {} {} table '{}' from source '{}'...",
            "[Config:".dimmed(),
            options.file.yellow(),
            "] Introspecting".bold().cyan(),
            table_full_name.bold(),
            options.source.bold()
        );
    }

    // Resolve introspector
    let introspector = resolve_introspector(&options.source, &options.file, config, ctx).await?;
    let mut introspected = introspector
        .introspect_table(&table_ref, options.full)
        .await
        .map_err(|e| anyhow!("Introspection failed: {}", e))?;

    // AI Enrichment
    if options.ai_descriptions {
        if !options.format.is_machine_readable() {
            println!("  {} Generating AI descriptions...", "✨".yellow());
        }
        let mut registry = AiProviderRegistry::new();

        // Register default providers
        if let Ok(key) = std::env::var("GOOGLE_API_KEY") {
            registry.register(
                "gemini",
                Box::new(GeminiProvider {
                    api_key: key,
                    model: std::env::var("STRAKE_AI_MODEL")
                        .unwrap_or_else(|_| "gemini-1.5-pro-latest".to_string()),
                }),
            );
        }
        if let Ok(_key) = std::env::var("ANTHROPIC_API_KEY") {
            registry.register(
                "anthropic",
                Box::new(AnthropicProvider {
                    _api_key: std::env::var("ANTHROPIC_API_KEY")
                        .unwrap_or_else(|_| "dummy".to_string()),
                    _model: std::env::var("STRAKE_AI_MODEL")
                        .unwrap_or_else(|_| "claude-3-5-sonnet-latest".to_string()),
                }),
            );
        }

        match registry.resolve_from_env() {
            Ok(provider) => {
                if let Err(e) = provider.enrich_descriptions(&mut introspected).await {
                    eprintln!(
                        "Warning: AI description enrichment failed: {}. Proceeding without them.",
                        e
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "Warning: No AI provider configured: {}. Proceeding without them.",
                    e
                );
            }
        }
    }

    if options.dry_run {
        println!("Dry run: would add/merge table '{}'", table_full_name);
        return Ok(exit_codes::EXIT_OK);
    }

    // Load existing config
    let mut current_config = if std::path::Path::new(&options.file).exists() {
        parse_yaml(&options.file, ctx).await?
    } else {
        strake_common::models::SourcesConfig {
            domain: None,
            sources: vec![],
        }
    };

    merge_introspected(&mut current_config, &options, introspected.clone())?;

    // Atomic write
    let mut yaml = serde_yaml::to_string(&current_config)?;

    // Post-process to add # ai-generated comments
    yaml = yaml.replace(" [AI_GEN]", " # ai-generated");

    fs::write(&options.file, yaml)?;

    if options.to_contracts {
        promote_to_contracts(&introspected, &options, ctx).await?;
    }

    if !options.format.is_machine_readable() {
        println!(
            "{} Successfully updated '{}' in {}.",
            "✔".green(),
            table_full_name.bold(),
            options.file.yellow()
        );
    }

    Ok(exit_codes::EXIT_OK)
}

async fn promote_to_contracts(
    introspected: &strake_common::schema::IntrospectedTable,
    options: &AddOptions,
    _ctx: &ResolverContext,
) -> Result<()> {
    let contracts_file = std::path::Path::new(&options.file)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("contracts.yaml");

    let mut contracts_config = if contracts_file.exists() {
        let content = fs::read_to_string(&contracts_file)?;
        serde_yaml::from_str(&content)?
    } else {
        strake_common::models::ContractsConfig { contracts: vec![] }
    };

    let table_full_name = format!("{}.{}", introspected.schema, introspected.name);

    let new_contract = strake_common::models::Contract {
        table: table_full_name.clone(),
        strict: false, // Default from spec D7
        columns: introspected
            .columns
            .iter()
            .map(|col| {
                let mut constraints = Vec::new();
                for c in &col.constraints {
                    if let strake_common::schema::ColumnConstraint::ContractRule(rule) = c {
                        match rule {
                            strake_common::schema::ContractRuleKind::Gt { value } => {
                                constraints.push(strake_common::models::Constraint {
                                    constraint_type: "gt".to_string(),
                                    value: value.clone(),
                                });
                            }
                            strake_common::schema::ContractRuleKind::Gte { value } => {
                                constraints.push(strake_common::models::Constraint {
                                    constraint_type: "gte".to_string(),
                                    value: value.clone(),
                                });
                            }
                            strake_common::schema::ContractRuleKind::Lt { value } => {
                                constraints.push(strake_common::models::Constraint {
                                    constraint_type: "lt".to_string(),
                                    value: value.clone(),
                                });
                            }
                            strake_common::schema::ContractRuleKind::Lte { value } => {
                                constraints.push(strake_common::models::Constraint {
                                    constraint_type: "lte".to_string(),
                                    value: value.clone(),
                                });
                            }
                            _ => {}
                        }
                    }
                }
                strake_common::models::ContractColumn {
                    name: col.name.clone(),
                    data_type: col.type_str.clone(),
                    nullable: Some(col.nullable),
                    constraints,
                }
            })
            .collect(),
    };

    if let Some(existing) = contracts_config
        .contracts
        .iter_mut()
        .find(|c| c.table == table_full_name)
    {
        // Idempotency check: if identical, skip.
        // For simplicity, let's check if strictly equal.
        let existing_yaml = serde_yaml::to_string(&existing)?;
        let new_yaml = serde_yaml::to_string(&new_contract)?;
        if existing_yaml == new_yaml {
            return Ok(());
        }

        if !options.yes {
            println!(
                "Contract for '{}' already exists and differs. Overwrite? [y/N]",
                table_full_name
            );
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            if !input.trim().eq_ignore_ascii_case("y") {
                println!("Skipped contract update for '{}'.", table_full_name);
                return Ok(());
            }
        }
        *existing = new_contract;
    } else {
        contracts_config.contracts.push(new_contract);
    }

    let yaml = serde_yaml::to_string(&contracts_config)?;
    fs::write(&contracts_file, yaml)?;

    if !options.format.is_machine_readable() {
        println!(
            "{} Successfully promoted '{}' to {}.",
            "✔".green(),
            table_full_name.bold(),
            contracts_file.display().to_string().yellow()
        );
    }

    Ok(())
}

async fn bulk_add(options: AddOptions, config: &CliConfig, ctx: &ResolverContext) -> Result<i32> {
    let introspector = resolve_introspector(&options.source, &options.file, config, ctx).await?;

    let mut table_refs = Vec::new();
    if options.all {
        table_refs = introspector
            .list_tables(None)
            .await
            .map_err(|e| anyhow!("Failed to list tables: {}", e))?;
    } else if let Some(pattern) = &options.pattern {
        let matcher = globset::Glob::new(pattern)?.compile_matcher();
        table_refs = introspector
            .list_tables(Some(&matcher))
            .await
            .map_err(|e| anyhow!("Failed to list tables with pattern: {}", e))?;
    } else if options.stdin {
        use std::io::{BufRead, stdin};
        for line in stdin().lock().lines() {
            let line = line?;
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.trim().split('.').collect();
            if parts.len() == 2 {
                table_refs.push(strake_connectors::introspect::TableRef {
                    schema: parts[0].to_string(),
                    table: parts[1].to_string(),
                });
            } else {
                eprintln!("Warning: invalid table name '{}' in stdin", line);
            }
        }
    }

    if table_refs.is_empty() {
        println!("No tables found matching criteria.");
        return Ok(exit_codes::EXIT_OK);
    }

    if table_refs.len() > 10 && !options.yes {
        println!("Found {} tables. Continue? [y/N]", table_refs.len());
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(exit_codes::EXIT_OK);
        }
    }

    // Load existing config once
    let mut current_config = if std::path::Path::new(&options.file).exists() {
        parse_yaml(&options.file, ctx).await?
    } else {
        strake_common::models::SourcesConfig {
            domain: None,
            sources: vec![],
        }
    };

    println!("Importing {} tables...", table_refs.len());

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(10));
    let introspector = std::sync::Arc::new(introspector);
    let mut join_handles = Vec::new();

    for table_ref in table_refs {
        let perm = semaphore.clone().acquire_owned().await?;
        let introspector = introspector.clone();
        let full = options.full;

        let handle = tokio::spawn(async move {
            let _perm = perm;
            let res = introspector.introspect_table(&table_ref, full).await;
            (table_ref, res)
        });
        join_handles.push(handle);
    }

    for handle in join_handles {
        let (table_ref, res) = handle.await?;
        print!("  {} ... ", table_ref);
        match res {
            Ok(introspected) => {
                merge_introspected(&mut current_config, &options, introspected)?;
                println!("{}", "done".green());
            }
            Err(e) => {
                println!("{} ({})", "failed".red(), e);
            }
        }
    }

    // Atomic write
    let yaml = serde_yaml::to_string(&current_config)?;
    fs::write(&options.file, yaml)?;

    Ok(exit_codes::EXIT_OK)
}

fn merge_introspected(
    config: &mut strake_common::models::SourcesConfig,
    options: &AddOptions,
    introspected: strake_common::schema::IntrospectedTable,
) -> Result<()> {
    // Find or create source
    let source = if let Some(s) = config.sources.iter_mut().find(|s| s.name == options.source) {
        s
    } else {
        config.sources.push(strake_common::models::SourceConfig {
            name: options.source.clone(),
            source_type: introspected.source.clone(),
            url: None, // TBD: How to get URL?
            username: None,
            max_concurrent_queries: None,
            password: None,
            default_limit: None,
            cache: None,
            tables: vec![],
            config: serde_json::Value::Object(Default::default()),
        });
        config.sources.last_mut().unwrap()
    };

    if options.overwrite {
        source
            .tables
            .retain(|t| !(t.name == introspected.name && t.schema == introspected.schema));
    }

    if let Some(existing_table) = source
        .tables
        .iter_mut()
        .find(|t| t.name == introspected.name && t.schema == introspected.schema)
    {
        if options.merge {
            for intro_col in introspected.columns {
                if let Some(existing_col) = existing_table
                    .columns
                    .iter_mut()
                    .find(|c| c.name == intro_col.name)
                {
                    // Update if bare type and new is precise
                    if !existing_col.data_type.contains('(') && intro_col.type_str.contains('(') {
                        existing_col.data_type = intro_col.type_str;
                    }
                    existing_col.not_null = !intro_col.nullable;
                    // Primary key / unique?
                    if intro_col.is_primary_key {
                        existing_col.primary_key = true;
                    }
                    // AI Description (merge only if missing)
                    if existing_col.description.is_none() {
                        existing_col.description =
                            intro_col.ai_description.map(|d| format!("{} [AI_GEN]", d));
                    }
                } else {
                    existing_table
                        .columns
                        .push(strake_common::models::ColumnConfig {
                            name: intro_col.name,
                            data_type: intro_col.type_str,
                            length: None,
                            primary_key: intro_col.is_primary_key,
                            unique: false,
                            not_null: !intro_col.nullable,
                            description: intro_col
                                .ai_description
                                .map(|d| format!("{} [AI_GEN]", d)),
                        });
                }
            }
        }
    } else {
        source.tables.push(strake_common::models::TableConfig {
            name: introspected.name,
            schema: introspected.schema,
            partition_column: None,
            columns: introspected
                .columns
                .into_iter()
                .map(|c| strake_common::models::ColumnConfig {
                    name: c.name,
                    data_type: c.type_str,
                    length: None,
                    primary_key: c.is_primary_key,
                    unique: false,
                    not_null: !c.nullable,
                    description: c.ai_description.map(|d| format!("{} [AI_GEN]", d)),
                })
                .collect(),
        });
    }

    Ok(())
}

async fn resolve_introspector(
    source_name: &str,
    file_path: &str,
    _config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<Box<dyn strake_connectors::introspect::SchemaIntrospector>> {
    let current_config = parse_yaml(file_path, ctx).await?;
    let source = current_config
        .sources
        .iter()
        .find(|s| s.name == source_name)
        .context(format!(
            "Source '{}' not found in {}",
            source_name, file_path
        ))?;

    match source.source_type.as_str() {
        "postgres" => {
            let conn_str = source
                .url
                .as_ref()
                .context("Postgres URL is required for introspection")?;
            Ok(Box::new(
                strake_connectors::sources::sql::postgres_introspect::PostgresIntrospector {
                    connection_string: conn_str.clone(),
                },
            ))
        }
        "duckdb" => {
            let db_path = source
                .url
                .as_ref()
                .context("DuckDB path is required for introspection")?;
            Ok(Box::new(
                strake_connectors::sources::sql::duckdb_introspect::DuckDBIntrospector {
                    db_path: db_path.clone(),
                },
            ))
        }
        _ => Err(anyhow!(
            "Source type '{}' does not support introspection yet",
            source.source_type
        )),
    }
}
