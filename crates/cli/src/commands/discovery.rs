//! # Discovery Command
//!
//! This module provides commands for discovering, searching, and importing data sources.
//!
//! ## Overview
//!
//! These commands allow users to interact with upstream data sources through the
//! Strake API to discover existing tables and automatically add them to their
//! local project configuration.
//!
//! ## Usage
//! ```bash
//! # Search for a table in the public schema
//! strake-cli search default public
//!
//! # Add a table and automatically generate AI descriptions
//! strake-cli add default public.alerts --ai-descriptions
//! ```
//!
//! ## Performance Characteristics
//!
//! Bulk introspection (`add --all`) uses concurrent Tokio tasks with a configurable
//! semaphore (default 10) to bound connection pressure against upstream systems.
//! Configuration files are persisted atomically to prevent corruption during writes.
//!
//! ## Errors
//!
//! The commands inside this module will return standard Error exit codes if introspection
//! queries fail, connection parameters are invalid, or if file I/O permissions deny
//! the atomic write operation into `sources.yaml`.
//!
//! ## Security
//!
//! All remote discovery operations require a valid Strake API token. Local operations
//! may require credentials for the introspected sources.

use super::{
    ai::AiProviderRegistry,
    helpers::{SearchResult, get_client, parse_yaml},
};
use crate::config::{AiConfig, CliConfig};
use crate::exit_codes;
use crate::output::{self, OutputFormat};
use crate::secrets::ResolverContext;
use anyhow::{Context, Result, anyhow};
use owo_colors::OwoColorize;
use secrecy::SecretString;
use strake_common::models::DomainName;

/// Performs a fuzzy search against all configured upstream sources to locate tables.
///
/// Output can be formatted visually or emitted as machine-readable JSON.
///
/// # Errors
/// Returns an error if the connection to the API fails or the search route returns 4xx/5xx.
///
/// # Examples
/// ```no_run
/// # use strake_cli::commands::discovery::search;
/// # use strake_cli::config::CliConfig;
/// # use strake_cli::output::OutputFormat;
/// # use strake_cli::secrets::ResolverContext;
/// # async fn run() {
/// search("public", "sources.yaml", None, OutputFormat::Json, &CliConfig::default(), &ResolverContext::new()).await.unwrap();
/// # }
/// ```
pub async fn search(
    source: &str,
    file_path: &str,
    domain: Option<&str>,
    format: OutputFormat,
    config: &CliConfig,
    _ctx: &ResolverContext,
) -> Result<i32> {
    let domain_str = domain.unwrap_or("default");

    if !format.is_machine_readable() {
        println!(
            "{} {} {} source '{}' [Domain: {}]...",
            "[Config:".dimmed(),
            file_path.yellow(),
            "] Searching for tables in".bold().cyan(),
            source.bold(),
            domain_str.bold()
        );
    }

    let domain_name = DomainName::from(domain_str);
    let client = get_client(config)?;
    let api_url = &config.api_url;

    let url = format!("{}/introspect/{}/{}", api_url, domain_name, source);
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
                source: source.parse().unwrap(),
                domain: domain.map(|d| d.parse().unwrap()),
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

/// Command-line options for adding or updating tables in the configuration.
#[derive(Debug, Clone)]
pub struct AddOptions {
    /// Source name containing the table.
    pub source: strake_common::models::SourceName,
    /// Schema-qualified table name (e.g. `public.users`). Required unless `--all` or `--stdin` is set.
    pub table: Option<String>,
    /// Path to the target configuration file (e.g. `sources.yaml`).
    pub file: String,
    /// Perform full deep introspection (may be slower on large data warehouses).
    pub full: bool,
    /// Connect to standard AI models to enrich table column comments.
    pub ai_descriptions: bool,
    /// Also promote the discovered schema cleanly into `contracts.yaml`.
    pub to_contracts: bool,
    /// Merge new introspection results over the top of the existing configuration.
    pub merge: bool,
    /// Replace the old table block with a fresh introspection sweep.
    pub overwrite: bool,
    /// Bulk target: Glob pattern of table names to include.
    pub pattern: Option<String>,
    /// Bulk target: Fetch all tables.
    pub all: bool,
    /// Bulk target: Fetch table list from STDIN.
    pub stdin: bool,
    /// Skip interactive confirmations.
    pub yes: bool,
    /// View the planned config manipulations without persisting them.
    pub dry_run: bool,
    /// Formatting mode for the output (Terminal/JSON).
    pub format: OutputFormat,
}

/// Imports a specific table from a source into the local project.
///
/// Dispatches to `bulk_add` seamlessly if bulk flags are detected.
///
/// # Errors
/// Returns an error if the table string is malformed or if introspection fails structurally.
///
/// # Panics
/// Does not panic in standard workflows.
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
            options.source.as_ref().bold()
        );
    }

    // Resolve introspector
    let introspector =
        resolve_introspector(options.source.as_ref(), &options.file, config, ctx).await?;
    let mut introspected = introspector
        .introspect_table(&table_ref, options.full)
        .await
        .map_err(|e| anyhow!("Introspection failed: {}", e))?;

    // AI Enrichment
    if options.ai_descriptions {
        if !options.format.is_machine_readable() {
            println!("  {} Generating AI descriptions...", "✨".yellow());
        }
        let provider_name = std::env::var("STRAKE_AI_PROVIDER")
            .ok()
            .or_else(|| config.ai.as_ref().and_then(|ai| ai.provider.clone()))
            .ok_or_else(|| {
                anyhow!(
                    "No AI provider configured. \
                 Set 'STRAKE_AI_PROVIDER' or add 'provider:' under 'ai:' in your config file."
                )
            })?;

        let ai_config_default = AiConfig::default();
        let mut registry = AiProviderRegistry::new();
        registry.register_from_env(
            config.ai.as_ref().unwrap_or(&ai_config_default),
            &provider_name,
            ctx,
        )?;

        let provider = registry.resolve(&provider_name)?;

        provider
            .enrich_descriptions(&mut introspected)
            .await
            .with_context(|| {
                format!(
                    "AI enrichment failed for table '{}.{}'",
                    introspected.schema, introspected.name
                )
            })?;
    }

    if options.dry_run {
        println!("Dry run: would add/merge table '{}'", table_full_name);
        return Ok(exit_codes::EXIT_OK);
    }

    // Load existing config
    let mut current_config = if std::path::Path::new(&options.file).exists() {
        parse_yaml(&options.file, ctx).await?
    } else {
        strake_common::models::SourcesConfig::default()
    };

    merge_introspected(&mut current_config, &options, introspected.clone())?;

    // Atomic write
    let mut yaml = serde_yaml::to_string(&current_config)?;

    // Post-process to add # ai-generated comments
    yaml = yaml.replace(" [AI_GEN]", " # ai-generated");

    let file_path = options.file.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut tmp = tempfile::NamedTempFile::new_in(
            std::path::Path::new(&file_path)
                .parent()
                .unwrap_or(std::path::Path::new(".")),
        )?;
        use std::io::Write;
        tmp.as_file_mut().write_all(yaml.as_bytes())?;
        tmp.persist(&file_path)?;
        Ok(())
    })
    .await??;

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
        let content = tokio::fs::read_to_string(&contracts_file).await?;
        serde_yaml::from_str(&content)?
    } else {
        let mut cfg = strake_common::models::ContractsConfig::default();
        cfg.contracts = vec![];
        cfg
    };

    let table_full_name = format!("{}.{}", introspected.schema, introspected.name);

    let mut new_contract = strake_common::models::Contract::default();
    new_contract.table = table_full_name.clone();
    new_contract.strict = false; // Default from spec D7
    new_contract.columns = introspected
        .columns
        .iter()
        .map(|col| {
            let mut constraints = Vec::new();
            for c in &col.constraints {
                if let strake_common::schema::ColumnConstraint::ContractRule(rule) = c {
                    match rule {
                        strake_common::schema::ContractRuleKind::Gt { value } => {
                            let mut ct = strake_common::models::Constraint::default();
                            ct.constraint_type = "gt".to_string();
                            ct.value = value.clone();
                            constraints.push(ct);
                        }
                        strake_common::schema::ContractRuleKind::Gte { value } => {
                            let mut ct = strake_common::models::Constraint::default();
                            ct.constraint_type = "gte".to_string();
                            ct.value = value.clone();
                            constraints.push(ct);
                        }
                        strake_common::schema::ContractRuleKind::Lt { value } => {
                            let mut ct = strake_common::models::Constraint::default();
                            ct.constraint_type = "lt".to_string();
                            ct.value = value.clone();
                            constraints.push(ct);
                        }
                        strake_common::schema::ContractRuleKind::Lte { value } => {
                            let mut ct = strake_common::models::Constraint::default();
                            ct.constraint_type = "lte".to_string();
                            ct.value = value.clone();
                            constraints.push(ct);
                        }
                        _ => {}
                    }
                }
            }
            let mut cc = strake_common::models::ContractColumn::default();
            cc.name = col.name.clone();
            cc.data_type = col.type_str.clone();
            cc.nullable = Some(col.nullable);
            cc.constraints = constraints;
            cc
        })
        .collect();

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
            let input = tokio::task::spawn_blocking(|| -> std::io::Result<String> {
                let mut s = String::new();
                std::io::stdin().read_line(&mut s)?;
                Ok(s)
            })
            .await??;
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
    let dest_file = contracts_file.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut tmp = tempfile::NamedTempFile::new_in(
            dest_file.parent().unwrap_or(std::path::Path::new(".")),
        )?;
        use std::io::Write;
        tmp.as_file_mut().write_all(yaml.as_bytes())?;
        tmp.persist(&dest_file)?;
        Ok(())
    })
    .await??;

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

/// Imports multiple tables concurrently from a source into the local project.
///
/// Honors concurrency limits to protect upstream databases from connection floods.
///
/// # Errors
/// Returns an error if wildcard lookup fails or atomic writes trigger IO failures.
///
/// # Panics
/// Panics if standard IO locks cannot be securely acquired during CLI prompts.
pub(crate) async fn bulk_add(
    options: AddOptions,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    let introspector =
        resolve_introspector(options.source.as_ref(), &options.file, config, ctx).await?;

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
        use tokio::io::AsyncBufReadExt;
        let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
        while let Some(line) = lines.next_line().await? {
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

    if options.dry_run {
        println!("Dry run: would add/merge {} tables:", table_refs.len());
        for t in &table_refs {
            println!("  - {}.{}", t.schema, t.table);
        }
        return Ok(exit_codes::EXIT_OK);
    }

    if table_refs.len() > 10 && !options.yes {
        println!("Found {} tables. Continue? [y/N]", table_refs.len());
        let input = tokio::task::spawn_blocking(|| -> std::io::Result<String> {
            let mut s = String::new();
            std::io::stdin().read_line(&mut s)?;
            Ok(s)
        })
        .await??;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(exit_codes::EXIT_OK);
        }
    }

    // Load existing config once
    let mut current_config = if std::path::Path::new(&options.file).exists() {
        parse_yaml(&options.file, ctx).await?
    } else {
        strake_common::models::SourcesConfig::default()
    };

    println!("Importing {} tables...", table_refs.len());

    let introspector = std::sync::Arc::new(introspector);
    let mut successfully_introspected =
        perform_introspections(introspector.clone(), table_refs, &options).await?;

    // AI Provider config
    let mut ai_context: Option<(AiProviderRegistry, String)> = None;

    if options.ai_descriptions {
        if !options.format.is_machine_readable() {
            println!("  {} Preparing AI defaults...", "✨".yellow());
        }
        let provider_name = std::env::var("STRAKE_AI_PROVIDER")
            .ok()
            .or_else(|| config.ai.as_ref().and_then(|ai| ai.provider.clone()))
            .ok_or_else(|| {
                anyhow!(
                    "No AI provider configured. \
                  Set 'STRAKE_AI_PROVIDER' or add 'provider:' under 'ai:' in your config file."
                )
            })?;

        let ai_config_default = AiConfig::default();
        let mut registry = AiProviderRegistry::new();
        registry.register_from_env(
            config.ai.as_ref().unwrap_or(&ai_config_default),
            &provider_name,
            ctx,
        )?;

        // Fail fast if provider cannot be resolved
        registry.resolve(&provider_name)?;

        ai_context = Some((registry, provider_name));
    }

    if !successfully_introspected.is_empty() {
        enrich_with_ai(&mut successfully_introspected, &ai_context).await?;
    }

    // Sort introspected results by schema and table name to ensure deterministic merge order
    successfully_introspected.sort_by(|a, b| a.schema.cmp(&b.schema).then(a.name.cmp(&b.name)));

    // Apply and write config updates
    merge_and_write_config(&mut current_config, &successfully_introspected, &options).await?;

    if options.to_contracts {
        for introspected in &successfully_introspected {
            promote_to_contracts(introspected, &options, ctx).await?;
        }
    }

    Ok(exit_codes::EXIT_OK)
}

/// Helper function to perform introspection on multiple tables concurrently.
async fn perform_introspections(
    introspector: std::sync::Arc<
        Box<dyn strake_connectors::introspect::SchemaIntrospector + Send + Sync>,
    >,
    table_refs: Vec<strake_connectors::introspect::TableRef>,
    options: &AddOptions,
) -> Result<Vec<strake_common::schema::IntrospectedTable>> {
    // Compile-time check that the introspector is Send + Sync
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    assert_send_sync(&introspector);

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(10));
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut tasks = FuturesUnordered::new();

    for table_ref in table_refs {
        let perm = semaphore.clone().acquire_owned().await?;
        let introspector = introspector.clone();
        let full = options.full;

        tasks.push(tokio::spawn(async move {
            let _perm = perm;
            let res = introspector.introspect_table(&table_ref, full).await;
            (table_ref, res)
        }));
    }

    let mut successfully_introspected = Vec::new();
    while let Some(res_task) = tasks.next().await {
        let (table_ref, res) = res_task?;
        print!("  {} ... ", table_ref);
        match res {
            Ok(introspected) => {
                successfully_introspected.push(introspected);
                println!("{}", "done".green());
            }
            Err(e) => {
                println!("{} ({})", "failed".red(), e);
            }
        }
    }
    Ok(successfully_introspected)
}

/// Helper function to enrich introspected tables with AI descriptions.
async fn enrich_with_ai(
    successfully_introspected: &mut [strake_common::schema::IntrospectedTable],
    ai_context: &Option<(AiProviderRegistry, String)>,
) -> Result<()> {
    if let Some((registry, name)) = ai_context {
        let provider = registry.resolve(name)?;
        for introspected in successfully_introspected {
            provider
                .enrich_descriptions(introspected)
                .await
                .with_context(|| {
                    format!(
                        "AI enrichment failed for table '{}.{}'",
                        introspected.schema, introspected.name
                    )
                })?;
        }
    }
    Ok(())
}

/// Helper function to merge introspections and write back to sources.yaml atomically.
async fn merge_and_write_config(
    current_config: &mut strake_common::models::SourcesConfig,
    successfully_introspected: &[strake_common::schema::IntrospectedTable],
    options: &AddOptions,
) -> Result<()> {
    // Apply config changes sequentially to prevent partial dirty states on error
    for introspected in successfully_introspected {
        merge_introspected(current_config, options, introspected.clone())?;
    }

    let mut yaml = serde_yaml::to_string(current_config)?;
    // Process line-by-line to properly format comments on lines with [AI_GEN]
    let mut processed_lines = Vec::new();
    for line in yaml.lines() {
        if line.contains(" [AI_GEN]") {
            let mut cleaned = line.replace(" [AI_GEN]", "");
            cleaned.push_str(" # ai-generated");
            processed_lines.push(cleaned);
        } else {
            processed_lines.push(line.to_string());
        }
    }
    yaml = processed_lines.join("\n") + "\n";

    let file_path = options.file.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut tmp = tempfile::NamedTempFile::new_in(
            std::path::Path::new(&file_path)
                .parent()
                .unwrap_or(std::path::Path::new(".")),
        )?;
        use std::io::Write;
        tmp.as_file_mut().write_all(yaml.as_bytes())?;
        tmp.persist(&file_path)?;
        Ok(())
    })
    .await??;
    Ok(())
}

fn merge_introspected(
    config: &mut strake_common::models::SourcesConfig,
    options: &AddOptions,
    introspected: strake_common::schema::IntrospectedTable,
) -> Result<()> {
    // Find or create source
    let source = if let Some(idx) = config.sources.iter().position(|s| s.name == options.source) {
        &mut config.sources[idx]
    } else {
        let mut sc = strake_common::models::SourceConfig::default();
        sc.name = options.source.clone();
        sc.source_type = strake_common::models::SourceType::Other(introspected.source.clone());
        sc.config = serde_json::Value::Object(Default::default());
        config.sources.push(sc);
        config
            .sources
            .last_mut()
            .expect("Last added source must exist")
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
        // Back-fill description only when the entry is currently absent
        existing_table.description.get_or_insert_with(|| {
            introspected
                .ai_description
                .as_deref()
                .map(|d| format!("{} [AI_GEN]", d))
                .or_else(|| introspected.db_comment.clone())
                .unwrap_or_default()
        });

        if options.merge {
            for intro_col in introspected.columns {
                match existing_table
                    .column_definitions
                    .iter_mut()
                    .find(|c| c.name == intro_col.name)
                {
                    Some(existing_col) => {
                        // Prefer the more precise type when the existing one lacks precision
                        if !existing_col.data_type.contains('(') && intro_col.type_str.contains('(')
                        {
                            existing_col.data_type = intro_col.type_str;
                        }
                        existing_col.not_null = !intro_col.nullable;
                        if intro_col.is_primary_key {
                            existing_col.primary_key = true;
                        }
                        // Back-fill AI description only when absent
                        existing_col.description.get_or_insert_with(|| {
                            intro_col
                                .ai_description
                                .map(|d| format!("{} [AI_GEN]", d))
                                .unwrap_or_default()
                        });
                    }
                    None => {
                        let mut col = strake_common::models::ColumnConfig::default();
                        col.name = intro_col.name;
                        col.data_type = intro_col.type_str;
                        col.primary_key = intro_col.is_primary_key;
                        col.not_null = !intro_col.nullable;
                        col.description =
                            intro_col.ai_description.map(|d| format!("{} [AI_GEN]", d));
                        existing_table.column_definitions.push(col);
                    }
                }
            }
        }
    } else {
        let mut t_cfg = strake_common::models::TableConfig::default();
        t_cfg.name = introspected.name;
        t_cfg.schema = introspected.schema;
        t_cfg.description = introspected
            .ai_description
            .map(|d| format!("{} [AI_GEN]", d))
            .or_else(|| introspected.db_comment.clone());
        t_cfg.column_definitions = introspected
            .columns
            .into_iter()
            .map(|c| {
                let mut col = strake_common::models::ColumnConfig::default();
                col.name = c.name;
                col.data_type = c.type_str;
                col.primary_key = c.is_primary_key;
                col.not_null = !c.nullable;
                col.description = c.ai_description.map(|d| format!("{} [AI_GEN]", d));
                col
            })
            .collect();
        source.tables.push(t_cfg);
    }

    Ok(())
}

pub(crate) async fn resolve_introspector(
    source_name: &str,
    file_path: &str,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<Box<dyn strake_connectors::introspect::SchemaIntrospector + Send + Sync>> {
    let current_config = if std::path::Path::new(file_path).exists() {
        parse_yaml(file_path, ctx).await.ok()
    } else {
        None
    };

    if let Some(source) = current_config
        .as_ref()
        .and_then(|c| c.sources.iter().find(|s| s.name.as_ref() == source_name))
    {
        match source.source_type.as_str() {
            "postgres" => {
                let conn_str_opt = source.url.clone().or_else(|| {
                    source
                        .config
                        .get("connection")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                });
                let conn_str = conn_str_opt
                    .context("Postgres URL/Connection string is required for introspection")?;
                return Ok(Box::new(
                    strake_connectors::sources::sql::postgres_introspect::PostgresIntrospector {
                        connection_string: SecretString::from(conn_str),
                    },
                ));
            }
            "sqlite" => {
                let db_path_opt = source.url.clone().or_else(|| {
                    source
                        .config
                        .get("connection")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                });
                let db_path = db_path_opt
                    .context("SQLite path/Connection string is required for introspection")?;
                let clean_path = db_path.strip_prefix("sqlite://").unwrap_or(&db_path);
                return Ok(Box::new(
                    strake_connectors::sources::sql::sqlite_introspect::SqliteIntrospector {
                        db_path: SecretString::from(clean_path.to_string()),
                    },
                ));
            }
            "duckdb" => {
                let db_path_opt = source.url.clone().or_else(|| {
                    source
                        .config
                        .get("connection")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                });
                let db_path = db_path_opt
                    .context("DuckDB path/Connection string is required for introspection")?;
                return Ok(Box::new(
                    strake_connectors::sources::sql::duckdb_introspect::DuckDBIntrospector {
                        db_path: SecretString::from(db_path),
                    },
                ));
            }
            _ => {}
        }
    }

    // Fallback to API-based introspection for unknown or missing sources
    Ok(Box::new(ApiIntrospector {
        api_url: config.api_url.clone(),
        domain: "default".to_string(), // TBD: resolve domain correctly
        source: source_name.to_string(),
        config: config.clone(),
    }))
}

/// Fallback introspector that queries the Strake API instead of a direct database connection.
pub struct ApiIntrospector {
    /// Strake API base URL.
    pub api_url: String,
    /// Tenant or domain name.
    pub domain: String,
    /// Upstream source name.
    pub source: String,
    /// Client configuration context.
    pub config: CliConfig,
}

#[async_trait::async_trait]
impl strake_connectors::introspect::SchemaIntrospector for ApiIntrospector {
    async fn list_tables(
        &self,
        _pattern: Option<&globset::GlobMatcher>,
    ) -> Result<
        Vec<strake_connectors::introspect::TableRef>,
        strake_connectors::introspect::IntrospectError,
    > {
        let client = get_client(&self.config).map_err(|e| {
            strake_connectors::introspect::IntrospectError::Connection(e.to_string())
        })?;

        let url = format!(
            "{}/introspect/{}/{}",
            self.api_url, self.domain, self.source
        );
        let response = client.get(&url).send().await.map_err(|e| {
            strake_connectors::introspect::IntrospectError::Connection(e.to_string())
        })?;

        if !response.status().is_success() {
            return Err(strake_connectors::introspect::IntrospectError::Query(
                format!("API search failed: {}", response.status()),
            ));
        }

        let tables: Vec<strake_common::models::TableDiscovery> =
            response.json().await.map_err(|e| {
                strake_connectors::introspect::IntrospectError::Serialization(e.to_string())
            })?;

        Ok(tables
            .into_iter()
            .map(|t| strake_connectors::introspect::TableRef {
                schema: t.schema,
                table: t.name,
            })
            .collect())
    }

    async fn introspect_table(
        &self,
        table: &strake_connectors::introspect::TableRef,
        _full: bool,
    ) -> Result<
        strake_common::schema::IntrospectedTable,
        strake_connectors::introspect::IntrospectError,
    > {
        let client = get_client(&self.config).map_err(|e| {
            strake_connectors::introspect::IntrospectError::Connection(e.to_string())
        })?;

        let url = format!(
            "{}/introspect/{}/{}/tables",
            self.api_url, self.domain, self.source
        );
        let response = client
            .post(&url)
            .json(&vec![format!("{}.{}", table.schema, table.table)])
            .send()
            .await
            .map_err(|e| {
                strake_connectors::introspect::IntrospectError::Connection(e.to_string())
            })?;

        if !response.status().is_success() {
            return Err(strake_connectors::introspect::IntrospectError::Query(
                format!("API introspection failed: {}", response.status()),
            ));
        }

        // The test indicates the API returns a SourcesConfig, not a raw IntrospectedTable.
        let response_data: strake_common::models::SourcesConfig =
            response.json().await.map_err(|e| {
                strake_connectors::introspect::IntrospectError::Serialization(e.to_string())
            })?;

        let source_config = response_data
            .sources
            .into_iter()
            .find(|s| s.name.as_ref() == self.source)
            .ok_or_else(|| {
                strake_connectors::introspect::IntrospectError::NotFound(format!(
                    "Source '{}' not found in API response",
                    self.source
                ))
            })?;

        let table_config = source_config
            .tables
            .into_iter()
            .find(|t| t.schema == table.schema && t.name == table.table)
            .ok_or_else(|| {
                strake_connectors::introspect::IntrospectError::NotFound(format!(
                    "Table '{}.{}' not found in API response",
                    table.schema, table.table
                ))
            })?;

        Ok(strake_common::schema::IntrospectedTable {
            source: source_config.source_type.to_string(),
            schema: table_config.schema,
            name: table_config.name,
            columns: table_config
                .column_definitions
                .into_iter()
                .map(|c| strake_common::schema::IntrospectedColumn {
                    name: c.name,
                    type_str: c.data_type,
                    nullable: !c.not_null,
                    is_primary_key: c.primary_key,
                    is_foreign_key: false,
                    constraints: vec![],
                    db_comment: None,
                    ai_description: c.description,
                })
                .collect(),
            db_comment: None,
            ai_description: table_config.description,
        })
    }
}
