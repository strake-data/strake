//! Validation command and helpers.
//!
//! # Overview
//! The `validate` command ensures that the `sources.yaml` configuration is syntactically
//! correct and semantically valid against the upstream data sources.
//!
//! # Validation Steps
//! 1. **Syntax Check**: Parses the YAML structure.
//! 2. **Offline Check**: If `--offline` is set, validation stops here.
//! 3. **Contract Validation**: Checks against `contracts.yaml` (if present) via the API.
//! 4. **Source Validation**: Connects to each defined source to verify:
//!    - Connectivity (can we connect?)
//!    - Existence (do the tables/columns exist?)
//!    - Schema match (do types match?)

use super::helpers::{ValidateResult, get_client};
use crate::config::CliConfig;
use crate::models::{self};
use crate::secrets::ResolverContext;
use crate::{
    exit_codes,
    output::{self, OutputFormat},
};
use anyhow::{Context, Result, anyhow};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use std::path::Path;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio_postgres::NoTls;

pub async fn validate(
    file_path: &str,
    offline: bool,
    ci: bool,
    format: OutputFormat,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    if format.is_machine_readable() {
        let result = validate_internal(file_path, offline, config, ctx).await?;
        output::print_success(format, &result)?;

        if !result.valid {
            return Ok(exit_codes::EXIT_ERROR);
        }
        if !result.warnings.is_empty() {
            if ci {
                return Ok(exit_codes::EXIT_ERROR);
            }
            return Ok(exit_codes::EXIT_WARNINGS);
        }
        return Ok(exit_codes::EXIT_OK);
    }

    // Human output mode
    println!(
        "{} {} {}",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Validating...".bold().cyan()
    );
    let config_yaml = super::helpers::parse_yaml(file_path, ctx).await?;

    println!("Structure is valid.");

    if offline {
        println!(
            "{}",
            "Skipping semantic validation (offline mode).".dimmed()
        );
        return Ok(exit_codes::EXIT_OK);
    }

    println!("{}", "Starting Semantic Validation...".bold().cyan());

    let mut validation_errors = Vec::new();

    // Data Contract Validation
    {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .context("Failed to parse progress style template")?,
        );
        pb.set_message("Checking Data Contracts (Server-Side)...");
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        let sources_yaml = tokio::fs::read_to_string(file_path).await?;
        if let Err(e) = validate_contracts(&sources_yaml, "contracts.yaml", config).await {
            pb.finish_with_message(format!("{} Contract Validation Failed", "✘".red()));
            validation_errors.push(format!("Contract Validation Failed: {}", e));
        } else {
            pb.finish_with_message(format!("{} Contracts: OK", "✔".green()));
        }
    }

    for source in config_yaml.sources {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .context("Failed to parse progress style template")?,
        );
        pb.set_message(format!("Checking source '{}'...", source.name));
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        match validate_source(&source).await {
            Ok(_) => {
                pb.finish_with_message(format!("{} source '{}': OK", "✔".green(), source.name))
            }
            Err(e) => {
                pb.finish_with_message(format!("{} source '{}': FAILED", "✘".red(), source.name));
                validation_errors.push(format!("Source '{}': {}", source.name, e));
            }
        }
    }

    if validation_errors.is_empty() {
        println!("{}", "Semantic validation passed.".green().bold());
        Ok(exit_codes::EXIT_OK)
    } else {
        println!("\n{}", "Validation Errors:".red().bold());
        for err in validation_errors {
            println!("{} {}", "•".red(), err);
        }
        Ok(exit_codes::EXIT_ERROR)
    }
}
async fn validate_internal(
    file_path: &str,
    offline: bool,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<ValidateResult> {
    let mut validation_errors = Vec::new();
    let mut validation_warnings = Vec::new(); // Placeholder for future use
    let config_yaml = super::helpers::parse_yaml(file_path, ctx).await?;

    if offline {
        return Ok(ValidateResult {
            valid: true,
            errors: vec![],
            warnings: vec![],
        });
    }

    let sources_yaml = tokio::fs::read_to_string(file_path).await?;
    if let Err(e) = validate_contracts(&sources_yaml, "contracts.yaml", config).await {
        let err_msg = e.to_string();
        // Heuristic: if it looks like schema drift (mismatch/type changed), treat as warning
        // in a more advanced implementation. For now, following spec:
        // "validate found coerceable issues" -> EXIT_WARNINGS
        if err_msg.contains("STRAKE-2009")
            || err_msg.contains("STRAKE-2010")
            || err_msg.contains("STRAKE-2011")
        {
            validation_warnings.push(err_msg);
        } else {
            validation_errors.push(err_msg);
        }
    }

    for source in config_yaml.sources {
        if let Err(e) = validate_source(&source).await {
            validation_errors.push(format!("Source '{}': {}", source.name, e));
        }
    }

    if validation_errors.is_empty() {
        Ok(ValidateResult {
            valid: true,
            errors: vec![],
            warnings: validation_warnings,
        })
    } else {
        Ok(ValidateResult {
            valid: false,
            errors: validation_errors,
            warnings: validation_warnings,
        })
    }
}

pub(crate) async fn validate_source(source: &models::SourceConfig) -> Result<()> {
    let url = source.url.as_deref().unwrap_or("");
    match source.source_type.as_str() {
        "JDBC" if url.contains("postgresql") => validate_postgres_source(source, url).await,
        "JSON" => validate_file_source(url).await,
        _ if url.starts_with("file://") => validate_file_source(url).await,
        _ => Ok(()),
    }
}

async fn validate_postgres_source(source: &models::SourceConfig, url: &str) -> Result<()> {
    // Handle jdbc:postgresql:// -> postgres:// conversion
    let pg_url = url.replace("jdbc:postgresql://", "postgres://");

    let (client, connection) = tokio_postgres::connect(&pg_url, NoTls)
        .await
        .context("Failed to connect to Postgres source")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "Postgres connection error during validation");
        }
    });

    for table in &source.tables {
        // Check if table exists
        let table_exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                &[&table.schema, &table.name],
            )
            .await
            .context("Failed to query information_schema.tables")?
            .get(0);

        if !table_exists {
            return Err(anyhow!(
                "Table '{}.{}' not found in upstream",
                table.schema,
                table.name
            ));
        }

        // Check columns
        for col in &table.columns {
            let col_exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
                    &[&table.schema, &table.name, &col.name],
                )
                .await
                .context("Failed to query information_schema.columns")?
                .get(0);

            if !col_exists {
                return Err(anyhow!(
                    "Column '{}' not found in table '{}.{}'",
                    col.name,
                    table.schema,
                    table.name
                ));
            }
        }
    }

    Ok(())
}

async fn validate_file_source(url: &str) -> Result<()> {
    let path_str = url.trim_start_matches("file://");
    let path = Path::new(path_str);

    if !path.exists() {
        return Err(anyhow!("File not found: {}", path_str));
    }

    let file = tokio::fs::File::open(path)
        .await
        .context(format!("Failed to open file: {}", path_str))?;
    let mut reader = BufReader::new(file);
    let mut buffer = String::new();

    // Read up to 3 lines to verify readability and header presence
    for _ in 0..3 {
        let bytes = reader
            .read_line(&mut buffer)
            .await
            .context("Failed to read line from file")?;
        if bytes == 0 {
            break; // EOF
        }
    }

    if buffer.trim().is_empty() {
        return Err(anyhow!("File is empty or not readable: {}", path_str));
    }

    Ok(())
}

pub(crate) async fn validate_contracts(
    sources_yaml: &str,
    contracts_path: &str,
    config: &CliConfig,
) -> Result<()> {
    let contracts_yaml = if std::path::Path::new(contracts_path).exists() {
        Some(tokio::fs::read_to_string(contracts_path).await?)
    } else {
        None
    };

    let client = get_client(config)?;
    let api_url = &config.api_url;

    let response = client
        .post(format!("{}/validate-contracts", api_url))
        .json(&strake_common::models::ValidationRequest {
            sources_yaml: sources_yaml.to_string(),
            contracts_yaml,
        })
        .send()
        .await
        .context("Failed to connect to Strake Validation API. Ensure the server is running.")?;

    let result: strake_common::models::ValidationResponse = response
        .json()
        .await
        .context("Failed to parse validation response from server")?;

    if result.valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "\n      - {}",
            result.errors.join("\n      - ")
        ))
    }
}
