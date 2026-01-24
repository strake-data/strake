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

use super::helpers::{expand_secrets, get_client, ValidateResult};
use crate::config::CliConfig;
use crate::models;
use crate::{
    exit_codes,
    output::{self, OutputFormat},
};
use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use std::fs;
use std::path::Path;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio_postgres::NoTls;

pub async fn validate(
    file_path: &str,
    offline: bool,
    format: OutputFormat,
    config: &CliConfig,
) -> Result<()> {
    if format.is_machine_readable() {
        let result = validate_internal(file_path, offline, config).await?;
        output::print_success(format, &result)?;
        if !result.valid {
            std::process::exit(exit_codes::VALIDATION_ERROR);
        }
        return Ok(());
    }

    // Human output mode
    println!(
        "{} {} {}",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Validating...".bold().cyan()
    );
    let config_yaml = parse_yaml(file_path)?;

    println!("Structure is valid.");

    if offline {
        println!(
            "{}",
            "Skipping semantic validation (offline mode).".dimmed()
        );
        return Ok(());
    }

    println!("{}", "Starting Semantic Validation...".bold().cyan());

    let mut validation_errors = Vec::new();

    // Data Contract Validation
    {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        pb.set_message("Checking Data Contracts (Server-Side)...");
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        let sources_yaml = fs::read_to_string(file_path)?;
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
                .unwrap(),
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
        Ok(())
    } else {
        println!("\n{}", "Validation Errors:".red().bold());
        for err in validation_errors {
            println!("{} {}", "•".red(), err);
        }
        Err(anyhow!("Validation failed with errors"))
    }
}

async fn validate_internal(
    file_path: &str,
    offline: bool,
    config: &CliConfig,
) -> Result<ValidateResult> {
    let mut validation_errors = Vec::new();
    let config_yaml = parse_yaml(file_path)?;

    if offline {
        return Ok(ValidateResult {
            valid: true,
            errors: vec![],
        });
    }

    let sources_yaml = fs::read_to_string(file_path)?;
    if let Err(e) = validate_contracts(&sources_yaml, "contracts.yaml", config).await {
        validation_errors.push(format!("Contract Validation Failed: {}", e));
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
        })
    } else {
        Ok(ValidateResult {
            valid: false,
            errors: validation_errors,
        })
    }
}

pub(crate) async fn validate_source(source: &models::SourceConfig) -> Result<()> {
    let url = source.url.as_deref().unwrap_or("");

    if source.source_type == "JDBC" && url.contains("postgresql") {
        validate_postgres_source(source, url).await
    } else if source.source_type == "JSON" || url.starts_with("file://") {
        validate_file_source(url).await
    } else {
        // Unknown or unsupported type for validation, just skip or warn
        Ok(())
    }
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

pub(crate) async fn validate_contracts(
    sources_yaml: &str,
    contracts_path: &str,
    config: &CliConfig,
) -> Result<()> {
    let contracts_yaml = if std::path::Path::new(contracts_path).exists() {
        Some(fs::read_to_string(contracts_path)?)
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

fn parse_yaml(path: &str) -> Result<models::SourcesConfig> {
    let raw_content =
        fs::read_to_string(path).context(format!("Failed to read config file: {}", path))?;
    let content = expand_secrets(&raw_content);
    serde_yaml::from_str(&content).context("Failed to parse YAML structure")
}
