//! # Validate Command
//!
//! Validates the `sources.yaml` configuration and optionally emits a structured CI receipt.
//!
//! # Overview
//!
//! The `validate` command is the primary GitOps gate for Strake. It verifies that the
//! local `sources.yaml` is syntactically correct, semantically consistent with upstream
//! data contracts, and aligned with the live schemas on each declared remote database.
//!
//! # Validation Steps
//!
//! 1. **Syntax Check**: Parses the YAML structure and expands secrets.
//! 2. **Offline Mode**: If `--offline` is set, validation stops after the syntax check.
//! 3. **Contract Validation**: Checks against `contracts.yaml` (if present) via the API.
//! 4. **Source Validation**: Connects to each defined source to verify:
//!    - Connectivity (can we connect?)
//!    - Existence (do the tables/columns exist?)
//!    - Schema match (do types match?)
//! 5. **Live Diff** *(non-offline only)*: Compares local column declarations against the
//!    remote schemas to surface drift.
//!
//! # CI Integration
//!
//! Pass `--output json` for a machine-readable [`ValidateReceipt`] suitable for parsing
//! in CI pipelines. Combine with `--notify-url` to POST the receipt to an external webhook
//! (e.g. a deployment tracker or Slack notifier) on success.
//!
//! ```bash
//! strake-cli validate --output json --notify-url https://control-plane/hook
//! ```

use super::diff::{diff_internal, print_diff_human};
use super::helpers::{ValidateResult, get_client};
use crate::config::CliConfig;
use crate::models;
use crate::secrets::ResolverContext;
use crate::{
    exit_codes,
    output::{self, OutputFormat},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use serde::Serialize;
use std::path::Path;
use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio_postgres::NoTls;

// ===== Public types =====

/// Machine-readable validation receipt emitted under `--output json`.
///
/// Designed for consumption by CI pipelines, deployment trackers, and
/// observability tools. The schema is versioned via `receipt_version`.
#[derive(Serialize, Clone, Debug)]
pub struct ValidateReceipt {
    /// Schema version of the receipt format; currently always `1`.
    pub receipt_version: u32,
    /// Timestamp at which validation completed.
    pub validated_at: DateTime<Utc>,
    /// Actor derived from `STRAKE_ACTOR` or `STRAKE_PROFILE` env vars.
    pub actor: String,
    /// Domain from `sources.yaml`, or `"default"` if absent.
    pub domain: String,
    /// Whether the configuration passed all validation checks.
    pub valid: bool,
    /// Validation errors, if any.
    pub errors: Vec<String>,
    /// Non-fatal warnings (coercible drift, missing optional fields, etc.).
    pub warnings: Vec<String>,
    /// Whether this was a dry-run (diff printed but webhook not fired).
    pub dry_run: bool,
    /// Schema drift detected between local config and live databases.
    pub drift_detected: bool,
    /// Wall-clock time of the full validation pass in milliseconds.
    pub duration_ms: u128,
}

// ===== Entry point =====

/// Options forwarded from the CLI layer to [`validate`].
pub struct ValidateOptions {
    /// Path to the `sources.yaml` file.
    pub file: String,
    /// Skip all network calls (syntax-only check).
    pub offline: bool,
    /// Treat warnings as errors (non-zero exit).
    pub fail_on_warnings: bool,
    /// Dry-run: show diff and skip webhook notification.
    pub dry_run: bool,
    /// Optional URL to POST the [`ValidateReceipt`] to on success.
    pub notify_url: Option<String>,
    /// Output format requested by the user.
    pub format: OutputFormat,
}

/// Validates the `sources.yaml` at `options.file`.
///
/// # Returns
///
/// * `EXIT_OK` — validation passed, no warnings.
/// * `EXIT_WARNINGS` — passed with non-fatal warnings (unless `fail_on_warnings`).
/// * `EXIT_DRY_RUN` — `--dry-run` flag was set; diff printed, webhook skipped.
/// * `EXIT_ERROR` — validation failed.
pub async fn validate(
    opts: ValidateOptions,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    let started_at = Instant::now();

    if opts.format.is_machine_readable() {
        return validate_machine_readable(opts, config, ctx, started_at).await;
    }
    validate_human(opts, config, ctx).await
}

// ===== Machine-readable path =====

async fn validate_machine_readable(
    opts: ValidateOptions,
    config: &CliConfig,
    ctx: &ResolverContext,
    started_at: Instant,
) -> Result<i32> {
    let result = validate_internal(&opts.file, opts.offline, config, ctx).await?;
    let actor = resolve_actor();

    // Parse domain for receipt
    let domain = parse_domain(&opts.file, ctx).await;

    // Compute drift against live DBs (skip if offline or validation already failed)
    let drift_detected = if !opts.offline && result.valid {
        let diff = diff_internal(&opts.file, config, ctx).await?;
        !diff.changes.is_empty()
    } else {
        false
    };

    let receipt = ValidateReceipt {
        receipt_version: 1,
        validated_at: Utc::now(),
        actor,
        domain,
        valid: result.valid,
        errors: result.errors.clone(),
        warnings: result.warnings.clone(),
        dry_run: opts.dry_run,
        drift_detected,
        duration_ms: started_at.elapsed().as_millis(),
    };

    output::print_success(opts.format, &receipt)?;

    if !result.valid {
        return Ok(exit_codes::EXIT_ERROR);
    }

    if opts.dry_run {
        return Ok(exit_codes::EXIT_DRY_RUN);
    }

    // Fire webhook on success (non-dry-run)
    notify_webhook(&receipt, opts.notify_url, opts.format).await;

    if !result.warnings.is_empty() {
        if opts.fail_on_warnings {
            return Ok(exit_codes::EXIT_ERROR);
        }
        return Ok(exit_codes::EXIT_WARNINGS);
    }
    Ok(exit_codes::EXIT_OK)
}

// ===== Human-readable path =====

async fn validate_human(
    opts: ValidateOptions,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    println!(
        "{} {} {}",
        "[Config:".dimmed(),
        opts.file.yellow(),
        "] Validating...".bold().cyan()
    );
    let config_yaml = super::helpers::parse_yaml(&opts.file, ctx).await?;

    println!("Structure is valid.");

    if opts.offline {
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
        let pb = spinner()?;
        pb.set_message("Checking Data Contracts (Server-Side)...");
        let sources_yaml = tokio::fs::read_to_string(&opts.file).await?;
        if let Err(e) = validate_contracts(&sources_yaml, "contracts.yaml", config).await {
            pb.finish_with_message(format!("{} Contract Validation Failed", "✘".red()));
            validation_errors.push(format!("Contract Validation Failed: {}", e));
        } else {
            pb.finish_with_message(format!("{} Contracts: OK", "✔".green()));
        }
    }

    for source in config_yaml.sources {
        let pb = spinner()?;
        pb.set_message(format!("Checking source '{}'...", source.name));
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

    if !validation_errors.is_empty() {
        println!("\n{}", "Validation Errors:".red().bold());
        for err in &validation_errors {
            println!("{} {}", "•".red(), err);
        }
        return Ok(exit_codes::EXIT_ERROR);
    }

    println!("{}", "Semantic validation passed.".green().bold());

    // Show live diff
    println!();
    let diff_result = diff_internal(&opts.file, config, ctx).await?;

    if opts.dry_run {
        print_diff_human(&diff_result);
        println!("\nNo changes applied (dry-run mode).");
        return Ok(exit_codes::EXIT_DRY_RUN);
    }

    print_diff_human(&diff_result);

    // Fire webhook when not dry-run and not offline
    if opts.notify_url.is_some() {
        let actor = resolve_actor();
        let domain = parse_domain(&opts.file, ctx).await;
        let receipt = ValidateReceipt {
            receipt_version: 1,
            validated_at: Utc::now(),
            actor,
            domain,
            valid: true,
            errors: vec![],
            warnings: vec![],
            dry_run: false,
            drift_detected: !diff_result.changes.is_empty(),
            duration_ms: 0,
        };
        notify_webhook(&receipt, opts.notify_url, opts.format).await;
    }

    Ok(exit_codes::EXIT_OK)
}

// ===== Internals =====

async fn validate_internal(
    file_path: &str,
    offline: bool,
    config: &CliConfig,
    ctx: &ResolverContext,
) -> Result<ValidateResult> {
    let mut validation_errors = Vec::new();
    let mut validation_warnings = Vec::new();
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
        // Heuristic: coerceable schema drift codes become warnings; hard errors fail validation.
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
        let table_exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = $2)",
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

        for col in &table.column_definitions {
            let col_exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT FROM information_schema.columns \
                     WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
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

    let mut req = strake_common::models::ValidationRequest::default();
    req.sources_yaml = sources_yaml.to_string();
    req.contracts_yaml = contracts_yaml;

    let response = client
        .post(format!("{}/validate-contracts", api_url))
        .json(&req)
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

// ===== CI helpers =====

/// POST the [`ValidateReceipt`] to `notify_url` (best-effort; errors are logged, not propagated).
async fn notify_webhook(
    receipt: &ValidateReceipt,
    notify_url: Option<String>,
    format: OutputFormat,
) {
    let Some(url) = notify_url else { return };
    if !format.is_machine_readable() {
        eprintln!("{} Notifying server at {}...", "ℹ".blue(), url);
    }
    let client = reqwest::Client::new();
    match client.post(&url).json(receipt).send().await {
        Ok(resp) if resp.status().is_success() => {
            if !format.is_machine_readable() {
                eprintln!("{} Server notification successful.", "✔".green());
            }
        }
        Ok(resp) => eprintln!("{} Server returned error: {}", "✖".red(), resp.status()),
        Err(e) => eprintln!("{} Failed to notify server: {}", "✖".red(), e),
    }
}

/// Resolve the acting identity from environment variables.
fn resolve_actor() -> String {
    std::env::var("STRAKE_ACTOR")
        .ok()
        .or_else(|| std::env::var("STRAKE_PROFILE").ok())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Best-effort extraction of the domain name from `sources.yaml` for the receipt.
async fn parse_domain(file_path: &str, ctx: &ResolverContext) -> String {
    super::helpers::parse_yaml(file_path, ctx)
        .await
        .ok()
        .and_then(|c| c.domain)
        .map(|d| d.to_string())
        .unwrap_or_else(|| "default".to_string())
}

/// Create a standard spinner for human-readable output.
fn spinner() -> Result<ProgressBar> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .context("Failed to parse progress style template")?,
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    Ok(pb)
}
