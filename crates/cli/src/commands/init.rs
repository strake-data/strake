//! Command to initialize a new sources.yaml configuration file.
//!
//! # Overview
//! The `init` command bootstraps a new Strake workspace by creating a default
//! or template-based `sources.yaml` file. This is typically the first command
//! a user runs.
//!
//! # Templates
//! - `default`: A basic example with comments.
//! - `sql`: Example configuration for a PostgreSQL source.
//! - `rest`: Example configuration for a REST API source.
//! - `file`: Example configuration for a local file source (Parquet/CSV/JSON).
//! - `grpc`: Example configuration for a gRPC source.

use crate::output::OutputFormat;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

pub async fn init(
    template: Option<String>,
    output_path: &Path,
    _format: OutputFormat,
) -> Result<()> {
    // Check if file exists at output_path
    if output_path.exists() {
        println!("sources.yaml already exists.");
        if !confirm_overwrite() {
            return Ok(());
        }
    }

    let default_config = match template.as_deref() {
        Some("sql") => include_str!("../../templates/sql.yaml"),
        Some("rest") => include_str!("../../templates/rest.yaml"),
        Some("file") => include_str!("../../templates/file.yaml"),
        Some("grpc") => include_str!("../../templates/grpc.yaml"),
        _ => include_str!("../../templates/default.yaml"),
    };

    fs::write(output_path, default_config).context(format!("Failed to write {:?}", output_path))?;
    println!(
        "Initialized sources.yaml with {:?} template",
        template.unwrap_or_else(|| "default".to_string())
    );
    Ok(())
}

fn confirm_overwrite() -> bool {
    use std::io::{self, Write};
    print!("File exists. Overwrite? (y/N): ");
    let _ = io::stdout().flush();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_err() {
        return false;
    }

    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}
