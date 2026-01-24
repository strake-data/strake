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
    sources_only: bool,
    _format: OutputFormat,
) -> Result<()> {
    // 1. Create sources.yaml
    create_sources_yaml(template, output_path)?;

    // 2. Additionally create strake.yaml and README.md unless --sources-only
    if !sources_only {
        let config_path = Path::new("strake.yaml");
        create_strake_yaml(config_path)?;

        let readme_path = Path::new("README.md");
        create_readme(readme_path)?;

        // 3. Initialize metadata database
        println!("Initializing metadata database...");
        let config =
            crate::config::load(None).context("Failed to load newly created configuration")?;
        let store = crate::metadata::init_store(&config)
            .await
            .context("Failed to initialize metadata store")?;
        store
            .init()
            .await
            .context("Failed to create initial schema in metadata database")?;
        println!("✓ Metadata database initialized");
    }

    Ok(())
}

fn create_sources_yaml(template: Option<String>, output_path: &Path) -> Result<()> {
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
        "✓ Created sources.yaml with {:?} template",
        template.as_deref().unwrap_or("default")
    );
    Ok(())
}

fn create_strake_yaml(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        println!("strake.yaml already exists.");
        if !confirm_overwrite() {
            return Ok(());
        }
    }

    let config_template = include_str!("../../templates/strake.yaml");
    fs::write(config_path, config_template)
        .context(format!("Failed to write {:?}", config_path))?;
    println!("✓ Created strake.yaml");
    Ok(())
}

fn create_readme(readme_path: &Path) -> Result<()> {
    if readme_path.exists() {
        println!("README.md already exists.");
        if !confirm_overwrite() {
            return Ok(());
        }
    }

    let readme_template = include_str!("../../templates/README.md");
    fs::write(readme_path, readme_template)
        .context(format!("Failed to write {:?}", readme_path))?;
    println!("✓ Created README.md");
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
