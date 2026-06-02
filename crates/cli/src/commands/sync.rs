//! # Sync Command
//!
//! Introspects configured database sources and updates local schema definitions in-place.
//!
//! ## Overview
//!
//! The `sync` command introspects live remote database schemas and merges their structures
//! directly back into the local `sources.yaml` file in-place. This enables developers to stay aligned
//! with live schema changes while preserving hand-written table and column descriptions. Atomic writes
//! are guaranteed by utilizing temporary-file persistence and rollback behaviors on failure.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let opts = SyncOptions {
//!     file: "sources.yaml".to_string(),
//!     format: OutputFormat::Human,
//! };
//! let code = sync(opts, &config, &ctx).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! The command performs live introspection queries against each remote database sequentially.
//! To protect schema definitions, it performs in-memory merging and writes to a temporary file
//! in the target directory, completing with an atomic swap to prevent files from being corrupted
//! on partial errors or network failure.
//!
//! ## Errors
//!
//! Returns `Err` if:
//! - Target configuration file `sources.yaml` does not exist or cannot be parsed.
//! - Network or database connectivity issues prevent querying remote schemas.
//! - Writing to the file system fails or permission errors prevent atomic persistence.

use crate::{
    commands::discovery::resolve_introspector, commands::helpers::parse_yaml, exit_codes,
    output::OutputFormat, secrets::ResolverContext,
};
use anyhow::Result;
use owo_colors::OwoColorize;

#[derive(Debug, Clone)]
pub struct SyncOptions {
    /// Path to the configuration file (e.g. sources.yaml).
    pub file: String,
    /// Format mode for output (Terminal/JSON).
    pub format: OutputFormat,
}

/// Introspects live remote database schemas and updates sources.yaml in-place.
pub async fn sync(
    opts: SyncOptions,
    config: &crate::config::CliConfig,
    ctx: &ResolverContext,
) -> Result<i32> {
    let file_path = opts.file.clone();

    let mut current_config = if std::path::Path::new(&file_path).exists() {
        parse_yaml(&file_path, ctx).await?
    } else {
        anyhow::bail!("Configuration file not found: {}", file_path);
    };

    if !opts.format.is_machine_readable() {
        println!(
            "{} syncing all configured source schemas in '{}'...",
            "⚙".cyan(),
            file_path.yellow()
        );
    }

    for source in &mut current_config.sources {
        let introspector =
            match resolve_introspector(source.name.as_ref(), &file_path, config, ctx).await {
                Ok(intro) => intro,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to resolve introspector for source '{}': {}",
                        source.name,
                        e
                    ));
                }
            };

        for table in &mut source.tables {
            let table_ref = strake_connectors::introspect::TableRef {
                schema: table.schema.clone(),
                table: table.name.clone(),
            };

            if !opts.format.is_machine_readable() {
                println!(
                    "  {} Introspecting remote '{}.{}' on source '{}'...",
                    "->".dimmed(),
                    table.schema.bold(),
                    table.name.bold(),
                    source.name.as_ref().bold()
                );
            }

            match introspector.introspect_table(&table_ref, false).await {
                Ok(introspected) => {
                    if table.description.is_none() {
                        table.description = introspected.db_comment.clone();
                    }

                    // Merge columns
                    for intro_col in introspected.columns.clone() {
                        if let Some(existing_col) = table
                            .column_definitions
                            .iter_mut()
                            .find(|c| c.name == intro_col.name)
                        {
                            existing_col.data_type = intro_col.type_str;
                            existing_col.not_null = !intro_col.nullable;
                            if intro_col.is_primary_key {
                                existing_col.primary_key = true;
                            }
                            if existing_col.description.is_none() {
                                existing_col.description = intro_col.db_comment;
                            }
                        } else {
                            let mut col = strake_common::models::ColumnConfig::default();
                            col.name = intro_col.name;
                            col.data_type = intro_col.type_str;
                            col.primary_key = intro_col.is_primary_key;
                            col.not_null = !intro_col.nullable;
                            col.description = intro_col.db_comment;
                            table.column_definitions.push(col);
                        }
                    }

                    // Prune columns that are no longer present on the remote database
                    table
                        .column_definitions
                        .retain(|col| introspected.columns.iter().any(|c| c.name == col.name));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Introspection failed for table '{}.{}': {}",
                        table.schema,
                        table.name,
                        e
                    ));
                }
            }
        }
    }

    let yaml = serde_yaml::to_string(&current_config)?;
    let dest_file = file_path.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut tmp = tempfile::NamedTempFile::new_in(
            std::path::Path::new(&dest_file)
                .parent()
                .unwrap_or(std::path::Path::new(".")),
        )?;
        use std::io::Write;
        tmp.as_file_mut().write_all(yaml.as_bytes())?;
        tmp.persist(&dest_file)?;
        Ok(())
    })
    .await??;

    if !opts.format.is_machine_readable() {
        println!(
            "{} Successfully synced all configurations in '{}'.",
            "✔".green(),
            file_path.yellow()
        );
    }

    Ok(exit_codes::EXIT_OK)
}
