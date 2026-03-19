#![deny(missing_docs)]
//! Strake CLI: GitOps-driven federated SQL management.
//!
//! The CLI is the primary interface for managing Strake state, designed for GitOps workflows.
//! It revolves around applying a declarative `sources.yaml` configuration to the metadata store.
//!
//! # Core Commands
//!
//! - `init`: Create a fresh configuration workspace.
//! - `apply`: Reconcile the local `sources.yaml` with the database state.
//! - `diff`: Preview changes before applying.
//! - `validate`: Check configuration validity (schema and network connectivity).
//!
//! # Exploration
//!
//! - `introspect`: Discover tables in upstream sources.
//! - `search`: Find specific tables across all sources.
//! - `add`: Quickly add a discovered table to `sources.yaml`.

use clap::{Parser, Subcommand};
use owo_colors::OwoColorize;

mod commands;
mod config;
mod exit_codes;
mod metadata;
mod models;
mod output;
mod secrets;

use crate::config::CliConfig;
use strake_error::ErrorCategory;

use output::OutputFormat;

/// The main CLI structure for Strake.
#[derive(Parser)]
#[command(name = "strake-cli")]
#[command(about = "Manage Strake configuration and metadata", long_about = None)]
struct Cli {
    /// The subcommand to execute.
    #[command(subcommand)]
    command: Commands,

    /// Output format (human, json, yaml)
    #[arg(long, global = true, value_enum, default_value = "human")]
    output: OutputFormat,

    /// API token for authentication
    #[arg(long, global = true, env = "STRAKE_TOKEN")]
    token: Option<String>,

    /// Configuration profile
    #[arg(long, global = true, env = "STRAKE_PROFILE")]
    profile: Option<String>,
}

/// All valid Strake CLI subcommands.
#[derive(Subcommand)]
enum Commands {
    /// Initialize the project with a default configuration
    Init {
        /// Type of template to use
        #[arg(long, value_enum)]
        template: Option<Template>,
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Only create sources.yaml, skip strake.yaml and README.md
        #[arg(long, default_value_t = false)]
        sources_only: bool,
    },
    /// Validate the sources.yaml configuration
    Validate {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Skip semantic validation (network calls)
        #[arg(long, default_value_t = false)]
        offline: bool,
        /// Strict mode: fail on warnings (coercions, drift)
        #[arg(long, default_value_t = false)]
        ci: bool,
    },
    /// Apply the configuration to the metadata store
    Apply {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Force operation (required for potentially destructive actions like deleting all sources)
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Run validation and preview changes without applying them
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Optimistic locking: Expected current version of the domain. Fails if mismatch.
        #[arg(long)]
        expected_version: Option<i32>,
        /// URL to notify after successful application (for cache invalidation)
        #[arg(long)]
        notify_url: Option<String>,
    },
    /// Aggregated health view of a domain
    Status {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: Option<String>,
        /// Specify domain to check status for
        #[arg(long)]
        domain: Option<String>,
        /// Timeout for reachability checks in milliseconds
        #[arg(long, default_value_t = 5000)]
        timeout: u64,
    },
    /// Safely remove a table from sources.yaml
    Remove {
        /// The name of the source
        source: String,
        /// The name of the table
        #[arg(required_unless_present = "source_only")]
        table: Option<String>,
        /// Path to the sources.yaml file to update
        #[arg(default_value = "sources.yaml")]
        file: Option<String>,
        /// Run checks without writing changes
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Force removal despite orphaned references
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Remove the entire source entry (Phase 1 stub)
        #[arg(long, default_value_t = false)]
        source_only: bool,
    },
    /// Preview changes between the configuration and metadata store
    Diff {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
    },
    /// Introspect a source to discover its schema (Legacy alias for search)
    Introspect {
        /// The name of the source or its connection string
        source: String,
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
    },
    /// Search for tables in an upstream source
    Search {
        /// The name of the source
        source: String,
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Optional domain to search within
        #[arg(long)]
        domain: Option<String>,
    },
    /// Add a table from an upstream source to sources.yaml
    Add {
        /// The name of the source
        source: String,
        /// The full table name (schema.table)
        table: String,
        /// Path to the sources.yaml file to update
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Optional domain to add to
        #[arg(long)]
        domain: Option<String>,
    },
    /// Test connections to defined sources
    TestConnection {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
    },
    /// Describe the current configuration and metadata
    Describe {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Specify domain to describe
        #[arg(long)]
        domain: Option<String>,
    },
    /// Manage domains (list, history)
    Domain {
        #[command(subcommand)]
        subcommand: DomainCommands,
    },
    /// Manage secrets
    Secrets {
        #[command(subcommand)]
        subcommand: SecretCommands,
    },
}

#[derive(Subcommand)]
enum SecretCommands {
    /// Validate secret references in a configuration file
    Validate {
        /// Path to the configuration file (sources.yaml, etc.)
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Skip external providers, only validate syntax and environment
        #[arg(long, default_value_t = false)]
        offline: bool,
    },
    /// Configure secret providers (Not implemented in 1.1)
    Configure,
}

#[derive(Subcommand)]
enum DomainCommands {
    /// List all registered domains
    List,
    /// Show the history of apply events for a domain
    History {
        /// The name of the domain
        #[arg(default_value = "default")]
        name: String,
    },
    /// Rollback a domain to a previous version
    Rollback {
        /// The name of the domain
        #[arg(default_value = "default")]
        name: String,
        /// The version to rollback to
        #[arg(long)]
        to_version: i32,
        /// Force operation (bypass safety guards)
        #[arg(long, default_value_t = false)]
        force: bool,
    },
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Template {
    Sql,
    Rest,
    File,
    Grpc,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    // Load configuration
    let mut config = config::load(cli.profile.as_deref())
        .map_err(|e| anyhow::anyhow!("Failed to load configuration: {}", e))?;

    // Override token from CLI args if present
    if let Some(token) = &cli.token {
        config.token = Some(token.clone());
    }

    // Load secrets context
    let resolver_ctx = load_resolver_context();

    match run_cli(&cli, &config, &resolver_ctx).await {
        Ok(exit_code) => {
            std::process::exit(exit_code);
        }
        Err(e) => {
            let exit_code = map_error_to_exit_code(&e);
            if cli.output.is_machine_readable() {
                output::print_error::<()>(cli.output, &e.to_string(), exit_code, None).ok();
            } else {
                eprintln!("{} {}", "Error:".red().bold(), e);
            }
            std::process::exit(exit_code);
        }
    }
}

fn map_error_to_exit_code(e: &anyhow::Error) -> i32 {
    // Try to downcast to StrakeError for type-safe mapping
    if let Some(strake_err) = e.downcast_ref::<strake_error::StrakeError>() {
        return match strake_err.code.category() {
            ErrorCategory::Connection => exit_codes::EXIT_ERROR,
            ErrorCategory::Config => exit_codes::EXIT_ERROR,
            ErrorCategory::Query => exit_codes::EXIT_ERROR,
            ErrorCategory::Auth => exit_codes::EXIT_ERROR,
            ErrorCategory::Internal => exit_codes::EXIT_ERROR,
            _ => exit_codes::EXIT_ERROR, // Handle future variants
        };
    }

    // Fallback: string heuristics for non-StrakeError types
    let s = e.to_string().to_lowercase();
    if s.contains("usage") || s.contains("argument") {
        return exit_codes::EXIT_ERROR;
    }
    if s.contains("config") || s.contains("yaml") {
        return exit_codes::EXIT_ERROR;
    }
    if s.contains("connect") || s.contains("timeout") {
        return exit_codes::EXIT_ERROR;
    }
    if s.contains("permission") || s.contains("unauthorized") {
        return exit_codes::EXIT_ERROR;
    }
    if s.contains("validation") || s.contains("contract") {
        return exit_codes::EXIT_ERROR;
    }
    if s.contains("conflict") || s.contains("version mismatch") {
        return exit_codes::EXIT_ERROR;
    }
    exit_codes::EXIT_ERROR
}

fn load_resolver_context() -> secrets::ResolverContext {
    use std::collections::HashMap;

    let system_env: HashMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| {
            let k_str = k.into_string().ok()?;
            let v_str = v.into_string().ok()?;
            Some((k_str, v_str))
        })
        .collect();

    // In a real implementation, we would search adjacent to sources.yaml
    // For now, CWD is the baseline.
    let mut dotenv = HashMap::new();
    if let Ok(iter) = dotenvy::from_path_iter(".env") {
        for (k, v) in iter.flatten() {
            dotenv.insert(k, v);
        }
    }

    secrets::ResolverContext {
        system_env,
        dotenv,
        offline: false, // Default to online, commands can override
    }
}

async fn run_cli(
    cli: &Cli,
    config: &CliConfig,
    resolver_ctx: &secrets::ResolverContext,
) -> Result<i32, anyhow::Error> {
    match &cli.command {
        Commands::Init {
            template,
            file,
            sources_only,
        } => {
            let template_str = template.as_ref().map(|t| format!("{:?}", t).to_lowercase());
            return commands::init(
                template_str,
                std::path::Path::new(file),
                *sources_only,
                cli.output,
                resolver_ctx,
            )
            .await;
        }
        Commands::Validate { file, offline, ci } => {
            return commands::validate(file, *offline, *ci, cli.output, config, resolver_ctx).await;
        }
        Commands::Apply {
            file,
            force,
            dry_run,
            expected_version,
            notify_url,
        } => {
            let store = metadata::init_store(config).await?;
            return commands::apply(
                &*store,
                commands::ApplyOptions {
                    file_path: file.clone(),
                    force: *force,
                    dry_run: *dry_run,
                    expected_version: *expected_version,
                    format: cli.output,
                    notify_url: notify_url.clone(),
                },
                config,
                resolver_ctx,
            )
            .await;
        }
        Commands::Diff { file } => {
            let store = metadata::init_store(config).await?;
            return commands::diff(&*store, file, cli.output, resolver_ctx).await;
        }
        Commands::Introspect { source, file } => {
            return commands::search(source, file, None, cli.output, config, resolver_ctx).await;
        }
        Commands::Search {
            source,
            file,
            domain,
        } => {
            return commands::search(
                source,
                file,
                domain.as_deref(),
                cli.output,
                config,
                resolver_ctx,
            )
            .await;
        }
        Commands::Add {
            source,
            table,
            file,
            domain,
        } => {
            return commands::add(
                source,
                table,
                domain.as_deref(),
                file,
                cli.output,
                config,
                resolver_ctx,
            )
            .await;
        }
        Commands::TestConnection { file } => {
            return commands::test_connection(file, cli.output, config, resolver_ctx).await;
        }
        Commands::Describe { file, domain } => {
            let store = metadata::init_store(config).await?;
            return commands::describe(&*store, file, domain.as_deref(), cli.output, resolver_ctx)
                .await;
        }
        Commands::Domain { subcommand } => {
            let store = metadata::init_store(config).await?;
            match subcommand {
                DomainCommands::List => {
                    return commands::list_domains(&*store, cli.output, resolver_ctx).await;
                }
                DomainCommands::History { name } => {
                    return commands::show_domain_history(
                        &*store,
                        name.to_string(),
                        cli.output,
                        resolver_ctx,
                    )
                    .await;
                }
                DomainCommands::Rollback {
                    name,
                    to_version,
                    force,
                } => {
                    return commands::rollback(
                        &*store,
                        name,
                        *to_version,
                        *force,
                        cli.output,
                        resolver_ctx,
                    )
                    .await;
                }
            }
        }
        Commands::Secrets { subcommand } => match subcommand {
            SecretCommands::Validate { file, offline } => {
                let mut ctx = load_resolver_context();
                ctx.offline = *offline;
                return commands::validate_secrets(file, &ctx, cli.output).await;
            }
            SecretCommands::Configure => {
                println!("'secrets configure' is not yet implemented");
            }
        },
        Commands::Status {
            file,
            domain,
            timeout,
        } => {
            let store = metadata::init_store(config).await?;
            return commands::status(
                &*store,
                file.as_deref(),
                domain.as_deref(),
                *timeout,
                cli.output,
                resolver_ctx,
            )
            .await;
        }
        Commands::Remove {
            source,
            table,
            file,
            dry_run,
            force,
            source_only,
        } => {
            return commands::remove(
                source,
                table.as_deref(),
                file.as_deref(),
                *dry_run,
                *force,
                *source_only,
                cli.output,
            )
            .await;
        }
    }
    Ok(exit_codes::EXIT_OK)
}
