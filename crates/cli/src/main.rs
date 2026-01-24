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
use dotenv::dotenv;
use owo_colors::OwoColorize;
use std::env;

mod commands;
mod config;
mod db;
mod exit_codes;
mod models;
mod output;

use config::CliConfig;
use strake_error::ErrorCategory;

use output::OutputFormat;

#[derive(Parser)]
#[command(name = "strake-cli")]
#[command(about = "Manage Strake configuration and metadata", long_about = None)]
struct Cli {
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

#[derive(Subcommand)]
enum Commands {
    /// Initialize the project with a default configuration
    Init {
        /// Type of template to use
        #[arg(long, value_enum)]
        template: Option<Template>,
    },
    /// Validate the sources.yaml configuration
    Validate {
        /// Path to the sources.yaml file
        #[arg(default_value = "sources.yaml")]
        file: String,
        /// Skip semantic validation (network calls)
        #[arg(long, default_value_t = false)]
        offline: bool,
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
    dotenv().ok();

    let cli = Cli::parse();

    // Load configuration
    let mut config = config::load(cli.profile.as_deref())
        .map_err(|e| anyhow::anyhow!("Failed to load configuration: {}", e))?;

    // Override token from CLI args if present
    if let Some(token) = &cli.token {
        config.token = Some(token.clone());
    }

    // Determine DB URL priority: Config > Env > Default
    let db_url = config.database_url.clone().unwrap_or_else(|| {
        env::var("DATABASE_URL").unwrap_or_else(|_| config::DEFAULT_DATABASE_URL.to_string())
    });

    if let Err(e) = run_cli(&cli, &db_url, &config).await {
        let exit_code = map_error_to_exit_code(&e);
        if cli.output.is_machine_readable() {
            output::print_error::<()>(cli.output, &e.to_string(), exit_code).ok();
        } else {
            eprintln!("{} {}", "Error:".red().bold(), e);
        }
        std::process::exit(exit_code);
    }

    Ok(())
}

fn map_error_to_exit_code(e: &anyhow::Error) -> i32 {
    // Try to downcast to StrakeError for type-safe mapping
    if let Some(strake_err) = e.downcast_ref::<strake_error::StrakeError>() {
        return match strake_err.code.category() {
            ErrorCategory::Connection => exit_codes::CONNECTION_ERROR,
            ErrorCategory::Config => exit_codes::CONFIG_ERROR,
            ErrorCategory::Query => exit_codes::VALIDATION_ERROR,
            ErrorCategory::Auth => exit_codes::PERMISSION_ERROR,
            ErrorCategory::Internal => exit_codes::GENERAL_ERROR,
            _ => exit_codes::GENERAL_ERROR, // Handle future variants
        };
    }

    // Fallback: string heuristics for non-StrakeError types
    let s = e.to_string().to_lowercase();
    if s.contains("usage") || s.contains("argument") {
        return exit_codes::USAGE_ERROR;
    }
    if s.contains("config") || s.contains("yaml") {
        return exit_codes::CONFIG_ERROR;
    }
    if s.contains("connect") || s.contains("timeout") {
        return exit_codes::CONNECTION_ERROR;
    }
    if s.contains("permission") || s.contains("unauthorized") {
        return exit_codes::PERMISSION_ERROR;
    }
    if s.contains("validation") || s.contains("contract") {
        return exit_codes::VALIDATION_ERROR;
    }
    if s.contains("conflict") || s.contains("version mismatch") {
        return exit_codes::CONFLICT_ERROR;
    }
    exit_codes::GENERAL_ERROR
}

async fn run_cli(cli: &Cli, db_url: &str, config: &CliConfig) -> Result<(), anyhow::Error> {
    match &cli.command {
        Commands::Init { template } => {
            commands::init(
                template.as_ref().map(|t| format!("{:?}", t).to_lowercase()),
                std::path::Path::new("sources.yaml"),
                cli.output,
            )
            .await?;
        }
        Commands::Validate { file, offline } => {
            commands::validate(file, *offline, cli.output, config).await?;
        }
        Commands::Apply {
            file,
            force,
            dry_run,
            expected_version,
        } => {
            let client = db::connect(db_url).await?;
            commands::apply(
                &client,
                file,
                *force,
                *dry_run,
                *expected_version,
                cli.output,
                config,
            )
            .await?;
        }
        Commands::Diff { file } => {
            let client = db::connect(db_url).await?;
            commands::diff(&client, file, cli.output).await?;
        }
        Commands::Introspect {
            source,
            file,
        } => {
            commands::introspect(
                source,
                file,
                cli.output,
                config,
            )
            .await?;
        }
        Commands::Search {
            source,
            file,
            domain,
        } => {
            commands::search(source, file, domain.as_deref(), cli.output, config).await?;
        }
        Commands::Add {
            source,
            table,
            file,
            domain,
        } => {
            commands::add(source, table, domain.as_deref(), file, cli.output, config).await?;
        }
        Commands::TestConnection { file } => {
            commands::test_connection(file, cli.output, config).await?;
        }
        Commands::Describe { file, domain } => {
            let client = db::connect(db_url).await?;
            commands::describe(&client, file, domain.as_deref(), cli.output).await?;
        }
        Commands::Domain { subcommand } => {
            let client = db::connect(db_url).await?;
            match subcommand {
                DomainCommands::List => {
                    commands::list_domains(&client, cli.output).await?;
                }
                DomainCommands::History { name } => {
                    commands::show_domain_history(&client, name.to_string(), cli.output).await?;
                }
                DomainCommands::Rollback { name, to_version } => {
                    commands::rollback(&client, name, *to_version, cli.output).await?;
                }
            }
        }
    }
    Ok(())
}
