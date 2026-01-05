use clap::{Parser, Subcommand};
use dotenv::dotenv;
use owo_colors::OwoColorize;
use std::env;

mod commands;
mod db;
mod models;

#[derive(Parser)]
#[command(name = "strake-cli")]
#[command(about = "Manage Strake configuration and metadata", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
        /// If true, introspect a source already registered in the metadata store
        #[arg(long, default_value_t = false)]
        registered: bool,
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

    // Default to localhost for testing, but typically comes from env
    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@rust-postgres_devcontainer-db-1:5432/postgres".to_string()
    });

    let cli = Cli::parse();

    if let Err(e) = run_cli(&cli, &db_url).await {
        eprintln!("{} {}", "Error:".red().bold(), e);
        std::process::exit(1);
    }

    Ok(())
}

async fn run_cli(cli: &Cli, db_url: &str) -> Result<(), anyhow::Error> {
    match &cli.command {
        Commands::Init { template } => {
            commands::init(template.as_ref().map(|t| format!("{:?}", t).to_lowercase())).await?;
        }
        Commands::Validate { file, offline } => {
            commands::validate(file, *offline).await?;
        }
        Commands::Apply {
            file,
            force,
            dry_run,
            expected_version,
        } => {
            let client = db::connect(db_url).await?;
            commands::apply(&client, file, *force, *dry_run, *expected_version).await?;
        }
        Commands::Diff { file } => {
            let client = db::connect(db_url).await?;
            commands::diff(&client, file).await?;
        }
        Commands::Introspect {
            source,
            file,
            registered,
        } => {
            let client = if *registered {
                Some(db::connect(db_url).await?)
            } else {
                None
            };
            commands::introspect(source, file, *registered, client.as_ref()).await?;
        }
        Commands::Search {
            source,
            file,
            domain,
        } => {
            commands::search(source, file, domain.as_deref()).await?;
        }
        Commands::Add {
            source,
            table,
            file,
            domain,
        } => {
            commands::add(source, table, domain.as_deref(), file).await?;
        }
        Commands::TestConnection { file } => {
            commands::test_connection(file).await?;
        }
        Commands::Describe { file, domain } => {
            let client = db::connect(db_url).await?;
            commands::describe(&client, file, domain.as_deref()).await?;
        }
        Commands::Domain { subcommand } => {
            let client = db::connect(db_url).await?;
            match subcommand {
                DomainCommands::List => {
                    commands::list_domains(&client).await?;
                }
                DomainCommands::History { name } => {
                    commands::show_domain_history(&client, name.to_string()).await?;
                }
                DomainCommands::Rollback { name, to_version } => {
                    commands::rollback(&client, name, *to_version).await?;
                }
            }
        }
    }
    Ok(())
}
