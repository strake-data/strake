//! Strake Server Executable.
//!
//! Launches the multi-protocol federation server.
//!
//! # CLI Arguments
//!
//! - `--config`: Path to `strake.yaml` (server config)
//! - `--sources`: Path to `sources.yaml` (data sources)
//! - `--mcp`: Flag to enable the Python MCP sidecar
use strake_server::StrakeServer;

#[derive(clap::Parser)]
struct Args {
    #[arg(long, default_value = "config/strake.yaml")]
    config: String,
    #[arg(long, default_value = "config/sources.yaml")]
    sources: String,

    /// Start the MCP sidecar agent
    #[arg(long)]
    mcp: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = <Args as clap::Parser>::parse();

    // Load AppConfig to get server address
    // Load AppConfig to get server address
    use strake_common::config::AppConfig;
    let mut app_config = AppConfig::from_file(&args.config).unwrap_or_default();

    if args.mcp {
        app_config.mcp.enabled = true;
    }

    // Spawn sidecar (if enabled in config or via override)
    let _sidecar_handle = strake_runtime::sidecar::spawn_sidecar(&app_config).await?;

    println!("--------------------------------------------------");
    println!("   Strake Community Edition");
    println!("   Licensed to: Open Source (Apache-2.0)");
    println!("   Tier:        community");
    println!("   Features:    [\"core\", \"flight-sql\", \"federation\"]");
    println!("   Server Addr: {}", app_config.server.listen_addr);
    println!("--------------------------------------------------");

    // Load sources path from environment or CLI argument
    let sources_path = std::env::var("SOURCES_YAML").unwrap_or(args.sources);

    StrakeServer::new()
        .with_app_config(&args.config)
        .with_config(&sources_path)
        .run()
        .await
}
