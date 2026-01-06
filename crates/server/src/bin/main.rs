use strake_server::StrakeServer;

#[derive(clap::Parser)]
struct Args {
    #[arg(long, default_value = "config/strake.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = <Args as clap::Parser>::parse();

    // Load AppConfig to get server address
    use strake_core::config::AppConfig;
    let app_config = AppConfig::from_file(&args.config).unwrap_or_default();

    println!("--------------------------------------------------");
    println!("   Strake Community Edition");
    println!("   Licensed to: Open Source (Apache-2.0)");
    println!("   Tier:        community");
    println!("   Features:    [\"core\", \"flight-sql\", \"federation\"]");
    println!("   Server Addr: {}", app_config.server.listen_addr);
    println!("--------------------------------------------------");

    StrakeServer::new()
        .with_app_config(&args.config)
        .run()
        .await
}
