use strake_server::StrakeServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    StrakeServer::new().run().await
}
