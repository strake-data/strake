use strake_core::config::Config;
use strake_core::federation::FederationEngine;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config = Config {
        sources: vec![],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(strake_core::federation::FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: strake_core::config::QueryLimits::default(),
        resource_config: strake_core::config::ResourceConfig::default(),
        datafusion_config: std::collections::HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
    })
    .await?;

    // Example query
    let sql = r#"
        SELECT 
            first_name, 
            last_name, 
            job_title 
        FROM metadata 
        WHERE last_payment_amount > 500 
        LIMIT 10
    "#;

    tracing::info!("Executing federated query with trace");

    // Execute with trace
    engine.execute_query_with_trace(sql).await?;

    // match engine.execute_query(sql).await {
    //     Ok(batches) => {
    //         for batch in batches {
    //             println!("{:?}", batch);
    //         }
    //     }
    //     Err(e) => {
    //         tracing::error!("Query failed: {}", e);
    //         return Err(e);
    //     }
    // }

    Ok(())
}
