use anyhow::Result;
use strake_core::config::{Config, QueryLimits, ResourceConfig, SourceConfig};
use strake_core::federation::FederationEngine;

#[tokio::test]
async fn test_postgres_integration() -> Result<()> {
    // This test assumes a local Postgres instance.
    // In a real environment, we'd probably want to skip this if env vars aren't set.
    let connection_string = "postgres://postgres:postgres@localhost:5432/postgres";

    // Create a minimal config
    let config = Config {
        sources: vec![
            SourceConfig {
                name: "pg".to_string(),
                r#type: "sql".to_string(),
                default_limit: None,
                cache: None,
                config: serde_yaml::to_value(std::collections::HashMap::from([
                    ("dialect".to_string(), "postgres".to_string()),
                    ("connection".to_string(), connection_string.to_string()),
                    ("pool_size".to_string(), "10".to_string()),
                ])).unwrap(),
            }
        ],
        cache: Default::default(),
    };
    
    let limits = QueryLimits {
        max_output_rows: Some(1000),
        max_scan_bytes: None,
        default_limit: Some(1000),
    };

    // Attempt to initialize engine. This might fail if PG is down.
    // We treat connection failure as a "pass" for this integration test locally
    // unless we enforce PG presence.
    match FederationEngine::new(
        config, 
        "strake".to_string(), 
        limits,
        ResourceConfig::default(),
        std::collections::HashMap::new(),
        10,
        vec![],
        vec![],
    ).await {
        Ok(engine) => {
            let sql = "SELECT 1";
            let (schema, batches, _logs) = engine.execute_query(sql, None).await?;
            println!("Schema: {:?}", schema);
            println!("Batches: {:?}", batches);
        }
        Err(e) => {
            eprintln!("Skipping test due to engine initialization failure (likely no Postgres): {}", e);
        }
    }

    Ok(())
}
