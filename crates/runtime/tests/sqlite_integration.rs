use anyhow::Result;
use arrow::array::Array;
use arrow::array::StringArray;
use std::collections::HashMap;
use strake_common::config::{Config, ResourceConfig, SourceConfig};
use strake_runtime::federation::FederationEngine;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_sqlite_integration() -> Result<()> {
    let temp_db = NamedTempFile::new()?;
    let db_path = temp_db.path().to_str().unwrap().to_string();

    // Create some data in SQLite using rusqlite
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])?;
        conn.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])?;
        conn.execute("INSERT INTO users (name) VALUES (?)", ["Bob"])?;
    }

    let config = Config {
        sources: vec![SourceConfig {
            name: "my_sqlite".to_string(),
            source_type: "sql".to_string(),
            url: Some(db_path.clone()),
            default_limit: None,
            cache: None,
            config: serde_json::to_value(HashMap::from([
                ("dialect".to_string(), "sqlite".to_string()),
                ("connection".to_string(), db_path.clone()),
            ]))
            .unwrap(),
            username: None,
            password: None,
            max_concurrent_queries: None,
            tables: vec![],
        }],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: strake_common::config::QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
    })
    .await?;

    let sql = "SELECT name FROM my_sqlite.users ORDER BY name";
    let (_schema, batches, _messages) = engine.execute_query(sql, None).await?;

    // Validate results
    assert!(!batches.is_empty(), "No batches returned");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Failed to downcast to StringArray");

    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Bob");

    Ok(())
}

#[tokio::test]
async fn test_sqlite_cross_db_federation() -> Result<()> {
    let db1 = NamedTempFile::new()?;
    let db1_path = db1.path().to_str().unwrap().to_string();
    let db2 = NamedTempFile::new()?;
    let db2_path = db2.path().to_str().unwrap().to_string();

    // DB 1: users
    {
        let conn = rusqlite::Connection::open(&db1_path)?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])?;
        conn.execute(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            rusqlite::params![1, "Alice"],
        )?;
    }

    // DB 2: user_details
    {
        let conn = rusqlite::Connection::open(&db2_path)?;
        conn.execute(
            "CREATE TABLE user_details (user_id INTEGER PRIMARY KEY, age INTEGER)",
            [],
        )?;
        conn.execute(
            "INSERT INTO user_details (user_id, age) VALUES (?, ?)",
            rusqlite::params![1, 30],
        )?;
    }

    let config = Config {
        sources: vec![
            SourceConfig {
                name: "s1".to_string(),
                source_type: "sql".to_string(),
                url: Some(db1_path.clone()),
                default_limit: None,
                cache: None,
                config: serde_json::to_value(HashMap::from([
                    ("dialect".to_string(), "sqlite".to_string()),
                    ("connection".to_string(), db1_path),
                ]))
                .unwrap(),
                username: None,
                password: None,
                max_concurrent_queries: None,
                tables: vec![],
            },
            SourceConfig {
                name: "s2".to_string(),
                source_type: "sql".to_string(),
                url: Some(db2_path.clone()),
                default_limit: None,
                cache: None,
                config: serde_json::to_value(HashMap::from([
                    ("dialect".to_string(), "sqlite".to_string()),
                    ("connection".to_string(), db2_path),
                ]))
                .unwrap(),
                username: None,
                password: None,
                max_concurrent_queries: None,
                tables: vec![],
            },
        ],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: strake_common::config::QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
    })
    .await?;

    let sql = "SELECT u.name, d.age FROM s1.users u JOIN s2.user_details d ON u.id = d.user_id";
    let (_schema, batches, _messages) = engine.execute_query(sql, None).await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(names.value(0), "Alice");
    assert_eq!(ages.value(0), 30);

    Ok(())
}
