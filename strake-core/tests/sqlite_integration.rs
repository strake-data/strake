use anyhow::Result;
use arrow::array::Array;
use arrow::array::StringArray;
use std::collections::HashMap;
use strake_core::config::{Config, ResourceConfig, SourceConfig};
use strake_core::federation::FederationEngine;
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
            r#type: "sql".to_string(),
            default_limit: None,
            cache: None,
            config: serde_yaml::to_value(HashMap::from([
                ("dialect".to_string(), "sqlite".to_string()),
                ("connection".to_string(), db_path.clone()),
            ]))
            .unwrap(),
        }],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(
        config,
        "strake".to_string(),
        strake_core::config::QueryLimits::default(),
        ResourceConfig::default(),
        HashMap::new(),
        10,
        vec![],
        vec![],
    )
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
async fn test_sqlite_joins() -> Result<()> {
    let temp_db = NamedTempFile::new()?;
    let db_path = temp_db.path().to_str().unwrap().to_string();

    // Create users and orders in SQLite
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])?;
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            [],
        )?;

        conn.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])?;
        conn.execute("INSERT INTO users (name) VALUES (?)", ["Bob"])?;

        conn.execute(
            "INSERT INTO orders (user_id, amount) VALUES (?, ?)",
            rusqlite::params![1, 100.0],
        )?;
        conn.execute(
            "INSERT INTO orders (user_id, amount) VALUES (?, ?)",
            rusqlite::params![1, 50.0],
        )?;
        conn.execute(
            "INSERT INTO orders (user_id, amount) VALUES (?, ?)",
            rusqlite::params![2, 200.0],
        )?;
    }

    let config = Config {
        sources: vec![SourceConfig {
            name: "db".to_string(),
            r#type: "sql".to_string(),
            default_limit: None,
            cache: None,
            config: serde_yaml::to_value(HashMap::from([
                ("dialect".to_string(), "sqlite".to_string()),
                ("connection".to_string(), db_path.clone()),
            ]))
            .unwrap(),
        }],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(
        config,
        "strake".to_string(),
        strake_core::config::QueryLimits::default(),
        ResourceConfig::default(),
        HashMap::new(),
        10,
        vec![],
        vec![],
    )
    .await?;

    let sql = "SELECT u.name, SUM(o.amount) as total 
               FROM db.users u 
               JOIN db.orders o ON u.id = o.user_id 
               GROUP BY u.name 
               ORDER BY u.name";
    let (_schema, batches, _messages) = engine.execute_query(sql, None).await?;

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    // Column 0: name, Column 1: total
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

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
                r#type: "sql".to_string(),
                default_limit: None,
                cache: None,
                config: serde_yaml::to_value(HashMap::from([
                    ("dialect".to_string(), "sqlite".to_string()),
                    ("connection".to_string(), db1_path),
                ]))
                .unwrap(),
            },
            SourceConfig {
                name: "s2".to_string(),
                r#type: "sql".to_string(),
                default_limit: None,
                cache: None,
                config: serde_yaml::to_value(HashMap::from([
                    ("dialect".to_string(), "sqlite".to_string()),
                    ("connection".to_string(), db2_path),
                ]))
                .unwrap(),
            },
        ],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(
        config,
        "strake".to_string(),
        strake_core::config::QueryLimits::default(),
        ResourceConfig::default(),
        HashMap::new(),
        10,
        vec![],
        vec![],
    )
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
