use anyhow::Result;
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use std::collections::HashMap;
use strake_common::config::{Config, ResourceConfig, SourceConfig};
use strake_runtime::federation::{FederationEngine, FederationEngineOptions};
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_same_source_cte_join_aggregate_pushdown() -> Result<()> {
    let db_file = NamedTempFile::new()?;
    let db_path = db_file.path().to_str().unwrap().to_string();

    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            [],
        )?;
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            [],
        )?;
        conn.execute(
            "CREATE TABLE details (user_id INTEGER PRIMARY KEY, status TEXT)",
            [],
        )?;

        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])?;
        conn.execute("INSERT INTO users VALUES (2, 'Bob', 20)", [])?;
        conn.execute("INSERT INTO orders VALUES (100, 1, 50.5)", [])?;
        conn.execute("INSERT INTO orders VALUES (101, 1, 49.5)", [])?;
        conn.execute("INSERT INTO orders VALUES (102, 2, 200.0)", [])?;
        conn.execute("INSERT INTO details VALUES (1, 'ACTIVE')", [])?;
        conn.execute("INSERT INTO details VALUES (2, 'INACTIVE')", [])?;
    }

    let mut config = Config::default();
    let mut s1 = SourceConfig::default();
    s1.name = "s1".into();
    s1.source_type = strake_common::models::SourceType::Other("sql".to_string());
    s1.url = Some(db_path.clone());
    s1.config = serde_json::to_value(HashMap::from([
        ("dialect".to_string(), "sqlite".to_string()),
        ("connection".to_string(), db_path.clone()),
    ]))?;
    config.sources = vec![s1];

    let engine = FederationEngine::new(FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: Default::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let sql = r#"
        WITH active_users AS (
            SELECT id, name FROM s1.users WHERE age >= 25
        )
        SELECT u.name, SUM(o.amount) as total
        FROM active_users u
        JOIN s1.orders o ON u.id = o.user_id
        JOIN s1.details d ON u.id = d.user_id
        WHERE d.status = 'ACTIVE'
        GROUP BY u.name
    "#;

    let tree = engine.explain_tree(sql).await?;
    let tree_lower = tree.to_lowercase();

    // Verify exactly 1 pushed execution block
    let fed_exec_count = tree_lower.matches("strakefederation").count();
    assert_eq!(
        fed_exec_count, 1,
        "Same-source subplan should push down into exactly 1 StrakeFederationExec, got plan:\n{}",
        tree
    );

    // Verify local DataFusion joins are absent
    assert!(
        !tree_lower.contains("hashjoin"),
        "Local HashJoinExec found in same-source plan:\n{}",
        tree
    );

    // Execute query and verify results
    let (_schema, batches, _warnings) = engine.execute_query(sql, None).await?;
    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(names.value(0), "Alice");
    assert_eq!(totals.value(0), 100.0);

    Ok(())
}

#[tokio::test]
async fn test_same_source_semi_join_pushdown() -> Result<()> {
    let db_file = NamedTempFile::new()?;
    let db_path = db_file.path().to_str().unwrap().to_string();

    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])?;
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            [],
        )?;

        conn.execute("INSERT INTO users VALUES (1, 'Alice')", [])?;
        conn.execute("INSERT INTO users VALUES (2, 'Bob')", [])?;
        conn.execute("INSERT INTO orders VALUES (100, 1, 50.0)", [])?;
    }

    let mut config = Config::default();
    let mut s1 = SourceConfig::default();
    s1.name = "s1".into();
    s1.source_type = strake_common::models::SourceType::Other("sql".to_string());
    s1.url = Some(db_path.clone());
    s1.config = serde_json::to_value(HashMap::from([
        ("dialect".to_string(), "sqlite".to_string()),
        ("connection".to_string(), db_path.clone()),
    ]))?;
    config.sources = vec![s1];

    let engine = FederationEngine::new(FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: Default::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let sql = r#"
        SELECT u.name FROM s1.users u
        WHERE EXISTS (
            SELECT 1 FROM s1.orders o WHERE o.user_id = u.id AND o.amount > 10.0
        )
        ORDER BY u.name
    "#;

    let tree = engine.explain_tree(sql).await?;
    let tree_lower = tree.to_lowercase();

    // Verify semi-join pushes down as single subplan
    let fed_exec_count = tree_lower.matches("strakefederation").count();
    assert_eq!(
        fed_exec_count, 1,
        "Semi-join subplan should push down as 1 StrakeFederationExec, got plan:\n{}",
        tree
    );

    // Verify EXISTS condition in generated SQL
    assert!(
        tree_lower.contains("exists"),
        "Pushed SQL should contain EXISTS subquery for semi-join:\n{}",
        tree
    );

    let (_schema, batches, _warnings) = engine.execute_query(sql, None).await?;
    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");

    Ok(())
}

#[tokio::test]
async fn test_mixed_source_join_preserves_independent_leaves() -> Result<()> {
    let db1 = NamedTempFile::new()?;
    let db1_path = db1.path().to_str().unwrap().to_string();
    let db2 = NamedTempFile::new()?;
    let db2_path = db2.path().to_str().unwrap().to_string();

    {
        let conn = rusqlite::Connection::open(&db1_path)?;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])?;
        conn.execute("INSERT INTO users VALUES (1, 'Alice')", [])?;
    }

    {
        let conn = rusqlite::Connection::open(&db2_path)?;
        conn.execute(
            "CREATE TABLE details (user_id INTEGER PRIMARY KEY, score INTEGER)",
            [],
        )?;
        conn.execute("INSERT INTO details VALUES (1, 95)", [])?;
    }

    let mut config = Config::default();
    let mut s1 = SourceConfig::default();
    s1.name = "src1".into();
    s1.source_type = strake_common::models::SourceType::Other("sql".to_string());
    s1.url = Some(db1_path.clone());
    s1.config = serde_json::to_value(HashMap::from([
        ("dialect".to_string(), "sqlite".to_string()),
        ("connection".to_string(), db1_path),
    ]))?;

    let mut s2 = SourceConfig::default();
    s2.name = "src2".into();
    s2.source_type = strake_common::models::SourceType::Other("sql".to_string());
    s2.url = Some(db2_path.clone());
    s2.config = serde_json::to_value(HashMap::from([
        ("dialect".to_string(), "sqlite".to_string()),
        ("connection".to_string(), db2_path),
    ]))?;

    config.sources = vec![s1, s2];

    let engine = FederationEngine::new(FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: Default::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let sql = "SELECT u.name, d.score FROM src1.users u JOIN src2.details d ON u.id = d.user_id";

    let tree = engine.explain_tree(sql).await?;
    let tree_lower = tree.to_lowercase();

    // Verify 2 independent leaf pushdowns (1 per source)
    let fed_exec_count = tree_lower.matches("strakefederation").count();
    assert_eq!(
        fed_exec_count, 2,
        "Mixed-source query should produce 2 StrakeFederationExec leaves, got plan:\n{}",
        tree
    );

    // Verify local DataFusion HashJoin is present for cross-source join
    assert!(
        tree_lower.contains("hashjoin"),
        "Local HashJoinExec must be present for cross-source join:\n{}",
        tree
    );

    let (_schema, batches, _warnings) = engine.execute_query(sql, None).await?;
    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(names.value(0), "Alice");
    assert_eq!(scores.value(0), 95);

    Ok(())
}
