use anyhow::Result;
use arrow::array::{Array, Int64Array};
use std::collections::HashMap;
use strake_common::config::{Config, ResourceConfig, SourceConfig};
use strake_runtime::federation::{FederationEngine, FederationEngineOptions};
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_cross_source_interval_join_domain_propagation() -> Result<()> {
    // Database A: operational intervals
    let db_a_file = NamedTempFile::new()?;
    let db_a_path = db_a_file.path().to_str().unwrap().to_string();

    {
        let conn = rusqlite::Connection::open(&db_a_path)?;
        conn.execute(
            "CREATE TABLE interval_table (
                entity_id INTEGER,
                start_time INTEGER,
                end_time INTEGER,
                equipment_id TEXT
            )",
            [],
        )?;

        // Insert historical intervals (mostly outside [2000, 2100])
        conn.execute(
            "INSERT INTO interval_table VALUES (1, 100, 200, 'EQ-1')",
            [],
        )?;
        conn.execute(
            "INSERT INTO interval_table VALUES (2, 500, 600, 'EQ-1')",
            [],
        )?;
        conn.execute(
            "INSERT INTO interval_table VALUES (3, 2000, 2100, 'EQ-1')",
            [],
        )?;
        conn.execute(
            "INSERT INTO interval_table VALUES (4, 3000, 3100, 'EQ-1')",
            [],
        )?;
        conn.execute(
            "INSERT INTO interval_table VALUES (5, 2000, 2100, 'EQ-OTHER')",
            [],
        )?;
    }

    // Database B: process events
    let db_b_file = NamedTempFile::new()?;
    let db_b_path = db_b_file.path().to_str().unwrap().to_string();

    {
        let conn = rusqlite::Connection::open(&db_b_path)?;
        conn.execute(
            "CREATE TABLE event_table (
                entity_id INTEGER,
                event_time INTEGER
            )",
            [],
        )?;

        conn.execute("INSERT INTO event_table VALUES (3, 2050)", [])?;
        conn.execute("INSERT INTO event_table VALUES (4, 3050)", [])?;
    }

    let mut config = Config::default();

    let mut sa = SourceConfig::default();
    sa.name = "source_a".into();
    sa.source_type = strake_common::models::SourceType::Other("sql".to_string());
    sa.url = Some(db_a_path.clone());
    sa.config = serde_json::to_value(HashMap::from([
        ("dialect".to_string(), "sqlite".to_string()),
        ("connection".to_string(), db_a_path.clone()),
    ]))?;

    let mut sb = SourceConfig::default();
    sb.name = "source_b".into();
    sb.source_type = strake_common::models::SourceType::Other("sql".to_string());
    sb.url = Some(db_b_path.clone());
    sb.config = serde_json::to_value(HashMap::from([
        ("dialect".to_string(), "sqlite".to_string()),
        ("connection".to_string(), db_b_path.clone()),
    ]))?;

    config.sources = vec![sa, sb];

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

    let query = r#"
        SELECT DISTINCT inv.entity_id
        FROM source_a.interval_table AS inv
        JOIN source_b.event_table AS ev
          ON ev.entity_id = inv.entity_id
         AND ev.event_time >= inv.start_time
         AND ev.event_time <= inv.end_time
        WHERE ev.event_time >= 2000
          AND ev.event_time <= 2100
          AND inv.equipment_id = 'EQ-1'
    "#;

    let tree = engine.explain_tree(query).await?;
    println!("Execution plan:\n{}", tree);

    let tree_signal = tree
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric()
                || matches!(
                    character,
                    '"' | '<' | '>' | '=' | '_' | '.' | ':' | '(' | ')' | ','
                )
        })
        .collect::<String>()
        .to_ascii_lowercase();
    assert!(
        tree_signal.contains("\"start_time\"<=2100"),
        "Expected derived start_time <= 2100 filter in plan:\n{}",
        tree
    );
    assert!(
        tree_signal.contains("\"end_time\">=2000"),
        "Expected derived end_time >= 2000 filter in plan:\n{}",
        tree
    );

    // Execute query and verify correctness
    let (_schema, batches, _warnings) = engine.execute_query(query, None).await?;
    assert!(!batches.is_empty(), "Expected query to return batches");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "Expected exactly 1 matching row");

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Downcast to Int64Array");

    assert_eq!(ids.value(0), 3, "Expected entity_id = 3");

    Ok(())
}
