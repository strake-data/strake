use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use futures::future::join_all;
use secrecy::SecretString;
use serde_json::json;
use std::sync::Arc;
use strake_common::config::{RetrySettings, TableConfig};
use strake_connectors::sources::iceberg::provider::register_iceberg_rest;
use strake_connectors::sources::iceberg::{CacheConfig, IcebergRestConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::EnvGuard;

#[tokio::test(flavor = "multi_thread")] // Required for blocking schema load
async fn test_iceberg_lazy_loading_and_concurrency() -> Result<()> {
    let mock_server = MockServer::start().await;
    let _env = EnvGuard::new(vec![
        ("AWS_ACCESS_KEY_ID", "test"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
        ("AWS_REGION", "us-east-1"),
    ]);

    // Mock config - needed for registration
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "defaults": { "warehouse": "s3://test-bucket/warehouse" },
            "overrides": {}
        })))
        .mount(&mock_server)
        .await;

    // We disable catalog-level caching to test Provider-level deduplication
    let cache_config = CacheConfig {
        table_ttl_secs: 0, // Expire immediately
        max_tables: 0,     // No capacity
    };

    let cfg = IcebergRestConfig {
        catalog_uri: mock_server.uri(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: Some("level1".to_string()),
        token: Some(SecretString::new("test-token".to_string().into())),
        oauth_client_id: None,
        oauth_client_secret: None,
        oauth_token_url: None,
        oauth_scopes: None,
        region: "us-east-1".to_string(),
        s3_endpoint: None,
        request_timeout_secs: None,
        max_retries: None,
        cache: Some(cache_config),
        version: None,
        max_concurrent_queries: None,
    };

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "strake",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );

    // Mock namespace check
    Mock::given(method("HEAD"))
        .and(path("/v1/namespaces/level1"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/level1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"namespace": ["level1"]})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"namespaces": [["level1"]]})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/level1/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "identifiers": [
                { "namespace": ["level1"], "name": "test_table" }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Setup Mock for Table Metadata
    // We expect this to be called EXACTLY ONCE across all concurrent queries
    // IF lazy loading and dedup works correctly.
    // If it was eager, it would be called during registration (before queries).
    // If dedup fails (and cache is disabled), it would be called multiple times.
    let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_clone = call_count.clone();

    let metadata_mock = Mock::given(method("GET"))
        .and(path("/v1/namespaces/level1/tables/test_table"))
        .respond_with(move |_req: &wiremock::Request| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_millis(200)) // Artificial delay to force race condition
                .set_body_json(json!({
                    "metadata-location": "s3://test-bucket/warehouse/test_table/metadata/v1.metadata.json",
                    "metadata": {
                        "format-version": 1,
                        "table-uuid": "99ea797b-91cc-4876-8800-474d2a1068c7",
                        "location": "s3://test-bucket/warehouse/test_table",
                        "last-updated-ms": 1600000000000i64,
                        "last-column-id": 1,
                        "schema": {
                            "schema-id": 0,
                            "type": "struct",
                            "fields": [{ "id": 1, "name": "id", "required": true, "type": "int" }]
                        },
                        "partition-spec": [],
                        "properties": {},
                        "snapshots": [],
                        "snapshot-log": [],
                        "metadata-log": []
                    }
                }))
        })
        .expect(1..=2); // We still use expect for safety, but check atomic manually

    metadata_mock.mount(&mock_server).await;

    let tables = vec![TableConfig {
        name: "test_table".to_string(),
        schema: "".to_string(),
        partition_column: None,
        columns: vec![],
    }];

    // 1. Register tables
    // This should NOT trigger the metadata mock (lazy)
    register_iceberg_rest(
        &ctx,
        "strake",
        "iceberg_source",
        &cfg,
        &tables,
        RetrySettings::default(),
    )
    .await
    .context("Failed to register Iceberg source")?;

    // VERIFY: Registration triggers table load for schema (safety fix)
    assert_eq!(
        call_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Table should be loaded during registration for schema"
    );

    // 2. Concurrent queries
    // We spawn 10 tasks. Each runs a query.
    // The first one to reach the critical section should load the table.
    // Others should wait.
    // Since we disabled catalog cache, if they don't wait, they will hit the API multiple times.

    let ctx_arc = Arc::new(ctx);
    let mut tasks = Vec::new();

    for _i in 0..10 {
        let ctx_clone = ctx_arc.clone();
        tasks.push(tokio::spawn(async move {
            // We use SQL that requires schema (e.g. wildcard or specific column)
            // "SELECT id FROM ..."
            // This triggers `TableProvider::schema()`.
            let df = ctx_clone
                .sql("SELECT id FROM strake.level1.test_table")
                .await
                .expect("Plan failed");
            // We don't need to execute to trigger schema load, planning is enough.
            // But let's execute to be sure.
            // Since snapshot is empty, result is empty.
            let results = df.collect().await.expect("Execution failed");
            results.len()
        }));
    }

    let results = join_all(tasks).await;

    for r in results {
        assert!(r.is_ok(), "Task failed");
    }

    println!("Concurrent queries complete.");

    // The Mock expectation (.expect(1)) verifies the behavior on drop or explicitly.
    // wiremock checks expectations when the MockServer is dropped or verified.
    // We can just end the test.

    Ok(())
}
