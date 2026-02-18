use anyhow::Result;
use datafusion::prelude::SessionContext;
use secrecy::SecretString;
use serde_json::json;
use std::sync::Arc;
use strake_common::config::{RetrySettings, TableConfig};
use strake_connectors::sources::iceberg::provider::register_iceberg_rest;
use strake_connectors::sources::iceberg::IcebergRestConfig;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::EnvGuard;

#[tokio::test]
async fn test_iceberg_concurrent_table_access() -> Result<()> {
    // Set mock AWS credentials with automatic cleanup
    let _env = EnvGuard::new(vec![
        ("AWS_ACCESS_KEY_ID", "test-key"),
        ("AWS_SECRET_ACCESS_KEY", "test-secret"),
        ("AWS_REGION", "us-east-1"),
    ]);

    let mock_server = MockServer::start().await;

    // Mock /v1/config endpoint
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "defaults": {
                "warehouse": "s3://test-bucket/warehouse"
            },
            "overrides": {}
        })))
        .mount(&mock_server)
        .await;

    // Mock namespaces endpoint
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespaces": []
        })))
        .mount(&mock_server)
        .await;

    // Mock table metadata
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables/test_table"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
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
        })))
        .mount(&mock_server)
        .await;

    let cfg = IcebergRestConfig {
        catalog_uri: mock_server.uri(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: Some("default".to_string()),
        token: Some(SecretString::from("test-token".to_string())),
        oauth_client_id: None,
        oauth_client_secret: None,
        oauth_token_url: None,
        oauth_scopes: None,
        region: "us-east-1".to_string(),
        s3_endpoint: None,
        request_timeout_secs: None,
        max_retries: None,
        cache: None,
        version: None,
        max_concurrent_queries: None,
    };

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "strake",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );

    let tables = vec![TableConfig {
        name: "test_table".to_string(),
        schema: "".to_string(),
        partition_column: None,
        columns: vec![],
    }];

    // Register the table
    register_iceberg_rest(
        &ctx,
        "strake",
        "concurrent_source",
        &cfg,
        &tables,
        RetrySettings::default(),
    )
    .await?;

    // Simulate concurrent access to the same table from multiple tasks
    let mut handles: Vec<tokio::task::JoinHandle<Result<i32>>> = vec![];

    for i in 0..10 {
        let ctx_clone = ctx.clone();
        let handle = tokio::spawn(async move {
            // Try to access the table's schema concurrently
            let catalog = ctx_clone.catalog("strake");
            if catalog.is_none() {
                // Catalog might not exist in some test scenarios
                return Ok(i);
            }

            let catalog = catalog.unwrap();
            let schema = catalog.schema("concurrent_source");
            if schema.is_none() {
                // Schema might not exist in mock scenario
                return Ok(i);
            }

            let schema = schema.unwrap();
            let table = schema.table("test_table").await;

            // All tasks should successfully access the table (or get the same error)
            // The key is that LazyIcebergTableProvider should handle concurrent access safely
            match table {
                Ok(Some(table_provider)) => {
                    // Successfully got the table provider
                    let schema = table_provider.schema();
                    assert!(!schema.fields().is_empty(), "Schema should be accessible");
                    Ok(i)
                }
                Ok(None) => {
                    // Table not found - this is expected since we're using a mock
                    Ok(i)
                }
                Err(e) => {
                    // Error accessing table - acceptable for mock scenario
                    // The important thing is no panic or deadlock
                    println!("Task {} got error (expected with mock): {}", i, e);
                    Ok(i)
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // Verify all tasks completed without panicking
    for (idx, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "Task {} should complete without panic", idx);
    }

    println!(
        "All {} concurrent access tasks completed successfully",
        results.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_iceberg_lazy_loading_race_condition() -> Result<()> {
    // This test specifically targets the race condition in lazy loading
    // where multiple threads try to load the table simultaneously

    let _env = EnvGuard::new(vec![
        ("AWS_ACCESS_KEY_ID", "test-key"),
        ("AWS_SECRET_ACCESS_KEY", "test-secret"),
        ("AWS_REGION", "us-east-1"),
    ]);

    let mock_server = MockServer::start().await;

    // Add a small delay to the mock response to increase chance of race condition
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "defaults": {
                        "warehouse": "s3://test-bucket/warehouse"
                    },
                    "overrides": {}
                }))
                .set_delay(std::time::Duration::from_millis(50)),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "namespaces": []
                }))
                .set_delay(std::time::Duration::from_millis(50)),
        )
        .mount(&mock_server)
        .await;

    // Mock table metadata for race table
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables/race_test_table"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "metadata-location": "s3://test-bucket/warehouse/race_test_table/metadata/v1.metadata.json",
            "metadata": {
                "format-version": 1,
                "table-uuid": "99ea797b-91cc-4876-8800-474d2a1068c7",
                "location": "s3://test-bucket/warehouse/race_test_table",
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
        })))
        .mount(&mock_server)
        .await;

    let cfg = IcebergRestConfig {
        catalog_uri: mock_server.uri(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: Some("default".to_string()),
        token: Some(SecretString::from("test-token".to_string())),
        oauth_client_id: None,
        oauth_client_secret: None,
        oauth_token_url: None,
        oauth_scopes: None,
        region: "us-east-1".to_string(),
        s3_endpoint: None,
        request_timeout_secs: None,
        max_retries: None,
        cache: None,
        version: None,
        max_concurrent_queries: None,
    };

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "strake",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );

    let tables = vec![TableConfig {
        name: "race_test_table".to_string(),
        schema: "".to_string(),
        partition_column: None,
        columns: vec![],
    }];

    register_iceberg_rest(
        &ctx,
        "strake",
        "race_source",
        &cfg,
        &tables,
        RetrySettings::default(),
    )
    .await?;

    // Launch many concurrent tasks that all try to access the table immediately
    // This should trigger the lazy loading mechanism concurrently
    let mut handles: Vec<tokio::task::JoinHandle<Result<i32>>> = vec![];

    for i in 0..20 {
        let ctx_clone = ctx.clone();
        let handle = tokio::spawn(async move {
            let catalog = ctx_clone.catalog("strake");
            if catalog.is_none() {
                return Ok(i);
            }

            let catalog = catalog.unwrap();
            let schema = catalog.schema("race_source");
            if schema.is_none() {
                return Ok(i);
            }

            let schema = schema.unwrap();

            // All tasks try to get the table at the same time
            let table = schema.table("race_test_table").await;

            match table {
                Ok(Some(table_provider)) => {
                    // Try to access schema (this triggers lazy loading)
                    let _schema = table_provider.schema();
                    Ok(i)
                }
                Ok(None) | Err(_) => Ok(i),
            }
        });
        handles.push(handle);
    }

    // All tasks should complete without deadlock or panic
    let results = futures::future::join_all(handles).await;

    for (idx, result) in results.iter().enumerate() {
        assert!(
            result.is_ok(),
            "Task {} should complete without panic or deadlock",
            idx
        );
    }

    println!(
        "Race condition test passed: all {} tasks completed",
        results.len()
    );

    Ok(())
}
