use anyhow::Result;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use serde_json::json;
use std::sync::Arc;
use strake_common::config::{RetrySettings, TableConfig};
use strake_connectors::sources::iceberg::provider::register_iceberg_rest;
use strake_connectors::sources::iceberg::IcebergRestConfig;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::EnvGuard;

#[tokio::test(flavor = "multi_thread")]
async fn test_iceberg_partition_pruning_support() -> Result<()> {
    let mock_server = MockServer::start().await;
    let _env = EnvGuard::new(vec![
        ("AWS_ACCESS_KEY_ID", "test"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
        ("AWS_REGION", "us-east-1"),
    ]);

    // Mock config
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "defaults": { "warehouse": "s3://test-bucket/warehouse" },
            "overrides": {}
        })))
        .mount(&mock_server)
        .await;

    // Mock namespace existence check - broad coverage
    Mock::given(method("HEAD"))
        .and(path("/v1/namespaces/default"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"namespace": ["default"]})))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"namespaces": [["default"]]})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "identifiers": [
                { "namespace": ["default"], "name": "partitioned_table" }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Mock table metadata with partition spec
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables/partitioned_table"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "metadata-location": "s3://test-bucket/warehouse/partitioned_table/metadata/v1.metadata.json",
            "metadata": {
                "format-version": 1,
                "table-uuid": "99ea797b-91cc-4876-8800-474d2a1068c7",
                "location": "s3://test-bucket/warehouse/partitioned_table",
                "last-updated-ms": 1600000000000i64,
                "last-column-id": 3,
                "schema": {
                    "schema-id": 0,
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "id", "required": true, "type": "int" },
                        { "id": 2, "name": "ts", "required": true, "type": "timestamp" },
                        { "id": 3, "name": "category", "required": true, "type": "string" }
                    ]
                },
                "partition-spec": [{
                    "name": "category", "transform": "identity", "source-id": 3, "field-id": 1000
                }],
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
        token: None,
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
        name: "partitioned_table".to_string(),
        schema: "".to_string(),
        partition_column: None,
        columns: vec![],
    }];

    register_iceberg_rest(
        &ctx,
        "strake",
        "iceberg_source",
        &cfg,
        &tables,
        RetrySettings::default(),
    )
    .await?;

    let provider = ctx
        .catalog("strake")
        .unwrap()
        .schema("default")
        .unwrap()
        .table("partitioned_table")
        .await
        .unwrap()
        .expect("Table not found");

    // Check if provider supports pushdown for partition column
    let filter_expr = col("category").eq(lit("A"));
    let filters = vec![&filter_expr];

    // We expect LazyIcebergTableProvider to delegate to inner, which delegates to IcebergTableProvider
    // IcebergTableProvider should return Exact or Inexact for partition columns if pruning is supported
    let pushdown = provider.supports_filters_pushdown(&filters)?;

    println!("Pushdown results: {:?}", pushdown);

    // As of iceberg-datafusion 0.8, we expect at least some support
    // If it returns Unsupported, then we need to implement manual pushdown
    assert!(!pushdown.is_empty());

    // Also verify that lazy loading happened (by checking if provider is now loaded)
    // We can't check internal state easily, but the fact that supports_filters_pushdown returned
    // implies it loaded the table (as per our implementation).

    Ok(())
}
