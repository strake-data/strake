use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use secrecy::SecretString;
use serde_json::json;
use std::sync::Arc;
use strake_common::config::{RetrySettings, TableConfig};
use strake_connectors::sources::iceberg::provider::register_iceberg_rest;
use strake_connectors::sources::iceberg::IcebergRestConfig;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::EnvGuard;

#[tokio::test]
async fn test_iceberg_oauth_flow() -> Result<()> {
    // Set mock AWS credentials with automatic cleanup on test completion
    let _env = EnvGuard::new(vec![
        ("AWS_ACCESS_KEY_ID", "test-key"),
        ("AWS_SECRET_ACCESS_KEY", "test-secret"),
        ("AWS_REGION", "us-east-1"),
    ]);

    // 1. Start Mock Server
    let mock_server = MockServer::start().await;

    // 2. Mock OAuth Token Endpoint
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "mock_oauth_token_123",
            "token_type": "Bearer",
            "expires_in": 3600
        })))
        .mount(&mock_server)
        .await;

    // 3. Mock Iceberg REST Config Endpoint
    // This expects the Authorization header to be present with the token we just issued
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(header("Authorization", "Bearer mock_oauth_token_123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "defaults": {
                "warehouse": "s3://test-bucket/warehouse"
            },
            "overrides": {}
        })))
        .mount(&mock_server)
        .await;

    // Mock generic namespaces endpoint to allow registration to proceed further
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(header("Authorization", "Bearer mock_oauth_token_123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespaces": []
        })))
        .mount(&mock_server)
        .await;

    // Mock generic tables discovery
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables"))
        .and(header("Authorization", "Bearer mock_oauth_token_123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "identifiers": [
                { "namespace": ["default"], "name": "test_table" }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Mock table metadata
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables/test_table"))
        .and(header("Authorization", "Bearer mock_oauth_token_123"))
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

    // 4. Configure Iceberg Source with OAuth
    let cfg = IcebergRestConfig {
        catalog_uri: mock_server.uri(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: Some("default".to_string()),
        token: None,
        oauth_client_id: Some("test-client-id".to_string()),
        oauth_client_secret: Some(SecretString::from("test-client-secret".to_string())),
        oauth_token_url: Some(format!("{}/oauth/token", mock_server.uri())),
        oauth_scopes: Some(vec!["catalog:read".to_string()]),
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

    // 5. Run Registration
    // This should triggers:
    // a. OAuth token fetch (POST /oauth/token)
    // b. Catalog config fetch (GET /v1/config) with Bearer token
    let tables = vec![TableConfig {
        name: "test_table".to_string(),
        schema: "".to_string(),
        partition_column: None,
        columns: vec![],
    }];

    register_iceberg_rest(
        &ctx,
        "strake",
        "oauth_source",
        &cfg,
        &tables,
        RetrySettings::default(),
    )
    .await
    .context("Failed to register Iceberg source with OAuth")?;

    // If we reached here, the mocks matched (including header check) and provided responses.

    Ok(())
}
