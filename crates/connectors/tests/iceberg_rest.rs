use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use secrecy::SecretString;
use serde_json::json;
use std::sync::Arc;
use strake_common::config::{RetrySettings, TableConfig};
use strake_connectors::sources::iceberg::IcebergRestConfig;
use strake_connectors::sources::iceberg::provider::register_iceberg_rest;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

#[tokio::test]
async fn test_iceberg_rest_registration_mock() -> Result<()> {
    // No global env setup needed

    let mock_server = MockServer::start().await;

    // Mock /v1/config - common Iceberg REST endpoint
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

    // Mock /v1/namespaces - used to verify schema existence
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespaces": [["default"]]
        })))
        .mount(&mock_server)
        .await;

    // Mock /v1/namespaces/default
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespace": ["default"],
            "properties": {}
        })))
        .mount(&mock_server)
        .await;

    // Mock /v1/namespaces/default/tables - used to discover tables
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "identifiers": [
                {
                    "namespace": ["default"],
                    "name": "test_table"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Mock /v1/namespaces/default/tables/test_table
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/default/tables/test_table"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "metadata-location": "s3://test-bucket/warehouse/test_table/metadata/v1.metadata.json",
            "metadata": {
                "format-version": 2,
                "table-uuid": "99ea797b-91cc-4876-8800-474d2a1068c7",
                "location": "s3://test-bucket/warehouse/test_table",
                "last-sequence-number": 0,
                "last-updated-ms": 1627293123456i64,
                "last-column-id": 1,
                "current-schema-id": 0,
                "current-snapshot-id": null,
                "schemas": [
                    {
                        "schema-id": 0,
                        "type": "struct",
                        "fields": [
                            {
                                "id": 1,
                                "name": "id",
                                "required": true,
                                "type": "int",
                                "doc": "Column comment"
                            }
                        ]
                    }
                ],
                "default-spec-id": 0,
                "partition-specs": [
                    {
                        "spec-id": 0,
                        "fields": []
                    }
                ],
                "last-partition-id": 0,
                "default-sort-order-id": 0,
                "sort-orders": [
                    {
                        "order-id": 0,
                        "fields": []
                    }
                ],
                "snapshots": [],
                "properties": {
                    "comment": "Table description"
                }
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
        s3_access_key: Some("test-key".to_string()),
        s3_secret_key: Some(SecretString::from("test-secret".to_string())),
        s3_session_token: None,
        request_timeout_secs: None,
        max_retries: None,
        cache: None,
        version: None,
        max_concurrent_queries: None,
    };

    let ctx = Arc::new(SessionContext::new());
    // Register the target catalog first, as register_iceberg_rest expects it to exist
    ctx.register_catalog(
        "strake",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );

    let mut t = TableConfig::default();
    t.name = "test_table".to_string();
    t.schema = "".to_string();
    let tables = Arc::new(vec![t]);
    let cfg = Arc::new(cfg);

    // Verify that the registration flow succeeds.
    register_iceberg_rest(
        Arc::clone(&ctx),
        "strake".to_string(),
        "iceberg_source".to_string(),
        Arc::clone(&cfg),
        Arc::clone(&tables),
        RetrySettings::default(),
        Arc::new(strake_common::predicate_cache::PredicateCache::new()),
        true,
    )
    .await
    .context("Failed to register Iceberg source in test")?;

    // Logic reached: catalog initialization and namespace verification endpoints would have been hit.

    Ok(())
}

#[tokio::test]
async fn test_iceberg_invalid_warehouse_uri() -> Result<()> {
    use strake_common::config::SourceConfig;
    use strake_connectors::sources::SourceProvider;
    use strake_connectors::sources::iceberg::IcebergSourceProvider;

    let provider = IcebergSourceProvider {
        global_retry: RetrySettings::default(),
        predicate_cache: Arc::new(strake_common::predicate_cache::PredicateCache::new()),
    };

    let cfg_json = json!({
        "catalog_uri": "http://localhost:8181/v1",
        "warehouse": "invalid://bucket/warehouse",
        "region": "us-east-1"
    });

    let mut config = SourceConfig::default();
    config.name = strake_common::models::SourceName::from("invalid_iceberg");
    config.source_type = strake_common::models::SourceType::Other("iceberg_rest".to_string());
    config.config = cfg_json;

    let ctx = SessionContext::new();
    let result = provider.register(&ctx, "strake", &config).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Unsupported warehouse scheme") && err_msg.contains("invalid"));

    Ok(())
}
