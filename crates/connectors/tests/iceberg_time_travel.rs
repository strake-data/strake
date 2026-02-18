use anyhow::Result;
use secrecy::SecretString;
use strake_connectors::sources::iceberg::{IcebergRestConfig, TableVersionSpec};

/// Test time travel with snapshot ID
#[tokio::test]
#[ignore] // Requires real Iceberg catalog
async fn test_time_travel_snapshot() -> Result<()> {
    let cfg = IcebergRestConfig {
        catalog_uri: "http://localhost:8181/v1".to_string(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: Some("default".to_string()),
        token: Some(SecretString::new("test-token".to_string().into())),
        oauth_client_id: None,
        oauth_client_secret: None,
        oauth_token_url: None,
        oauth_scopes: None,
        region: "us-east-1".to_string(),
        s3_endpoint: None,
        request_timeout_secs: None,
        max_retries: None,
        cache: None,
        version: Some(TableVersionSpec::SnapshotId(1234567890)),
        max_concurrent_queries: None,
    };

    // Validate config
    cfg.validate()?;

    Ok(())
}

/// Test time travel with timestamp
#[tokio::test]
#[ignore] // Requires real Iceberg catalog
async fn test_time_travel_timestamp() -> Result<()> {
    let cfg = IcebergRestConfig {
        catalog_uri: "http://localhost:8181/v1".to_string(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: Some("default".to_string()),
        token: Some(SecretString::new("test-token".to_string().into())),
        oauth_client_id: None,
        oauth_client_secret: None,
        oauth_token_url: None,
        oauth_scopes: None,
        region: "us-east-1".to_string(),
        s3_endpoint: None,
        request_timeout_secs: None,
        max_retries: None,
        cache: None,
        version: Some(TableVersionSpec::Timestamp(1700000000000)),
        max_concurrent_queries: None,
    };

    // Validate config
    cfg.validate()?;

    Ok(())
}

/// Test invalid snapshot ID (negative)
#[tokio::test]
async fn test_invalid_snapshot_id() {
    let version = TableVersionSpec::SnapshotId(-1);
    assert!(version.validate().is_err());
}

/// Test invalid timestamp (future)
#[tokio::test]
async fn test_invalid_future_timestamp() {
    // Far future timestamp
    let version = TableVersionSpec::Timestamp(9999999999999);
    assert!(version.validate().is_err());
}

/// Test empty tag name
#[tokio::test]
async fn test_empty_tag() {
    let version = TableVersionSpec::Tag("".to_string());
    assert!(version.validate().is_err());
}

/// Test empty branch name
#[tokio::test]
async fn test_empty_branch() {
    let version = TableVersionSpec::Branch("".to_string());
    assert!(version.validate().is_err());
}
