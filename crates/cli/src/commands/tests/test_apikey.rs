//! Tests for API key lifecycle (create, list, revoke).

use crate::commands::{apikey, db};
use crate::metadata::MetadataStore;
use crate::metadata::sqlite::SqliteStore;
use serial_test::serial;
use tempfile::tempdir;

#[tokio::test]
#[serial]
async fn test_database_init_and_apikey_lifecycle() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("metadata.db");

    // 1. Create SQLite store
    let store = SqliteStore::new(db_path).unwrap();

    // 2. Initialize database schema via db init command
    let db_init_res = db::init(&store).await;
    assert!(db_init_res.is_ok());
    assert_eq!(db_init_res.unwrap(), 0);

    // 3. Create a new API key
    let create_options = apikey::ApikeyCreateOptions {
        name: "Test Grafana Key".to_string(),
        description: Some("Used to query telemetry dashboard".to_string()),
        user_id: "grafana-user".to_string(),
        permissions: vec!["read".to_string(), "query".to_string()],
    };

    let create_res =
        apikey::create(&store, create_options, crate::output::OutputFormat::Human).await;
    assert!(create_res.is_ok());
    assert_eq!(create_res.unwrap(), 0);

    // 4. List keys and verify the key is successfully stored
    let keys = store.list_api_keys().await.unwrap();
    assert_eq!(keys.len(), 1);
    let key_info = &keys[0];
    assert_eq!(key_info.name, "Test Grafana Key");
    assert_eq!(key_info.user_id, "grafana-user");
    assert_eq!(
        key_info.description,
        Some("Used to query telemetry dashboard".to_string())
    );
    assert_eq!(
        key_info.permissions,
        vec!["read".to_string(), "query".to_string()]
    );
    assert_eq!(key_info.key_prefix.len(), 8);
    assert!(key_info.key_prefix.starts_with("strk_"));
    assert!(key_info.revoked_at.is_none());

    // 5. Revoke key using the prefix
    let revoke_res = apikey::revoke(
        &store,
        &key_info.key_prefix,
        crate::output::OutputFormat::Human,
    )
    .await;
    assert!(revoke_res.is_ok());
    assert_eq!(revoke_res.unwrap(), 0);

    // 6. Verify key is revoked in the store
    let keys_after = store.list_api_keys().await.unwrap();
    assert_eq!(keys_after.len(), 1);
    assert!(keys_after[0].revoked_at.is_some());
}
