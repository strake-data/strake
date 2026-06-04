//! Tests for diff command.

use crate::commands::diff::diff_internal;
use crate::commands::sync::{SyncOptions, sync};
use crate::config::CliConfig;
use crate::output::OutputFormat;
use crate::secrets::ResolverContext;
use anyhow::Result;
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_diff_and_sync_sqlite_integration() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    // Create a sqlite database and schema
    let conn = rusqlite::Connection::open(db_path_str)?;
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
        [],
    )?;

    // Create a local sources.yaml config pointing to this database
    let yaml_content = format!(
        r#"
domain: test_domain
sources:
  - name: my_sqlite
    type: sqlite
    url: "sqlite://{}"
    tables:
      - name: users
        schema: main
        columns:
          - name: id
            type: integer
            primary_key: true
            not_null: true
          - name: name
            type: text
            not_null: true
"#,
        db_path_str
    );

    let config_file = dir.path().join("sources.yaml");
    fs::write(&config_file, yaml_content)?;

    let config = CliConfig::default();
    let ctx = ResolverContext {
        system_env: std::collections::HashMap::new(),
        dotenv: std::collections::HashMap::new(),
        offline: false,
    };

    // Run diff: the database has 'age' but local config does not (it should show 'age' added on remote)
    let diff_res = diff_internal(config_file.to_str().unwrap(), &config, &ctx).await?;
    assert_eq!(diff_res.changes.len(), 1);
    assert_eq!(
        diff_res.changes[0].change_type,
        crate::commands::helpers::ChangeType::Added
    );
    assert!(diff_res.changes[0].path.contains("age"));

    // Run sync to update sources.yaml in place
    let sync_opts = SyncOptions {
        file: config_file.to_str().unwrap().to_string(),
        format: OutputFormat::Human,
    };
    let sync_status = sync(sync_opts, &config, &ctx).await?;
    assert_eq!(sync_status, 0);

    // Read the updated sources.yaml
    let updated_yaml = fs::read_to_string(&config_file)?;
    assert!(updated_yaml.contains("age"));
    assert!(updated_yaml.contains("INTEGER")); // type should be updated/merged from introspected column

    // Run diff again: should have zero changes now!
    let diff_res_after = diff_internal(config_file.to_str().unwrap(), &config, &ctx).await?;
    assert!(diff_res_after.changes.is_empty());

    Ok(())
}
