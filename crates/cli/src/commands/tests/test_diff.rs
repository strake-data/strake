#![allow(clippy::field_reassign_with_default)]
use crate::commands::diff_logic::{diff_sources, diff_tables};
use crate::models::{ColumnConfig, SourceConfig, TableConfig};

#[test]
fn test_diff_sources_no_changes() {
    let mut source = SourceConfig::default();
    source.name = "test".into();
    source.source_type = strake_common::models::SourceType::Other("postgres".to_string());
    source.url = Some("postgres://localhost:5432/db".to_string());
    source.tables = vec![];

    let changes = diff_sources(&source, &source);
    assert!(changes.is_empty());
}

#[test]
fn test_diff_sources_url_change() {
    let mut local = SourceConfig::default();
    local.name = "test".into();
    local.source_type = strake_common::models::SourceType::Other("postgres".to_string());
    local.url = Some("postgres://new:5432/db".to_string());
    local.tables = vec![];

    let mut db = SourceConfig::default();
    db.name = "test".into();
    db.source_type = strake_common::models::SourceType::Other("postgres".to_string());
    db.url = Some("postgres://old:5432/db".to_string());
    db.tables = vec![];

    let changes = diff_sources(&local, &db);
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].change_type,
        crate::commands::helpers::ChangeType::Modified
    );
    assert_eq!(changes[0].path, "sources[test].url");
}

#[test]
fn test_diff_tables_add_column() {
    let mut local = TableConfig::default();
    local.name = "users".to_string();
    local.schema = "public".to_string();
    let mut col = ColumnConfig::default();
    col.name = "new_col".to_string();
    col.data_type = "int".to_string();
    local.column_definitions = vec![col];

    let mut db = TableConfig::default();
    db.name = "users".to_string();
    db.schema = "public".to_string();
    db.column_definitions = vec![];

    let changes = diff_tables("test_source", &local, &db);
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].change_type,
        crate::commands::helpers::ChangeType::Added
    );
    assert_eq!(
        changes[0].path,
        "sources[test_source].tables[public.users].column_definitions[new_col]"
    );
}
