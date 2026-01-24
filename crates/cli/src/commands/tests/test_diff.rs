use crate::commands::diff::{diff_sources, diff_tables};
use crate::models::{ColumnConfig, SourceConfig, TableConfig};

#[test]
fn test_diff_sources_no_changes() {
    let source = SourceConfig {
        name: "test".to_string(),
        source_type: "postgres".to_string(),
        url: Some("postgres://localhost:5432/db".to_string()),
        tables: vec![],
        username: None,
        password: None,
    };

    let changes = diff_sources(&source, &source);
    assert!(changes.is_empty());
}

#[test]
fn test_diff_sources_url_change() {
    let local = SourceConfig {
        name: "test".to_string(),
        source_type: "postgres".to_string(),
        url: Some("postgres://new:5432/db".to_string()),
        tables: vec![],
        username: None,
        password: None,
    };

    let db = SourceConfig {
        name: "test".to_string(),
        source_type: "postgres".to_string(),
        url: Some("postgres://old:5432/db".to_string()),
        tables: vec![],
        username: None,
        password: None,
    };

    let changes = diff_sources(&local, &db);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type, "MODIFY");
    assert_eq!(changes[0].category, "source");
}

#[test]
fn test_diff_tables_add_column() {
    let local_col = ColumnConfig {
        name: "new_col".to_string(),
        data_type: "int".to_string(),
        primary_key: false,
        unique: false,
        is_not_null: false,
        length: None,
    };

    let local = TableConfig {
        name: "users".to_string(),
        schema: "public".to_string(),
        columns: vec![local_col],
        partition_column: None,
    };

    let db = TableConfig {
        name: "users".to_string(),
        schema: "public".to_string(),
        columns: vec![],
        partition_column: None,
    };

    let changes = diff_tables(&local, &db);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type, "ADD");
    assert_eq!(changes[0].category, "column");
    assert_eq!(changes[0].name, "new_col");
}
