//! # SQLite Introspector
//!
//! Provides metadata discovery for SQLite databases using `PRAGMA` queries.

use anyhow::Result;
use async_trait::async_trait;
use globset::GlobMatcher;
use rusqlite::Connection;

use crate::introspect::{IntrospectError, SchemaIntrospector, TableRef};
use strake_common::schema::{IntrospectedColumn, IntrospectedTable, normalize_type_str};

use secrecy::{ExposeSecret, SecretString};

pub struct SqliteIntrospector {
    pub db_path: SecretString,
}

#[async_trait]
impl SchemaIntrospector for SqliteIntrospector {
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError> {
        let db_path = self.db_path.expose_secret().to_string();
        let table_names = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| IntrospectError::Connection(e.to_string()))?;

            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let mut names = Vec::new();
            for row in rows {
                names.push(row.map_err(|e| IntrospectError::Query(e.to_string()))?);
            }
            Ok::<Vec<String>, IntrospectError>(names)
        })
        .await
        .map_err(|e| IntrospectError::Query(e.to_string()))??;

        let mut tables = Vec::new();
        for table_name in table_names {
            let table_ref = TableRef {
                schema: "main".to_string(),
                table: table_name,
            };

            if let Some(matcher) = pattern {
                if matcher.is_match(format!("main.{}", table_ref.table)) {
                    tables.push(table_ref);
                }
            } else {
                tables.push(table_ref);
            }
        }
        Ok(tables)
    }

    async fn introspect_table(
        &self,
        table: &TableRef,
        _full: bool,
    ) -> Result<IntrospectedTable, IntrospectError> {
        let db_path = self.db_path.expose_secret().to_string();
        let table_name = table.table.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| IntrospectError::Connection(e.to_string()))?;

            let mut stmt = conn
                .prepare(&format!(
                    "PRAGMA table_info(\"{}\")",
                    table_name.replace('"', "\"\"")
                ))
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let rows = stmt
                .query_map([], |row| {
                    Ok(IntrospectedColumn {
                        name: row.get(1)?,
                        type_str: normalize_type_str(&row.get::<_, String>(2)?),
                        nullable: row.get::<_, i32>(3)? == 0,
                        is_primary_key: row.get::<_, i32>(5)? > 0,
                        is_foreign_key: false, // PRAGMA foreign_key_list needed for this
                        constraints: vec![],
                        db_comment: None,
                        ai_description: None,
                    })
                })
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let mut columns = Vec::new();
            for row in rows {
                columns.push(row.map_err(|e| IntrospectError::Query(e.to_string()))?);
            }

            Ok(IntrospectedTable {
                source: "sqlite".to_string(),
                schema: "main".to_string(),
                name: table_name,
                columns,
                db_comment: None,
                ai_description: None,
            })
        })
        .await
        .map_err(|e| IntrospectError::Query(e.to_string()))?
    }
}
