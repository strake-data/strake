use anyhow::Result;
use async_trait::async_trait;
use globset::GlobMatcher;

use crate::introspect::{IntrospectError, SchemaIntrospector, TableRef};
use strake_common::schema::{IntrospectedColumn, IntrospectedTable, normalize_type_str};

pub struct DuckDBIntrospector {
    pub db_path: String,
}

#[async_trait]
impl SchemaIntrospector for DuckDBIntrospector {
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError> {
        let db_path = self.db_path.clone();
        let rows = tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&db_path)
                .map_err(|e| IntrospectError::Connection(e.to_string()))?;

            let mut stmt = conn
                .prepare("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let rows = stmt
                .query_map([], |row| {
                    let schema: String = row.get(0)?;
                    let table: String = row.get(1)?;
                    Ok(TableRef { schema, table })
                })
                .map_err(|e| IntrospectError::Query(e.to_string()))?
                .collect::<std::result::Result<Vec<TableRef>, _>>()
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            Ok(rows)
        })
        .await
        .map_err(|e| IntrospectError::Query(e.to_string()))??;

        let mut filtered = Vec::new();
        for table in rows {
            if let Some(matcher) = pattern {
                if matcher.is_match(format!("{}.{}", table.schema, table.table)) {
                    filtered.push(table);
                }
            } else {
                filtered.push(table);
            }
        }

        Ok(filtered)
    }

    async fn introspect_table(
        &self,
        table: &TableRef,
        _full: bool,
    ) -> Result<IntrospectedTable, IntrospectError> {
        let db_path = self.db_path.clone();
        let table_ref = table.clone();

        let columns = tokio::task::spawn_blocking(move || {
            let conn = duckdb::Connection::open(&db_path)
                .map_err(|e| IntrospectError::Connection(e.to_string()))?;

            let mut stmt = conn
                .prepare("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let rows = stmt
                .query_map([&table_ref.schema, &table_ref.table], |row| {
                    let name: String = row.get(0)?;
                    let type_str: String = row.get(1)?;
                    let nullable_str: String = row.get(2)?;
                    Ok(IntrospectedColumn {
                        name,
                        type_str: normalize_type_str(&type_str),
                        nullable: nullable_str == "YES",
                        is_primary_key: false, // DuckDB full introspection TBD
                        is_foreign_key: false,
                        constraints: vec![],
                        db_comment: None,
                        ai_description: None,
                    })
                })
                .map_err(|e| IntrospectError::Query(e.to_string()))?
                .collect::<std::result::Result<Vec<IntrospectedColumn>, _>>()
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            Ok(rows)
        })
        .await
        .map_err(|e| IntrospectError::Query(e.to_string()))??;

        Ok(IntrospectedTable {
            source: "duckdb".to_string(),
            schema: table.schema.clone(),
            name: table.table.clone(),
            columns,
            db_comment: None,
            ai_description: None,
        })
    }
}
