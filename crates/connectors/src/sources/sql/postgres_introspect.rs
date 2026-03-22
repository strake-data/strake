use anyhow::Result;
use async_trait::async_trait;
use globset::GlobMatcher;
use tokio_postgres::NoTls;

use crate::introspect::{IntrospectError, SchemaIntrospector, TableRef};
use strake_common::schema::{
    lift_check_expression, normalize_type_str, ColumnConstraint, IntrospectedColumn,
    IntrospectedTable,
};

pub struct PostgresIntrospector {
    pub connection_string: String,
}

#[async_trait]
impl SchemaIntrospector for PostgresIntrospector {
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| IntrospectError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {}", e);
            }
        });

        let rows = client
            .query(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
                &[],
            )
            .await
            .map_err(|e| IntrospectError::Query(e.to_string()))?;

        let mut tables = Vec::new();
        for row in rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            let table_ref = TableRef {
                schema: schema.clone(),
                table: table.clone(),
            };

            if let Some(matcher) = pattern {
                if matcher.is_match(format!("{}.{}", schema, table)) {
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
        full: bool,
    ) -> Result<IntrospectedTable, IntrospectError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| IntrospectError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {}", e);
            }
        });

        if !full {
            let rows = client
                .query(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                    &[&table.schema, &table.table],
                )
                .await
                .map_err(|e| IntrospectError::Query(e.to_string()))?;

            let mut columns = Vec::new();
            for row in rows {
                let name: String = row.get(0);
                let type_str: String = row.get(1);
                let nullable_str: String = row.get(2);
                columns.push(IntrospectedColumn {
                    name,
                    type_str: normalize_type_str(&type_str),
                    nullable: nullable_str == "YES",
                    is_primary_key: false,
                    is_foreign_key: false,
                    constraints: vec![],
                    db_comment: None,
                    ai_description: None,
                });
            }

            return Ok(IntrospectedTable {
                source: "postgres".to_string(), // This will be updated by caller
                schema: table.schema.clone(),
                name: table.table.clone(),
                columns,
                db_comment: None,
                ai_description: None,
            });
        }

        // --- Full Introspection ---

        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            vec![&table.schema, &table.table];

        // 1. Column details + comments
        let col_query = client.query(
            "
            SELECT 
                a.attname as name, 
                pg_catalog.format_type(a.atttypid, a.atttypmod) as type_str,
                NOT a.attnotnull as nullable,
                col_description(a.attrelid, a.attnum) as comment
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $2 AND n.nspname = $1 AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            ",
            &params,
        );

        // 2. PK/FK constraints
        let constraint_query = client.query(
            "
            SELECT kcu.column_name, tc.constraint_type, ccu.table_name AS foreign_table
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            LEFT JOIN information_schema.referential_constraints rc
              ON tc.constraint_name = rc.constraint_name
            LEFT JOIN information_schema.constraint_column_usage ccu
              ON rc.unique_constraint_name = ccu.constraint_name
            WHERE tc.table_schema = $1 AND tc.table_name = $2
            ",
            &params,
        );

        // 3. CHECK constraints
        let check_query = client.query(
            "
            SELECT pg_get_constraintdef(oid) as def
            FROM pg_constraint
            WHERE conrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2)
            AND contype = 'c'
            ",
            &params,
        );

        // 4. Table comment
        let table_comment_query = client.query(
            "
            SELECT obj_description(c.oid, 'pg_class')
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            ",
            &params,
        );

        let (col_rows, constraint_rows, check_rows, table_comment_rows) = tokio::try_join!(
            col_query,
            constraint_query,
            check_query,
            table_comment_query
        )
        .map_err(|e| IntrospectError::Query(e.to_string()))?;

        let mut columns = Vec::new();
        for row in col_rows {
            let name: String = row.get(0);
            let type_str: String = row.get(1);
            let nullable: bool = row.get(2);
            let db_comment: Option<String> = row.get(3);

            let mut is_pk = false;
            let mut is_fk = false;
            let mut constraints = Vec::new();

            for c_row in &constraint_rows {
                let col_name: String = c_row.get(0);
                if col_name == name {
                    let c_type: String = c_row.get(1);
                    if c_type == "PRIMARY KEY" {
                        is_pk = true;
                        constraints.push(ColumnConstraint::PrimaryKey);
                    } else if c_type == "FOREIGN KEY" {
                        is_fk = true;
                        let foreign_table: String = c_row.get(2);
                        constraints.push(ColumnConstraint::ForeignKey {
                            references: foreign_table,
                        });
                    }
                }
            }

            // CHECK constraints (best effort)
            for ch_row in &check_rows {
                let def: String = ch_row.get(0);
                if def.contains(&format!("\"{}\"", name)) || def.contains(&name) {
                    if let Some(rule) = lift_check_expression(&name, &def) {
                        constraints.push(ColumnConstraint::ContractRule(rule));
                    } else {
                        constraints.push(ColumnConstraint::Check { expression: def });
                    }
                }
            }

            columns.push(IntrospectedColumn {
                name,
                type_str: normalize_type_str(&type_str),
                nullable,
                is_primary_key: is_pk,
                is_foreign_key: is_fk,
                constraints,
                db_comment,
                ai_description: None,
            });
        }

        let db_comment = table_comment_rows.first().and_then(|r| r.get(0));

        Ok(IntrospectedTable {
            source: "postgres".to_string(),
            schema: table.schema.clone(),
            name: table.table.clone(),
            columns,
            db_comment,
            ai_description: None,
        })
    }
}
