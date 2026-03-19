//! # SQLite Metadata Store
//!
//! SQLite implementation of the `MetadataStore` trait.
//!
//! ## Overview
//!
//! Handles local persistence of sources configuration using SQLite. Supports optimistic
//! locking, audit history, and configuration extraction.
//!
//! ## Usage
//!
//! ```rust
//! // let store = SqliteStore::new(path)?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Operations are handled via blocking `tokio::task::spawn_blocking` to prevent starving
//! the async executor. Uses connection pooling/mutexes securely.
//!
//! ## Safety
//!
//! Standard safe Rust.
//!
//! ## References
//!
//! - SQLite backend design doc.

use super::{
    MetadataStore,
    models::{ApplyLogEntry, ApplyResult},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::BoxFuture;
use rusqlite::{Connection, OptionalExtension, Row, ToSql, params};
use secrecy::ExposeSecret;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use strake_common::models::{ColumnConfig, SourceConfig, SourcesConfig, TableConfig};

pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn new(path: PathBuf) -> Result<Self> {
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("Failed to create metadata directory")?;
        }
        let conn = Connection::open(path).context("Failed to open SQLite database")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

const DEFAULT_DOMAIN: &str = "default";

impl MetadataStore for SqliteStore {
    fn init(&self) -> BoxFuture<'_, Result<()>> {
        let conn = self.conn.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;

                const MIGRATIONS: &[(&str, &str)] = &[
                    ("001_initial_schema", include_str!("../../migrations/sqlite/001_initial_schema.sql")),
                    ("002_group_rbac",     include_str!("../../migrations/002_group_rbac.sql")),
                ];

                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TEXT DEFAULT CURRENT_TIMESTAMP);"
                ).context("Failed to create schema_migrations table")?;

                for (name, sql) in MIGRATIONS {
                    let applied: bool = conn.query_row(
                        "SELECT 1 FROM schema_migrations WHERE name = ?", params![name], |_| Ok(true)
                    ).optional().context("Failed to query schema_migrations")?.is_some();
                    if !applied {
                        conn.execute_batch(sql).context(format!("Failed to execute migration {}", name))?;
                        conn.execute("INSERT INTO schema_migrations (name) VALUES (?)", params![name])?;
                    }
                }
                Ok(())
            })
            .await?
        })
    }

    fn get_domain_version<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<i32>> {
        let conn = self.conn.clone();
        let domain = domain.to_string();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let mut stmt = conn.prepare("SELECT version FROM domains WHERE name = ?")?;
                let mut rows = stmt.query(params![domain])?;

                if let Some(row) = rows.next()? {
                    let ver: i32 = row.get(0)?;
                    Ok(ver)
                } else {
                    conn.execute(
                        "INSERT OR IGNORE INTO domains (name, version) VALUES (?, 1)",
                        params![domain],
                    )?;
                    Ok(1)
                }
            })
            .await?
        })
    }

    fn increment_domain_version<'a>(
        &'a self,
        domain: &'a str,
        expected_version: i32,
    ) -> BoxFuture<'a, Result<i32>> {
        let conn = self.conn.clone();
        let domain = domain.to_string();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let rows = conn.execute(
                    "UPDATE domains SET version = version + 1 WHERE name = ? AND version = ?",
                    params![domain, expected_version],
                )?;

                if rows == 0 {
                    return Err(anyhow!(
                        "Optimistic locking failure: Domain '{}' version has changed (expected v{})",
                        domain,
                        expected_version
                    ));
                }
                Ok(expected_version + 1)
            }).await?
        })
    }

    fn apply_sources<'a>(
        &'a self,
        config: &'a SourcesConfig,
        force: bool,
    ) -> BoxFuture<'a, Result<ApplyResult>> {
        let conn = self.conn.clone();
        let config = config.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut conn = conn.lock().map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let tx = conn.transaction()?;

                let domain = config.domain.as_deref().unwrap_or(DEFAULT_DOMAIN);

                tx.execute("INSERT OR IGNORE INTO domains (name) VALUES (?)", params![domain])?;

                let mut active_source_ids: Vec<i64> = Vec::new();
                let mut sources_added = Vec::new();

                for source in &config.sources {

                    let exists: bool = tx.query_row(
                        "SELECT 1 FROM sources WHERE domain_name = ? AND name = ?",
                        params![domain, source.name],
                        |_| Ok(true)
                    ).optional().context("Failed to query sources")?.is_some();

                    let source_id: i64 = tx.query_row(
                        "INSERT INTO sources (name, type, url, username, password, domain_name)
                         VALUES (?, ?, ?, ?, ?, ?)
                         ON CONFLICT (domain_name, name) DO UPDATE SET type=excluded.type, url=excluded.url, username=excluded.username, password=excluded.password
                         RETURNING id",
                        params![
                            source.name,
                            source.source_type,
                            source.url,
                            source.username,
                            source.password.as_ref().map(|s| s.expose_secret()),
                            domain
                        ],
                        |row| row.get(0),
                    )?;

                    active_source_ids.push(source_id);
                    if !exists {
                        sources_added.push(source.name.clone());
                    }

                    let mut active_table_ids: Vec<i64> = Vec::new();

                    for table in &source.tables {


                        let table_id: i64 = tx.query_row(
                            "INSERT INTO tables (source_id, name, schema_name, partition_column)
                             VALUES (?, ?, ?, ?)
                             ON CONFLICT (source_id, schema_name, name)
                             DO UPDATE SET partition_column=excluded.partition_column
                             RETURNING id",
                            params![source_id, table.name, table.schema, table.partition_column],
                            |row| row.get(0)
                        )?;
                        active_table_ids.push(table_id);

                        let mut active_column_names: Vec<String> = Vec::new();

                        for (idx, col) in table.columns.iter().enumerate() {
                            tx.execute(
                                "INSERT INTO columns (table_id, name, data_type, length, is_primary_key, is_unique, is_not_null, position)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                 ON CONFLICT (table_id, name)
                                 DO UPDATE SET data_type=excluded.data_type, length=excluded.length,
                                               is_primary_key=excluded.is_primary_key, is_unique=excluded.is_unique,
                                               is_not_null=excluded.is_not_null, position=excluded.position",
                                params![table_id, col.name, col.data_type, col.length, col.primary_key, col.unique, col.not_null, idx as i32]
                            )?;
                            active_column_names.push(col.name.clone());
                        }

                        if !active_column_names.is_empty() {
                            // Check for deletions if not forcing
                            if !force {
                                let placeholders = vec!["?"; active_column_names.len()].join(",");
                                let sql_sel = format!("SELECT name FROM columns WHERE table_id = ? AND name NOT IN ({})", placeholders);
                                let mut params_vec: Vec<&dyn ToSql> = Vec::new();
                                params_vec.push(&table_id);
                                for name in &active_column_names {
                                    params_vec.push(name);
                                }
                                let mut stmt = tx.prepare(&sql_sel)?;
                                let rows = stmt.query_map(rusqlite::params_from_iter(params_vec.iter().copied()), |row: &Row| row.get::<_, String>(0))?;
                                let mut to_delete = Vec::new();
                                for name in rows {
                                    to_delete.push(name?);
                                }

                                if !to_delete.is_empty() {
                                    return Err(anyhow!(
                                        "Safety guard: This update would delete columns {:?} in table '{}'. Use --force to proceed.",
                                        to_delete, table.name
                                    ));
                                }
                            }

                             let placeholders = vec!["?"; active_column_names.len()].join(",");
                             let sql = format!("DELETE FROM columns WHERE table_id = ? AND name NOT IN ({})", placeholders);
                             let mut params_vec: Vec<&dyn ToSql> = Vec::new();
                             params_vec.push(&table_id);
                             for name in &active_column_names {
                                 params_vec.push(name);
                             }
                             tx.execute(&sql, rusqlite::params_from_iter(params_vec))?;
                        }
                    }

                    if !active_table_ids.is_empty() {
                        // Check for deletions if not forcing
                        if !force {
                            let placeholders = vec!["?"; active_table_ids.len()].join(",");
                            let sql_sel = format!("SELECT name FROM tables WHERE source_id = ? AND id NOT IN ({})", placeholders);
                            let mut params_vec: Vec<&dyn ToSql> = Vec::new();
                            params_vec.push(&source_id);
                            for id in &active_table_ids {
                                params_vec.push(id);
                            }
                            let mut stmt = tx.prepare(&sql_sel)?;
                            let rows = stmt.query_map(rusqlite::params_from_iter(params_vec.iter().copied()), |row: &Row| row.get::<_, String>(0))?;
                            let mut to_delete = Vec::new();
                            for name in rows {
                                to_delete.push(name?);
                            }

                            if !to_delete.is_empty() {
                                return Err(anyhow!(
                                    "Safety guard: This update would delete tables {:?} in source '{}'. Use --force to proceed.",
                                    to_delete, source.name
                                ));
                            }
                        }

                        let placeholders = vec!["?"; active_table_ids.len()].join(",");
                        let sql = format!("DELETE FROM tables WHERE source_id = ? AND id NOT IN ({})", placeholders);

                        let mut params_vec: Vec<&dyn ToSql> = Vec::new();
                        params_vec.push(&source_id);
                        for id in &active_table_ids {
                            params_vec.push(id);
                        }
                        tx.execute(&sql, rusqlite::params_from_iter(params_vec))?;
                    }
                }

                // Prune sources
                let mut sources_deleted = Vec::new();
                if !active_source_ids.is_empty() {
                     let placeholders = vec!["?"; active_source_ids.len()].join(",");

                     let sql_sel = format!("SELECT name FROM sources WHERE domain_name = ? AND id NOT IN ({})", placeholders);
                     let mut params_vec: Vec<&dyn ToSql> = Vec::new();
                     params_vec.push(&domain);
                     for id in &active_source_ids {
                        params_vec.push(id);
                     }

                     let mut to_delete = Vec::new();
                     {
                        let mut stmt = tx.prepare(&sql_sel)?;
                        let rows = stmt.query_map(rusqlite::params_from_iter(params_vec.iter().copied()), |row: &Row| row.get::<_, String>(0))?;
                        for name in rows {
                            to_delete.push(name?);
                        }
                     }

                     if !to_delete.is_empty() {
                        if !force {
                            return Err(anyhow!(
                                "Safety guard: This update would delete sources {:?} in domain '{}'. Use --force to proceed.",
                                to_delete, domain
                            ));
                        }
                        sources_deleted = to_delete;
                        let sql_del = format!("DELETE FROM sources WHERE domain_name = ? AND id NOT IN ({})", placeholders);
                        tx.execute(&sql_del, rusqlite::params_from_iter(params_vec))?;
                     }

                } else if config.sources.is_empty() {
                    let mut to_delete = Vec::new();
                    {
                        let mut stmt = tx.prepare("SELECT name FROM sources WHERE domain_name = ?")?;
                        let rows = stmt.query_map(params![domain], |row: &Row| row.get::<_, String>(0))?;
                        for name in rows {
                            to_delete.push(name?);
                        }
                    }

                    if !to_delete.is_empty() {
                        if !force {
                            return Err(anyhow!(
                                "Safety guard: This update would delete ALL sources {:?} in domain '{}'. Use --force to proceed.",
                                to_delete, domain
                            ));
                        }
                        sources_deleted = to_delete;
                        tx.execute("DELETE FROM sources WHERE domain_name = ?", params![domain])?;
                    }
                }
                tx.commit()?;
                Ok(ApplyResult {
                    sources_added,
                    sources_deleted
                })
            }).await?
        })
    }

    fn log_apply_event<'a>(&'a self, entry: ApplyLogEntry) -> BoxFuture<'a, Result<()>> {
        let conn = self.conn.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                conn.execute(
                    "INSERT INTO apply_history (domain_name, version, user_id, sources_added, sources_deleted, tables_modified, config_hash, config_yaml)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        entry.domain,
                        entry.version,
                        entry.user_id,
                        entry.sources_added.to_string(),
                        entry.sources_deleted.to_string(),
                        entry.tables_modified.to_string(),
                        entry.config_hash,
                        entry.config_yaml
                    ],
                )?;
                Ok(())
            }).await?
        })
    }

    fn get_history<'a>(
        &'a self,
        domain: &'a str,
        limit: i64,
    ) -> BoxFuture<'a, Result<Vec<ApplyLogEntry>>> {
        let conn = self.conn.clone();
        let domain = domain.to_string();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let mut stmt = conn.prepare(
                    "SELECT domain_name, version, user_id, sources_added, sources_deleted, tables_modified, config_hash, config_yaml, timestamp
                     FROM apply_history WHERE domain_name = ? ORDER BY version DESC LIMIT ?"
                )?;

                let rows = stmt.query_map(params![domain, limit], |row: &Row| {
                     let added_str: String = row.get("sources_added")?;
                     let deleted_str: String = row.get("sources_deleted")?;
                     let modified_str: String = row.get("tables_modified")?;

                     Ok(ApplyLogEntry {
                        domain: row.get("domain_name")?,
                        version: row.get("version")?,
                        user_id: row.get("user_id")?,
                        sources_added: serde_json::from_str(&added_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?,
                        sources_deleted: serde_json::from_str(&deleted_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e)))?,
                        tables_modified: serde_json::from_str(&modified_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e)))?,
                        config_hash: row.get("config_hash")?,
                        config_yaml: row.get("config_yaml")?,
                        timestamp: row.get::<_, Option<String>>("timestamp")?
                            .and_then(|t| t.parse::<DateTime<Utc>>().ok()),
                     })
                })?;

                let mut entries = Vec::new();
                for row in rows {
                    entries.push(row?);
                }
                Ok(entries)
            }).await?
        })
    }

    fn get_sources<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<SourcesConfig>> {
        let conn = self.conn.clone();
        let domain = domain.to_string();

        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let mut sources = Vec::new();

                let mut s_stmt = conn.prepare("SELECT id, name, type, url FROM sources WHERE domain_name = ?")?;
                let s_rows = s_stmt.query_map(params![domain], |row: &Row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?))
                })?;

                for s_res in s_rows {
                    let (source_id, name, source_type, url) = s_res?;

                    let mut tables = Vec::new();
                    let mut t_stmt = conn.prepare("SELECT id, name, schema_name, partition_column FROM tables WHERE source_id = ?")?;
                    let t_rows = t_stmt.query_map(params![source_id], |row: &Row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?))
                    })?;

                    for t_res in t_rows {
                         let (table_id, table_name, schema, partition_column) = t_res?;

                         let mut columns = Vec::new();
                         let mut c_stmt = conn.prepare("SELECT name, data_type, length, is_primary_key, is_unique, is_not_null FROM columns WHERE table_id = ? ORDER BY position")?;
                         let c_rows = c_stmt.query_map(params![table_id], |row: &Row| {
                             Ok(ColumnConfig {
                                name: row.get("name")?,
                                data_type: row.get("data_type")?,
                                length: row.get("length")?,
                                primary_key: row.get("is_primary_key")?,
                                unique: row.get("is_unique")?,
                                not_null: row.get("is_not_null")?,
                             })
                         })?;

                         for col in c_rows {
                             columns.push(col?);
                         }

                         tables.push(TableConfig {
                            name: table_name,
                            schema,
                            partition_column,
                            columns
                         });
                    }

                    sources.push(SourceConfig {
                         name,
                         source_type,
                         url,
                         username: None,
                         password: None,
                         default_limit: None,
                         cache: None,
                         max_concurrent_queries: None,
                         tables,
                         config: serde_json::Value::Null,
                    });
                }

                Ok(SourcesConfig {
                    domain: Some(domain),
                    sources
                })
            }).await?
        })
    }

    fn get_history_config<'a>(
        &'a self,
        domain: &'a str,
        version: i32,
    ) -> BoxFuture<'a, Result<String>> {
        let conn = self.conn.clone();
        let domain = domain.to_string();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let mut stmt = conn.prepare(
                    "SELECT config_yaml FROM apply_history WHERE domain_name = ? AND version = ?",
                )?;
                let mut rows = stmt.query(params![domain, version])?;
                if let Some(row) = rows.next()? {
                    Ok(row.get(0)?)
                } else {
                    Err(anyhow::anyhow!(
                        "Version {} not found for domain '{}'",
                        version,
                        domain
                    ))
                }
            })
            .await?
        })
    }

    fn list_domains(&self) -> BoxFuture<'_, Result<Vec<super::models::DomainStatus>>> {
        let conn = self.conn.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let mut stmt =
                    conn.prepare("SELECT name, version, created_at FROM domains ORDER BY name")?;
                let rows = stmt.query_map([], |row: &Row| {
                    let created_at_str: Option<String> = row.get(2).ok();
                    let created_at = match created_at_str {
                        Some(s) => {
                            if let Ok(nd) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                                Some(nd.and_utc())
                            } else if let Ok(nd) =
                                NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S.%f")
                            {
                                Some(nd.and_utc())
                            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                                Some(dt.with_timezone(&Utc))
                            } else {
                                return Err(rusqlite::Error::FromSqlConversionFailure(
                                    2,
                                    rusqlite::types::Type::Text,
                                    Box::<dyn std::error::Error + Send + Sync>::from(format!(
                                        "Unrecognized timestamp format: {}",
                                        s
                                    )),
                                ));
                            }
                        }
                        None => None,
                    };

                    Ok(super::models::DomainStatus {
                        name: row.get(0)?,
                        version: row.get(1)?,
                        created_at,
                    })
                })?;

                let mut results = Vec::new();
                for row in rows {
                    results.push(row?);
                }
                Ok(results)
            })
            .await?
        })
    }
}
