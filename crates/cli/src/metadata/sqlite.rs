//! # SQLite Metadata Store
//!
//! SQLite implementation of the `MetadataStore` trait.
#![allow(clippy::field_reassign_with_default)]
//!
//! ## Overview
//!
//! Handles local persistence of sources configuration using SQLite. Supports optimistic
//! locking, audit history, and configuration extraction.
//!
//! ## Usage
//!
//! ```ignore
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

use super::MetadataStore;
use anyhow::{Context, Result};
use futures::future::BoxFuture;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// SQLite implementation of the `MetadataStore`.
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    /// Creates a new `SqliteStore` with the given file path.
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

    fn create_api_key<'a>(
        &'a self,
        name: &'a str,
        description: Option<&'a str>,
        user_id: &'a str,
        key_prefix: &'a str,
        key_hash: &'a str,
        permissions: &'a [String],
    ) -> BoxFuture<'a, Result<()>> {
        let conn = self.conn.clone();
        let name = name.to_string();
        let description = description.map(|s| s.to_string());
        let user_id = user_id.to_string();
        let key_prefix = key_prefix.to_string();
        let key_hash = key_hash.to_string();
        let permissions = permissions.to_vec();

        Box::pin(async move {
            if key_prefix.len() != crate::metadata::KEY_PREFIX_LEN {
                anyhow::bail!(
                    "Invalid prefix length: expected {}, got {}",
                    crate::metadata::KEY_PREFIX_LEN,
                    key_prefix.len()
                );
            }

            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let id = uuid::Uuid::new_v4().to_string();
                let perms_str = serde_json::to_string(&permissions)
                    .context("Failed to serialize permissions to JSON")?;

                conn.execute(
                    "INSERT INTO api_keys (id, name, description, user_id, key_prefix, key_hash, permissions)
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![id, name, description, user_id, key_prefix, key_hash, perms_str],
                )
                .context("Failed to insert API key into sqlite")?;
                Ok(())
            })
            .await?
        })
    }

    fn list_api_keys(&self) -> BoxFuture<'_, Result<Vec<super::models::ApiKeyInfo>>> {
        let conn = self.conn.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let mut stmt = conn.prepare(
                    "SELECT id, name, key_prefix, user_id, permissions, created_at, last_used_at, revoked_at, description
                     FROM api_keys ORDER BY created_at DESC"
                ).context("Failed to prepare query API keys in sqlite")?;

                let rows = stmt.query_map([], |row| {
                    let perms_str: String = row.get("permissions")?;
                    let permissions: Vec<String> = serde_json::from_str(&perms_str)
                        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e)))?;

                    Ok(super::models::ApiKeyInfo {
                        id: row.get("id")?,
                        name: row.get("name")?,
                        key_prefix: row.get("key_prefix")?,
                        user_id: row.get("user_id")?,
                        permissions,
                        created_at: row.get("created_at")?,
                        last_used_at: row.get("last_used_at")?,
                        revoked_at: row.get("revoked_at")?,
                        description: row.get("description")?,
                    })
                })?;

                let mut results = Vec::new();
                for r in rows {
                    results.push(r?);
                }
                Ok(results)
            })
            .await?
        })
    }

    fn revoke_api_key<'a>(&'a self, key_prefix: &'a str) -> BoxFuture<'a, Result<bool>> {
        let conn = self.conn.clone();
        let prefix = key_prefix.to_string();
        Box::pin(async move {
            if prefix.len() != crate::metadata::KEY_PREFIX_LEN {
                anyhow::bail!(
                    "Invalid prefix length: expected {}, got {}",
                    crate::metadata::KEY_PREFIX_LEN,
                    prefix.len()
                );
            }

            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("SQLite lock poisoned: {}", e))?;
                let rows_updated = conn.execute(
                    "UPDATE api_keys SET revoked_at = datetime('now') WHERE key_prefix = ? AND revoked_at IS NULL",
                    params![prefix],
                )
                .context("Failed to revoke API key in sqlite")?;
                Ok(rows_updated > 0)
            })
            .await?
        })
    }
}
