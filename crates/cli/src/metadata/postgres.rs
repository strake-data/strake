//! # Postgres Metadata Store
//!
//! Postgres backend implementation of the `MetadataStore` trait.
#![allow(clippy::field_reassign_with_default)]
//!
//! ## Overview
//!
//! Interacts with an upstream Postgres database to persist the domain configurations.
//! Ideal for multi-tenant and distributed Strake deployments.
//!
//! ## Usage
//!
//! ```ignore
//! // let store = PostgresStore::new(db_url).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Uses `tokio-postgres` for fully asynchronous, pipelined database access. Handles
//! background connections via detached Tokio tasks. Leverages `Arc<Client>` to enable
//! lock-free, zero-contention concurrent access across multiple async tasks.
//!
//! ## Safety
//!
//! Standard safe Rust.
//!
//! ## References
//!
//! - Postgres backend architecture documentation.

use super::MetadataStore;
use anyhow::{Context, Result};
use futures::future::BoxFuture;
use tokio_postgres::{Client, NoTls};

use std::sync::Arc;

/// Postgres implementation of the `MetadataStore`.
pub struct PostgresStore {
    client: Arc<Client>,
}

impl PostgresStore {
    /// Creates a new `PostgresStore` with the given database URL.
    pub async fn new(db_url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(db_url, NoTls)
            .await
            .context("Failed to connect to database")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "Postgres background connection task failed");
            }
        });

        Ok(Self {
            client: Arc::new(client),
        })
    }
}

impl MetadataStore for PostgresStore {
    fn init(&self) -> BoxFuture<'_, Result<()>> {
        let client = self.client.clone();
        Box::pin(async move {
            const MIGRATIONS: &[(&str, &str)] = &[
                (
                    "001_initial_schema",
                    include_str!("../../migrations/001_initial_schema.sql"),
                ),
                (
                    "002_group_rbac",
                    include_str!("../../migrations/002_group_rbac.sql"),
                ),
            ];

            client.batch_execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            ).await.context("Failed to create schema_migrations table")?;

            for (name, sql) in MIGRATIONS {
                let row_count = client
                    .query("SELECT 1 FROM schema_migrations WHERE name = $1", &[&name])
                    .await?
                    .len();

                if row_count == 0 {
                    client
                        .batch_execute(sql)
                        .await
                        .context(format!("Failed to execute migration {}", name))?;
                    client
                        .execute("INSERT INTO schema_migrations (name) VALUES ($1)", &[&name])
                        .await?;
                }
            }
            Ok(())
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
        let client = self.client.clone();
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

            client.execute(
                "INSERT INTO api_keys (name, description, user_id, key_prefix, key_hash, permissions)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &name as &(dyn tokio_postgres::types::ToSql + Sync),
                    &description as &(dyn tokio_postgres::types::ToSql + Sync),
                    &user_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    &key_prefix as &(dyn tokio_postgres::types::ToSql + Sync),
                    &key_hash as &(dyn tokio_postgres::types::ToSql + Sync),
                    &permissions as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .context("Failed to insert API key into postgres")?;
            Ok(())
        })
    }

    fn list_api_keys(&self) -> BoxFuture<'_, Result<Vec<super::models::ApiKeyInfo>>> {
        let client = self.client.clone();
        Box::pin(async move {
            let rows = client.query(
                "SELECT id, name, key_prefix, user_id, permissions, created_at, last_used_at, revoked_at, description
                 FROM api_keys ORDER BY created_at DESC",
                &[],
            )
            .await
            .context("Failed to query API keys from postgres")?;

            let mut results = Vec::new();
            for row in rows {
                let id: uuid::Uuid = row.get("id");
                let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
                let last_used_at: Option<chrono::DateTime<chrono::Utc>> = row.get("last_used_at");
                let revoked_at: Option<chrono::DateTime<chrono::Utc>> = row.get("revoked_at");

                results.push(super::models::ApiKeyInfo {
                    id: id.to_string(),
                    name: row.get("name"),
                    key_prefix: row.get("key_prefix"),
                    user_id: row.get("user_id"),
                    permissions: row.get("permissions"),
                    created_at: created_at.to_rfc3339(),
                    last_used_at: last_used_at.map(|t| t.to_rfc3339()),
                    revoked_at: revoked_at.map(|t| t.to_rfc3339()),
                    description: row.get("description"),
                });
            }
            Ok(results)
        })
    }

    fn revoke_api_key<'a>(&'a self, key_prefix: &'a str) -> BoxFuture<'a, Result<bool>> {
        let client = self.client.clone();
        let prefix = key_prefix.to_string();
        Box::pin(async move {
            if prefix.len() != crate::metadata::KEY_PREFIX_LEN {
                anyhow::bail!(
                    "Invalid prefix length: expected {}, got {}",
                    crate::metadata::KEY_PREFIX_LEN,
                    prefix.len()
                );
            }

            let rows_updated = client.execute(
                "UPDATE api_keys SET revoked_at = NOW() WHERE key_prefix = $1 AND revoked_at IS NULL",
                &[&prefix],
            )
            .await
            .context("Failed to revoke API key in postgres")?;
            Ok(rows_updated > 0)
        })
    }
}
