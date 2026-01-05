use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::sqlite::SqliteTableFactory;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::*;

use crate::config::{TableConfig, RetrySettings};
use super::common::{SqlMetadataFetcher, FetchedMetadata, SqlProviderFactory, next_retry_delay};
use super::wrappers::register_tables;

pub struct SqliteMetadataFetcher {
    #[allow(dead_code)]
    pub db_path: String,
}

#[async_trait]
impl SqlMetadataFetcher for SqliteMetadataFetcher {
    async fn fetch_metadata(&self, _schema: &str, _table: &str) -> Result<FetchedMetadata> {
        // SQLite doesn't natively support column/table comments in a standard way
        Ok(FetchedMetadata::default())
    }
}

#[async_trait]
impl SqlProviderFactory for SqliteTableFactory {
    async fn create_table_provider(&self, table_ref: TableReference) -> Result<Arc<dyn TableProvider>> {
        self.table_provider(table_ref).await.map_err(|e| anyhow::anyhow!(e))
    }
}

pub async fn register_sqlite(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<crate::query::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
    retry: RetrySettings,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        match try_register_sqlite(context, catalog_name, name, connection_string, cb.clone(), explicit_tables).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                attempt += 1;
                if attempt >= retry.max_attempts {
                    tracing::error!("Failed to register SQLite source '{}' after {} attempts: {}", name, retry.max_attempts, e);
                    return Err(e);
                }
                let delay = next_retry_delay(attempt, retry.base_delay_ms, retry.max_delay_ms);
                tracing::warn!("Connection failed for source '{}'. Retrying in {:?} (Attempt {}/{}): {}", name, delay, attempt, retry.max_attempts, e);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn try_register_sqlite(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<crate::query::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let pool = SqliteConnectionPool::new(
        connection_string,
        Mode::File,
        JoinPushDown::Disallow,
        vec![],
        Duration::from_secs(30),
    )
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create SQLite connection pool")?;
    let factory = SqliteTableFactory::new(Arc::new(pool));

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables.iter().map(|t| {
             let target_schema = t.schema.clone().unwrap_or_else(|| name.to_string());
             (t.name.clone(), target_schema)
        }).collect()
    } else {
        introspect_sqlite_tables(connection_string).await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(SqliteMetadataFetcher {
         db_path: connection_string.to_string(),
    }));

    register_tables(context, catalog_name, name, fetcher, &factory, cb, tables_to_register).await?;
    Ok(())
}

pub async fn introspect_sqlite_tables(db_path: &str) -> Result<Vec<String>> {
     let conn = rusqlite::Connection::open(db_path)
        .context("Failed to open SQLite database for introspection")?;
    
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .context("Failed to prepare SQLite introspection query")?;
        
    let rows = stmt.query_map([], |row| row.get(0))
        .context("Failed to execute SQLite introspection query")?
        .collect::<std::result::Result<Vec<String>, _>>()
        .context("Failed to collect SQLite table names")?;

    Ok(rows)
}
