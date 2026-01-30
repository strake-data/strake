use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::*;
use datafusion_table_providers::sqlite::SqliteTableFactory;
use std::sync::Arc;
use std::time::Duration;

use super::common::{
    next_retry_delay, FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams,
};
use super::wrappers::register_tables;
use strake_common::config::TableConfig;

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

pub async fn register_sqlite(params: SqlSourceParams<'_>) -> Result<()> {
    let mut attempt = 0;
    let retry = params.retry;
    loop {
        match try_register_sqlite(
            params.context,
            params.catalog_name,
            params.name,
            params.connection_string,
            params.cb.clone(),
            params.explicit_tables,
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                attempt += 1;
                if attempt >= retry.max_attempts {
                    tracing::error!(
                        "Failed to register SQLite source '{}' after {} attempts: {}",
                        params.name,
                        retry.max_attempts,
                        e
                    );
                    return Err(e);
                }
                let delay = next_retry_delay(attempt, retry.base_delay_ms, retry.max_delay_ms);
                tracing::warn!(
                    "Connection failed for source '{}'. Retrying in {:?} (Attempt {}/{}): {}",
                    params.name,
                    delay,
                    attempt,
                    retry.max_attempts,
                    e
                );
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
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
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
    let inner_factory = SqliteTableFactory::new(Arc::new(pool));

    // Create federation provider
    let executor = super::sqlite_federation::SqliteExecutor::new(connection_string.to_string());
    let federation_provider = executor.create_federation_provider();

    let factory = FederatedSqliteTableFactory {
        inner: inner_factory,
        federation_provider,
    };

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables
            .iter()
            .map(|t| {
                let target_schema = t.schema.clone().unwrap_or_else(|| name.to_string());
                (t.name.clone(), target_schema)
            })
            .collect()
    } else {
        introspect_sqlite_tables(connection_string)
            .await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(SqliteMetadataFetcher {
        db_path: connection_string.to_string(),
    }));

    register_tables(
        context,
        catalog_name,
        name,
        fetcher,
        &factory,
        cb,
        tables_to_register,
    )
    .await?;
    Ok(())
}

struct FederatedSqliteTableFactory {
    inner: SqliteTableFactory,
    federation_provider: Arc<datafusion_federation::sql::SQLFederationProvider>,
}

#[async_trait]
impl SqlProviderFactory for FederatedSqliteTableFactory {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: FetchedMetadata,
        cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner_provider = self
            .inner
            .table_provider(table_ref.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Wrap the inner provider with metadata and circuit breaker
        let wrapped_inner = super::wrappers::wrap_provider(inner_provider, cb, metadata);

        // Use SQLTableSource for federation logic
        let sql_source = datafusion_federation::sql::SQLTableSource::new_with_schema(
            self.federation_provider.clone(),
            table_ref.into(),
            wrapped_inner.schema(),
        );

        // Wrap with federation adaptor
        // First arg: Source (logical), Second arg: Provider (physical fallback)
        let adaptor = datafusion_federation::FederatedTableProviderAdaptor::new_with_provider(
            Arc::new(sql_source),
            wrapped_inner,
        );

        Ok(Arc::new(adaptor))
    }
}

pub async fn introspect_sqlite_tables(db_path: &str) -> Result<Vec<String>> {
    let conn = rusqlite::Connection::open(db_path)
        .context("Failed to open SQLite database for introspection")?;

    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .context("Failed to prepare SQLite introspection query")?;

    let rows = stmt
        .query_map([], |row| row.get(0))
        .context("Failed to execute SQLite introspection query")?
        .collect::<std::result::Result<Vec<String>, _>>()
        .context("Failed to collect SQLite table names")?;

    Ok(rows)
}
