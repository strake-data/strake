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

use super::common::{FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams};
use super::wrappers::register_tables;
use strake_common::config::TableConfig;
use strake_common::retry::retry_async;

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
    let context = params.context;
    let catalog_name = params.catalog_name;
    let name = params.name;
    let connection_string = params.connection_string;
    let cb = params.cb.clone();
    let explicit_tables = params.explicit_tables;
    let retry_settings = params.retry;
    let max_concurrent_queries = params.max_concurrent_queries;

    retry_async(
        &format!("register_sqlite({})", name),
        retry_settings,
        move || {
            let cb = cb.clone();
            async move {
                try_register_sqlite(
                    context,
                    catalog_name,
                    name,
                    connection_string,
                    cb,
                    explicit_tables,
                    max_concurrent_queries,
                )
                .await
            }
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn try_register_sqlite(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
    max_concurrent_queries: usize,
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
                // FIX: Transform schema - use source name if empty, "public", or "main"
                let target_schema =
                    if t.schema.is_empty() || t.schema == "public" || t.schema == "main" {
                        name.to_string()
                    } else {
                        t.schema.clone()
                    };
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
        max_concurrent_queries,
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
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open(&db_path)
            .context("Failed to open SQLite database for introspection")?;

        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            .context("Failed to prepare SQLite introspection query")?;

        let rows = stmt
            .query_map([], |row| row.get(0))
            .context("Failed to execute SQLite introspection query")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect SQLite table names")?;

        Ok(rows)
    })
    .await
    .context("Join error during introspection")?
}
