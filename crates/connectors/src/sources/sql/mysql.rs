use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use mysql_async::params;
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;

use super::common::{FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams};
use super::wrappers::register_tables;
use strake_common::config::TableConfig;
use strake_common::retry::retry_async;

pub struct MySqlMetadataFetcher {
    pub connection_string: String,
}

#[async_trait]
impl SqlMetadataFetcher for MySqlMetadataFetcher {
    async fn fetch_metadata(&self, _schema: &str, table: &str) -> Result<FetchedMetadata> {
        fetch_mysql_comments(&self.connection_string, table).await
    }
}

pub struct MySQLTableFactoryWrapper {
    pub factory: MySQLTableFactory,
}

#[async_trait]
impl SqlProviderFactory for MySQLTableFactoryWrapper {
    async fn create_table_provider(
        &self,
        _table_ref: TableReference,
        metadata: FetchedMetadata,
        cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .factory
            .table_provider(_table_ref)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Wrap with metadata and circuit breaker
        Ok(super::wrappers::wrap_provider(inner, cb, metadata))
    }
}

pub async fn register_mysql(params: SqlSourceParams<'_>) -> Result<()> {
    let context = params.context;
    let catalog_name = params.catalog_name;
    let name = params.name;
    let connection_string = params.connection_string;
    let pool_size = params.pool_size;
    let cb = params.cb.clone();
    let explicit_tables = params.explicit_tables;
    let retry_settings = params.retry;
    let max_concurrent_queries = params.max_concurrent_queries;

    retry_async(
        &format!("register_mysql({})", name),
        retry_settings,
        move || {
            let cb = cb.clone();
            async move {
                try_register_mysql(
                    context,
                    catalog_name,
                    name,
                    connection_string,
                    pool_size,
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
async fn try_register_mysql(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    pool_size: usize,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
    max_concurrent_queries: usize,
) -> Result<()> {
    let mut pool_params = HashMap::new();
    pool_params.insert(
        "connection_string".to_string(),
        SecretString::from(connection_string.to_string()),
    );
    pool_params.insert(
        "max_pool_size".to_string(),
        SecretString::from(pool_size.to_string()),
    );
    let pool = MySQLConnectionPool::new(pool_params)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create MySQL connection pool")?;
    let factory = MySQLTableFactory::new(Arc::new(pool));
    let factory_wrapper = MySQLTableFactoryWrapper { factory };

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables
            .iter()
            .map(|t: &TableConfig| {
                // FIX: Transform schema - use source name if empty or "public"
                let target_schema = if t.schema.is_empty() || t.schema == "public" {
                    name.to_string()
                } else {
                    t.schema.clone()
                };
                (t.name.clone(), target_schema)
            })
            .collect()
    } else {
        introspect_mysql_tables(connection_string)
            .await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(MySqlMetadataFetcher {
        connection_string: connection_string.to_string(),
    }));

    register_tables(
        context,
        catalog_name,
        name,
        fetcher,
        &factory_wrapper,
        cb,
        max_concurrent_queries,
        tables_to_register,
    )
    .await?;
    Ok(())
}

pub async fn introspect_mysql_tables(connection_string: &str) -> Result<Vec<String>> {
    use mysql_async::prelude::Queryable;
    let pool = mysql_async::Pool::new(connection_string);
    let mut conn = pool
        .get_conn()
        .await
        .context("Failed to connect to MySQL for introspection")?;

    let rows: Vec<String> = conn
        .query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'")
        .await
        .context("Failed to query information_schema.tables in MySQL")?;

    Ok(rows)
}

pub async fn fetch_mysql_comments(connection_string: &str, table: &str) -> Result<FetchedMetadata> {
    use mysql_async::prelude::Queryable;
    let pool = mysql_async::Pool::new(connection_string);
    let mut conn = pool
        .get_conn()
        .await
        .context("Failed to connect to MySQL for metadata")?;

    // Table comment
    let table_desc: Option<String> = conn
        .exec_first(
            "SELECT table_comment FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :table",
            params! { "table" => table },
        )
        .await
        .context("Failed to query MySQL table metadata")?;

    // Column comments
    let col_rows: Vec<(String, String)> = conn
        .exec(
            "SELECT column_name, column_comment FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = :table",
            params! { "table" => table },
        )
        .await
        .context("Failed to query MySQL column metadata")?;

    let mut metadata = FetchedMetadata {
        table_description: table_desc.filter(|s| !s.is_empty()),
        columns: HashMap::new(),
    };

    for (name, comment) in col_rows {
        if !comment.is_empty() {
            metadata.columns.insert(name, comment);
        }
    }

    Ok(metadata)
}
