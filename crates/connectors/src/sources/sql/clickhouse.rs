use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::clickhouse::ClickHouseTableFactory;
use datafusion_table_providers::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use super::common::{FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams};
use super::wrappers::register_tables;
use strake_common::config::TableConfig;
use strake_common::retry::retry_async;

pub struct ClickHouseMetadataFetcher;

#[async_trait]
impl SqlMetadataFetcher for ClickHouseMetadataFetcher {
    async fn fetch_metadata(&self, _schema: &str, _table: &str) -> Result<FetchedMetadata> {
        // ClickHouse doesn't have a standard comments system like Postgres
        Ok(FetchedMetadata::default())
    }
}

#[async_trait]
impl SqlProviderFactory for ClickHouseTableFactory {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: FetchedMetadata,
        cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .table_provider(table_ref, None)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Wrap with metadata and circuit breaker
        Ok(super::wrappers::wrap_provider(inner, cb, metadata))
    }
}

pub async fn register_clickhouse(params: SqlSourceParams<'_>) -> Result<()> {
    let context = params.context;
    let catalog_name = params.catalog_name;
    let name = params.name;
    let connection_string = params.connection_string;
    let cb = params.cb.clone();
    let explicit_tables = params.explicit_tables;
    let retry_settings = params.retry;
    let max_concurrent_queries = params.max_concurrent_queries;

    retry_async(
        &format!("register_clickhouse({})", name),
        retry_settings,
        move || {
            let cb = cb.clone();
            async move {
                try_register_clickhouse(
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
async fn try_register_clickhouse(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
    max_concurrent_queries: usize,
) -> Result<()> {
    let pool = create_clickhouse_pool(connection_string).await?;
    let factory = ClickHouseTableFactory::new(pool);

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables
            .iter()
            .map(|t| {
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
        introspect_clickhouse_tables(connection_string)
            .await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(ClickHouseMetadataFetcher {}));

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

async fn create_clickhouse_pool(connection_string: &str) -> Result<Arc<ClickHouseConnectionPool>> {
    let url = Url::parse(connection_string).context("Invalid ClickHouse connection URL")?;

    let mut params = HashMap::new();

    // Build URL without path for the connection
    let base_url = format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str().unwrap_or("localhost"),
        url.port().unwrap_or(8123)
    );
    params.insert("url".to_string(), SecretString::from(base_url));

    // Extract database from path
    let db = url.path().trim_start_matches('/');
    if !db.is_empty() {
        params.insert("database".to_string(), SecretString::from(db.to_string()));
    }

    // Extract credentials from URL
    if !url.username().is_empty() {
        params.insert(
            "user".to_string(),
            SecretString::from(url.username().to_string()),
        );
    }
    if let Some(password) = url.password() {
        params.insert(
            "password".to_string(),
            SecretString::from(password.to_string()),
        );
    }

    let pool = ClickHouseConnectionPool::new(params)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create ClickHouse connection pool")?;

    Ok(Arc::new(pool))
}

async fn introspect_clickhouse_tables(connection_string: &str) -> Result<Vec<String>> {
    // We need to query system.tables to get the list of tables
    // For now, use the HTTP API for introspection (simpler than setting up full connection)
    let url = Url::parse(connection_string)?;
    let db = url.path().trim_start_matches('/');
    let db_filter = if db.is_empty() { "default" } else { db };

    let sql = format!(
        "SELECT name FROM system.tables WHERE database = '{}'",
        db_filter
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(connection_string)
        .body(sql)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    Ok(resp.lines().map(|s| s.to_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_introspect_clickhouse_tables() -> Result<()> {
        let server: MockServer = MockServer::start().await;

        Mock::given(method("POST"))
            .and(body_string(
                "SELECT name FROM system.tables WHERE database = 'default'",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string("table1\ntable2"))
            .mount(&server)
            .await;

        let tables = introspect_clickhouse_tables(&server.uri()).await?;
        assert_eq!(tables, vec!["table1", "table2"]);
        Ok(())
    }
}
