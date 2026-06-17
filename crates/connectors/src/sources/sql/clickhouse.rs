//! # ClickHouse Connector
//!
//! Provides integration for ClickHouse data sources, including table discovery
//! and provider creation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::clickhouse::ClickHouseTableFactory;
use datafusion_table_providers::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;
use strake_common::circuit_breaker::AdaptiveCircuitBreaker;
use url::Url;

use super::base_connector::GenericSqlConnector;
use super::common::{SchemaMappingRule, SqlProviderFactory, SqlSourceParams};
use crate::introspect::{IntrospectError, SchemaIntrospector, TableRef};
use globset::GlobMatcher;

#[async_trait]
impl SqlProviderFactory for ClickHouseTableFactory {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        cb: Arc<AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .table_provider(table_ref, None)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Wrap with circuit breaker.
        // ClickHouse is a remote source, so enable schema drift detection.
        Ok(super::wrappers::wrap_provider(inner, cb, true))
    }
}

use secrecy::ExposeSecret;

/// Introspects ClickHouse database schemas to discover tables and columns.
pub struct ClickHouseIntrospector {
    /// The ClickHouse connection string used to connect to the database.
    pub connection_string: SecretString,
}

#[async_trait]
impl SchemaIntrospector for ClickHouseIntrospector {
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError> {
        let tables = introspect_clickhouse_tables(self.connection_string.expose_secret())
            .await
            .map_err(|e| IntrospectError::Query(e.to_string()))?;

        let mut filtered = Vec::new();
        for table in tables {
            let table_ref = TableRef {
                schema: "default".into(), // Default DB
                table,
            };
            if let Some(matcher) = pattern {
                if matcher.is_match(&table_ref.table) {
                    filtered.push(table_ref);
                }
            } else {
                filtered.push(table_ref);
            }
        }
        Ok(filtered)
    }

    async fn introspect_table(
        &self,
        table: &TableRef,
        _full: bool,
    ) -> Result<strake_common::schema::IntrospectedTable, IntrospectError> {
        Ok(strake_common::schema::IntrospectedTable {
            source: "clickhouse".to_string(),
            schema: table.schema.clone(),
            name: table.table.clone(),
            columns: vec![],
            db_comment: None,
            ai_description: None,
        })
    }
}

/// Registers a ClickHouse source into the provided context.
pub async fn register_clickhouse(params: SqlSourceParams) -> Result<()> {
    let connection_string = params.connection_string.clone();

    let pool = create_clickhouse_pool(connection_string.expose_secret()).await?;
    let factory = ClickHouseTableFactory::new(pool);

    let connector = GenericSqlConnector {
        introspector: Arc::new(ClickHouseIntrospector {
            connection_string: SecretString::from(connection_string.clone()),
        }),
        factory: Arc::new(factory),
        schema_mapping: SchemaMappingRule::standard(&params.schema_mapping),
    };

    connector.register(params).await
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
        let password_str: String = password.to_string();
        params.insert("password".to_string(), SecretString::from(password_str));
    }

    let pool = ClickHouseConnectionPool::new(params)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create ClickHouse connection pool")?;

    Ok(Arc::new(pool))
}

static CLICKHOUSE_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(reqwest::Client::new);

async fn introspect_clickhouse_tables(connection_string: &str) -> Result<Vec<String>> {
    // Use JSONCompact format for more robust parsing than plain text lines
    let url = Url::parse(connection_string)?;
    let db = url.path().trim_start_matches('/');
    let db_filter = if db.is_empty() { "default" } else { db };

    let sql = "SELECT name FROM system.tables WHERE database = {db:String} FORMAT JSONCompact";

    let resp = CLICKHOUSE_CLIENT
        .post(connection_string)
        .query(&[("param_db", db_filter)])
        .body(sql)
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;

    let mut tables = Vec::new();
    if let Some(data) = resp.get("data").and_then(|v| v.as_array()) {
        for row in data {
            if let Some(row_arr) = row.as_array()
                && let Some(name) = row_arr.first().and_then(|v| v.as_str())
            {
                tables.push(name.to_string());
            }
        }
    }

    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_introspect_clickhouse_tables() -> Result<()> {
        let server: MockServer = MockServer::start().await;

        let mock_response = serde_json::json!({
            "data": [
                ["table1"],
                ["table2"]
            ]
        });

        Mock::given(method("POST"))
            .and(body_string(
                "SELECT name FROM system.tables WHERE database = {db:String} FORMAT JSONCompact",
            ))
            .and(wiremock::matchers::query_param("param_db", "default"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_response))
            .mount(&server)
            .await;

        let tables = introspect_clickhouse_tables(&server.uri()).await?;
        assert_eq!(tables, vec!["table1", "table2"]);
        Ok(())
    }
}
