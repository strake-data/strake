use std::sync::Arc;
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use crate::config::{RetrySettings, SourceConfig, TableConfig};
use crate::sources::SourceProvider;

pub mod common;
pub mod postgres;
pub mod mysql;
pub mod sqlite;
pub mod wrappers;

pub use common::SqlDialect;
use postgres::register_postgres;
use mysql::register_mysql;
use sqlite::register_sqlite;
mod clickhouse;
use clickhouse::register_clickhouse;

pub struct SqlSourceProvider {
    pub global_retry: RetrySettings,
}

#[async_trait]
impl SourceProvider for SqlSourceProvider {
    fn type_name(&self) -> &'static str {
        "sql"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        #[derive(serde::Deserialize)]
        struct SqlConfig {
            dialect: SqlDialect,
            connection: String,
            #[serde(default = "default_pool_size")]
            pool_size: usize,
            #[serde(default)]
            retry: Option<RetrySettings>,
            #[serde(default)]
            tables: Option<Vec<TableConfig>>,
        }
        fn default_pool_size() -> usize { 10 }

        let sql_config: SqlConfig = serde_yaml::from_value(config.config.clone())
            .context("Failed to parse SQL source configuration")?;

        let effective_retry = sql_config.retry.unwrap_or(self.global_retry);
        register_sql_source(
            context, 
            catalog_name, 
            &config.name, 
            &sql_config.dialect, 
            &sql_config.connection, 
            sql_config.pool_size, 
            &sql_config.tables, 
            effective_retry
        ).await
    }
}

pub async fn register_sql_source(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    dialect: &SqlDialect,
    connection_string: &str,
    pool_size: usize,
    tables: &Option<Vec<TableConfig>>,
    retry: RetrySettings,
) -> Result<()> {
    use crate::query::circuit_breaker::{AdaptiveCircuitBreaker, CircuitBreakerConfig};
    let cb = Arc::new(AdaptiveCircuitBreaker::new(CircuitBreakerConfig::default()));
    
    match dialect {
        SqlDialect::Postgres => register_postgres(context, catalog_name, name, connection_string, pool_size, cb, tables, retry).await,
        SqlDialect::MySql => register_mysql(context, catalog_name, name, connection_string, pool_size, cb, tables, retry).await,
        SqlDialect::Sqlite => register_sqlite(context, catalog_name, name, connection_string, cb, tables, retry).await,
        SqlDialect::Clickhouse => register_clickhouse(context, catalog_name, name, connection_string, cb, tables, retry).await,
    }
}
