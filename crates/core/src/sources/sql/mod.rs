use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use crate::config::{RetrySettings, SourceConfig, TableConfig};
use crate::sources::SourceProvider;

pub mod common;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
pub mod wrappers;

pub use common::SqlDialect;
use mysql::register_mysql;
use postgres::register_postgres;
use sqlite::register_sqlite;
pub mod clickhouse;
pub mod duckdb;
use clickhouse::register_clickhouse;
// use duckdb::register_duckdb;

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
        fn default_pool_size() -> usize {
            10
        }

        let sql_config: SqlConfig = serde_yaml::from_value(config.config.clone())
            .context("Failed to parse SQL source configuration")?;

        let effective_retry = sql_config.retry.unwrap_or(self.global_retry);
        register_sql_source(common::SqlRegistrationOptions {
            context,
            catalog_name,
            name: &config.name,
            dialect: sql_config.dialect,
            connection_string: &sql_config.connection,
            pool_size: sql_config.pool_size,
            explicit_tables: &sql_config.tables,
            retry: effective_retry,
        })
        .await
    }
}

pub async fn register_sql_source(options: common::SqlRegistrationOptions<'_>) -> Result<()> {
    use crate::query::circuit_breaker::{AdaptiveCircuitBreaker, CircuitBreakerConfig};
    let cb = Arc::new(AdaptiveCircuitBreaker::new(CircuitBreakerConfig::default()));

    let params = common::SqlSourceParams {
        context: options.context,
        catalog_name: options.catalog_name,
        name: options.name,
        connection_string: options.connection_string,
        pool_size: options.pool_size,
        cb,
        explicit_tables: options.explicit_tables,
        retry: options.retry,
    };

    match options.dialect {
        SqlDialect::Postgres => register_postgres(params).await,
        SqlDialect::MySql => register_mysql(params).await,
        SqlDialect::Sqlite => register_sqlite(params).await,
        SqlDialect::Clickhouse => register_clickhouse(params).await,
    }
}
