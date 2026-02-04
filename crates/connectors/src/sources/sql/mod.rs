//! SQL Database connectors.
//!
//! Provides support for JDBC-style SQL sources including Postgres, MySQL, SQLite, DuckDB, and ClickHouse.
//! Handles dialect-specific SQL generation and type mapping.
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use crate::sources::SourceProvider;
use strake_common::config::{RetrySettings, SourceConfig, TableConfig};

pub mod common;
pub mod mysql;
pub mod postgres;
pub mod postgres_federation;
pub mod sqlite;
pub mod sqlite_federation;
pub mod wrappers;

pub use common::SqlDialect;
use mysql::register_mysql;
use postgres::register_postgres;
use sqlite::register_sqlite;
pub mod clickhouse;
pub mod duckdb;
pub mod duckdb_federation;
use clickhouse::register_clickhouse;
use duckdb::register_duckdb;

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

        let sql_config: SqlConfig = serde_json::from_value(config.config.clone())
            .context("Failed to parse SQL source configuration")?;

        let effective_retry = sql_config.retry.unwrap_or(self.global_retry);

        let mut tables = if !config.tables.is_empty() {
            config.tables.clone()
        } else {
            sql_config.tables.unwrap_or_default()
        };

        // Refined Schema Rule for SQL:
        // If schema is empty or "public", use the source name as the schema namespace.
        for table in &mut tables {
            if table.schema.is_empty() || table.schema == "public" {
                table.schema = config.name.clone();
            }
        }

        let explicit_tables = if !tables.is_empty() {
            Some(tables)
        } else {
            None
        };

        register_sql_source(common::SqlRegistrationOptions {
            context,
            catalog_name,
            name: &config.name,
            dialect: sql_config.dialect,
            connection_string: &sql_config.connection,
            pool_size: sql_config.pool_size,
            explicit_tables: &explicit_tables,
            retry: effective_retry,
            max_concurrent_queries: config.max_concurrent_queries.unwrap_or(0),
        })
        .await
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn register_sql_source(options: common::SqlRegistrationOptions<'_>) -> Result<()> {
    use strake_common::circuit_breaker::{AdaptiveCircuitBreaker, CircuitBreakerConfig};
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
        max_concurrent_queries: options.max_concurrent_queries,
    };

    match options.dialect {
        SqlDialect::Postgres => register_postgres(params).await,
        SqlDialect::MySql => register_mysql(params).await,
        SqlDialect::Sqlite => register_sqlite(params).await,
        SqlDialect::Clickhouse => register_clickhouse(params).await,
        SqlDialect::DuckDB => register_duckdb(params).await,
    }
}
