//! # SQL Database Connectors
//!
//! Provides support for JDBC-style SQL sources including Postgres, MySQL, SQLite, DuckDB, and ClickHouse.
//! Handles dialect-specific SQL generation and type mapping.
//!
//! ## Overview
//!
//! This module acts as the entry point for all SQL-based data sources. It
//! delegates to specific dialect implementations while providing common
//! infrastructure for connection pooling, circuit breaking, and concurrency control.
//!
//! ## Errors
//!
//! - `anyhow::Error` for configuration parsing failures or unsupported dialects.
//! - Specific connector errors (e.g., `postgres::Error`) for database-level failures.
//!
//! ## Performance Characteristics
//!
//! - Uses asynchronous connection pools to manage resources efficiently.
//! - Implements adaptive circuit breaking to protect against cascading failures.
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use crate::sources::SourceProvider;
use strake_common::config::{RetrySettings, SourceConfig, TableConfig};

pub mod base_connector;
pub mod common;
pub mod mysql;
pub mod postgres;
pub mod postgres_federation;
pub mod sqlite;
pub mod sqlite_federation;
pub mod sqlite_introspect;
pub mod strake_federation;
pub mod wrappers;

pub use common::SqlDialect;
use mysql::register_mysql;
use postgres::register_postgres;
use sqlite::register_sqlite;
/// Provides case-insensitive schema resolution for SQL dialects.
pub mod case_insensitive_schema;
pub mod clickhouse;
pub mod duckdb;
pub mod duckdb_federation;
/// Introspection support for DuckDB databases.
pub mod duckdb_introspect;
pub mod oracle;
/// Introspection support for PostgreSQL databases.
pub mod postgres_introspect;
use clickhouse::register_clickhouse;
use duckdb::register_duckdb;
use oracle::register_oracle;

/// A provider for SQL-based data sources.
pub struct SqlSourceProvider {
    /// The default retry settings to use for SQL operations if not overridden.
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
            dialect: Option<SqlDialect>,
            connection: Option<String>,
            #[serde(default = "default_pool_size")]
            pool_size: usize,
            #[serde(default)]
            retry: Option<RetrySettings>,
            #[serde(default)]
            tables: Option<Vec<TableConfig>>,
            #[serde(default)]
            username: Option<String>,
            #[serde(default)]
            password: Option<String>,
            #[serde(default)]
            schema_mapping: strake_common::config::SchemaMappingConfig,
        }
        fn default_pool_size() -> usize {
            10
        }

        let sql_config: SqlConfig =
            serde_json::from_value(config.config.clone()).unwrap_or_else(|_| SqlConfig {
                dialect: None,
                connection: None,
                pool_size: default_pool_size(),
                retry: None,
                tables: None,
                username: None,
                password: None,
                schema_mapping: strake_common::config::SchemaMappingConfig::default(),
            });

        let dialect = if let Some(d) = sql_config.dialect {
            d
        } else {
            match &config.source_type {
                strake_common::models::SourceType::Postgres => SqlDialect::Postgres,
                strake_common::models::SourceType::Mysql => SqlDialect::MySql,
                strake_common::models::SourceType::Sqlite => SqlDialect::Sqlite,
                strake_common::models::SourceType::Clickhouse => SqlDialect::Clickhouse,
                strake_common::models::SourceType::Duckdb => SqlDialect::DuckDB,
                strake_common::models::SourceType::Other(s) => match s.to_lowercase().as_str() {
                    "postgres" => SqlDialect::Postgres,
                    "mysql" => SqlDialect::MySql,
                    "sqlite" => SqlDialect::Sqlite,
                    "clickhouse" => SqlDialect::Clickhouse,
                    "duckdb" => SqlDialect::DuckDB,
                    "oracle" => SqlDialect::Oracle,
                    _ => anyhow::bail!(
                        "SQL dialect must be explicitly configured or inferred from the source type (e.g. 'postgres')"
                    ),
                },
                other => anyhow::bail!("Cannot infer SQL dialect for source type: {:?}", other),
            }
        };

        let mut connection_string = config
            .url
            .clone()
            .or_else(|| sql_config.connection.clone())
            .context("Connection string/URL is required for SQL source registration (specify either 'url' or 'connection')")?;

        connection_string = common::merge_credentials_into_url(
            &connection_string,
            sql_config.username.as_deref(),
            sql_config.password.as_deref(),
        );

        let effective_retry = sql_config.retry.unwrap_or(self.global_retry);

        let tables = if !config.tables.is_empty() {
            config.tables.clone()
        } else {
            sql_config.tables.unwrap_or_default()
        };

        // Schema mapping logic is now handled in GenericSqlConnector::register
        // using SchemaMappingRule, so we just pass the tables through.

        let explicit_tables = if !tables.is_empty() {
            Some(tables)
        } else {
            None
        };

        register_sql_source(common::SqlRegistrationOptions {
            context: Arc::new(context.clone()),
            catalog_name: catalog_name.to_string(),
            name: config.name.to_string(),
            dialect,
            connection_string,
            pool_size: sql_config.pool_size,
            explicit_tables: Arc::new(explicit_tables),
            retry: effective_retry,
            max_concurrent_queries: config.max_concurrent_queries.unwrap_or(0),
            schema_mapping: sql_config.schema_mapping,
        })
        .await
    }
}

/// Registers a SQL data source using the provided options.
#[allow(clippy::too_many_arguments)]
pub async fn register_sql_source(options: common::SqlRegistrationOptions) -> Result<()> {
    use strake_common::circuit_breaker::{AdaptiveCircuitBreaker, CircuitBreakerConfig};
    let cb = Arc::new(AdaptiveCircuitBreaker::new(CircuitBreakerConfig::default()));

    let params = common::SqlSourceParams {
        context: options.context,
        catalog_name: options.catalog_name,
        name: options.name,
        connection_string: options.connection_string.into(),
        pool_size: options.pool_size,
        cb,
        explicit_tables: options.explicit_tables,
        retry: options.retry,
        max_concurrent_queries: options.max_concurrent_queries,
        schema_mapping: options.schema_mapping,
    };

    match options.dialect {
        SqlDialect::Postgres => register_postgres(params).await,
        SqlDialect::MySql => register_mysql(params).await,
        SqlDialect::Sqlite => register_sqlite(params).await,
        SqlDialect::Clickhouse => register_clickhouse(params).await,
        SqlDialect::DuckDB => register_duckdb(params).await,
        SqlDialect::Oracle => register_oracle(params).await,
    }
}
