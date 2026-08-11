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
//! ## Usage
//!
//! ```rust
//! use datafusion::prelude::SessionContext;
//! use strake_connectors::sources::SourceProvider;
//! use strake_connectors::sources::sql::SqlSourceProvider;
//! use strake_common::config::RetrySettings;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//! let provider = SqlSourceProvider {
//!     global_retry: RetrySettings::default(),
//! };
//! assert_eq!(provider.type_name(), "sql");
//! # Ok(())
//! # }
//! ```
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
            #[serde(default)]
            dialect: Option<SqlDialect>,
            #[serde(default)]
            url: Option<String>,
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

        let sql_config: SqlConfig = serde_json::from_value(config.config.clone()).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse SQL source configuration from '{:?}': {}",
                config.config,
                e
            )
        })?;

        let dialect = match sql_config.dialect {
            Some(d) => d,
            None => SqlDialect::try_from(&config.source_type)?,
        };

        let mut connection_string = config
            .url
            .clone()
            .or_else(|| sql_config.url.clone())
            .context("Connection string/URL is required for SQL source registration (specify either 'url' or 'connection')")?;

        let username = config
            .username
            .as_deref()
            .or(sql_config.username.as_deref());
        let password = config
            .password
            .as_ref()
            .map(|p| {
                use secrecy::ExposeSecret;
                p.expose_secret()
            })
            .or(sql_config.password.as_deref());

        connection_string =
            common::merge_credentials_into_url(&connection_string, username, password);

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
