//! # Common SQL Connector Types
//!
//! Shared traits and types used across all SQL-based data connectors,
//! including provider factories and metadata fetchers.

use anyhow::Result;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_federation::FederatedTableProviderAdaptor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use strake_common::circuit_breaker::AdaptiveCircuitBreaker;

/// Enriched metadata for a table or column, typically sourced from database comments.
#[derive(Debug, Default, Clone)]
pub struct FetchedMetadata {
    /// Optional human-readable description of the table.
    pub table_description: Option<String>,
    /// Mapping of column names to their human-readable descriptions.
    pub columns: HashMap<String, String>,
}

/// Factory for creating dialect-specific [`TableProvider`] instances.
#[async_trait]
pub trait SqlProviderFactory: Send + Sync {
    /// Creates a new [`TableProvider`] for the given table reference.
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        cb: Arc<AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// Lower-level factory for dialect-specific table providers without generic wrapping.
#[async_trait]
pub trait TableFactory: Send + Sync {
    /// Creates a raw dialect-specific [`TableProvider`].
    async fn table_provider(&self, table_ref: TableReference) -> Result<Arc<dyn TableProvider>>;
}

/// A generic implementation of [`SqlProviderFactory`] that adds federation support.
///
/// Wraps an inner [`TableFactory`] with a [`SQLFederationProvider`] to enable
/// cross-table join pushdown within the same source.
pub struct GenericFederatedTableFactory<F> {
    /// The inner factory creating the dialect-specific provider.
    pub inner_factory: F,
    /// The federation provider shared across all tables in the source.
    pub federation_provider: Arc<super::strake_federation::StrakeFederationProvider>,
    /// Whether to enable schema drift detection.
    pub schema_drift: bool,
}

#[async_trait]
impl<F: TableFactory + Send + Sync> SqlProviderFactory for GenericFederatedTableFactory<F> {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        cb: Arc<AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .inner_factory
            .table_provider(table_ref.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let wrapped = super::wrappers::wrap_provider(inner, cb, self.schema_drift);

        let sql_source = super::strake_federation::StrakeTableSource::new(
            self.federation_provider.clone(),
            table_ref,
            wrapped.schema(),
        );
        let adaptor =
            FederatedTableProviderAdaptor::new_with_provider(Arc::new(sql_source), wrapped);

        Ok(Arc::new(adaptor))
    }
}

/// Rules for mapping database schema names to Strake source names.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum SchemaMappingRule {
    /// Maps empty schemas or the "public" schema to the Strake source name.
    Standard,
    /// Maps empty, "public", or "main" schemas to the Strake source name (specifically for SQLite).
    SQLite,
}

impl SchemaMappingRule {
    /// Maps a database schema name to a Strake source name based on the rule.
    pub fn map_schema<'a>(&self, schema: &'a str, source_name: &str) -> std::borrow::Cow<'a, str> {
        match self {
            SchemaMappingRule::Standard => {
                if schema.is_empty() || schema == "public" || schema == "main" {
                    std::borrow::Cow::Owned(source_name.to_string())
                } else {
                    std::borrow::Cow::Borrowed(schema)
                }
            }
            SchemaMappingRule::SQLite => {
                if schema.is_empty() || schema == "public" || schema == "main" {
                    std::borrow::Cow::Owned(source_name.to_string())
                } else {
                    std::borrow::Cow::Borrowed(schema)
                }
            }
        }
    }
}

/// Supported SQL dialects for data sources.
#[non_exhaustive]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    /// PostgreSQL dialect.
    Postgres,
    /// MySQL dialect.
    MySql,
    /// SQLite dialect.
    Sqlite,
    /// ClickHouse dialect.
    Clickhouse,
    /// DuckDB dialect.
    #[serde(alias = "duckdb")]
    DuckDB,
    /// Oracle dialect.
    Oracle,
}

impl SqlDialect {
    /// Returns the lowercase string representation of the dialect.
    pub fn as_str(&self) -> &'static str {
        match self {
            SqlDialect::Postgres => "postgres",
            SqlDialect::MySql => "mysql",
            SqlDialect::Sqlite => "sqlite",
            SqlDialect::Clickhouse => "clickhouse",
            SqlDialect::DuckDB => "duckdb",
            SqlDialect::Oracle => "oracle",
        }
    }
}

/// Parameters for creating and registering a SQL-based data source.
#[derive(Clone)]
pub struct SqlSourceParams {
    /// The session context to register the source in.
    pub context: Arc<SessionContext>,
    /// The name of the catalog.
    pub catalog_name: String,
    /// The unique name of the source.
    pub name: String,
    /// The connection string to the database.
    pub connection_string: secrecy::SecretString,
    /// The maximum size of the connection pool.
    pub pool_size: usize,
    /// The circuit breaker for the source.
    pub cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    /// Explicitly listed tables to register (if any).
    pub explicit_tables: Arc<Option<Vec<strake_common::config::TableConfig>>>,
    /// Retry settings for the source.
    pub retry: strake_common::config::RetrySettings,
    /// Maximum number of concurrent queries allowed for this source.
    pub max_concurrent_queries: usize,
}

/// Options for registering a SQL source.
pub struct SqlRegistrationOptions {
    /// The session context to register the source in.
    pub context: Arc<SessionContext>,
    /// The name of the catalog.
    pub catalog_name: String,
    /// The unique name of the source.
    pub name: String,
    /// The dialect of the SQL source.
    pub dialect: SqlDialect,
    /// The connection string to the database.
    pub connection_string: String,
    /// The maximum size of the connection pool.
    pub pool_size: usize,
    /// Explicitly listed tables to register (if any).
    pub explicit_tables: Arc<Option<Vec<strake_common::config::TableConfig>>>,
    /// Retry settings for the source.
    pub retry: strake_common::config::RetrySettings,
    /// Maximum number of concurrent queries allowed for this source.
    pub max_concurrent_queries: usize,
}
