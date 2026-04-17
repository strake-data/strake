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
use datafusion_federation::sql::{SQLFederationProvider, SQLTableSource};
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

/// Trait for fetching dialect-specific metadata (comments, descriptions, stats).
#[async_trait]
pub trait SqlMetadataFetcher: Send + Sync {
    /// Fetches metadata for a specific table identifier.
    async fn fetch_metadata(&self, schema: &str, table: &str) -> Result<FetchedMetadata>;
}

/// Factory for creating dialect-specific [`TableProvider`] instances.
#[async_trait]
pub trait SqlProviderFactory: Send + Sync {
    /// Creates a new [`TableProvider`] for the given table reference.
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: Arc<FetchedMetadata>,
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
    pub federation_provider: Arc<SQLFederationProvider>,
    /// Whether to enable schema drift detection.
    pub schema_drift: bool,
}

#[async_trait]
impl<F: TableFactory + Send + Sync> SqlProviderFactory for GenericFederatedTableFactory<F> {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: Arc<FetchedMetadata>,
        cb: Arc<AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .inner_factory
            .table_provider(table_ref.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let wrapped = super::wrappers::wrap_provider(inner, cb, metadata, self.schema_drift);

        let sql_source = SQLTableSource::new_with_schema(
            self.federation_provider.clone(),
            table_ref.into(),
            wrapped.schema(),
        );
        let adaptor =
            FederatedTableProviderAdaptor::new_with_provider(Arc::new(sql_source), wrapped);

        Ok(Arc::new(adaptor))
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum SchemaMappingRule {
    /// Map empty or "public" to source name
    Standard,
    /// Map empty, "public", or "main" to source name (SQLite)
    SQLite,
}

impl SchemaMappingRule {
    pub fn map_schema<'a>(&self, schema: &'a str, source_name: &str) -> std::borrow::Cow<'a, str> {
        match self {
            SchemaMappingRule::Standard => {
                if schema.is_empty() || schema == "public" {
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

#[non_exhaustive]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    Postgres,
    MySql,
    Sqlite,
    Clickhouse,
    #[serde(alias = "duckdb")]
    DuckDB,
}

#[derive(Clone)]
pub struct SqlSourceParams {
    pub context: Arc<SessionContext>,
    pub catalog_name: String,
    pub name: String,
    pub connection_string: String,
    pub pool_size: usize,
    pub cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    pub explicit_tables: Arc<Option<Vec<strake_common::config::TableConfig>>>,
    pub retry: strake_common::config::RetrySettings,
    pub max_concurrent_queries: usize,
}

pub struct SqlRegistrationOptions {
    pub context: Arc<SessionContext>,
    pub catalog_name: String,
    pub name: String,
    pub dialect: SqlDialect,
    pub connection_string: String,
    pub pool_size: usize,
    pub explicit_tables: Arc<Option<Vec<strake_common::config::TableConfig>>>,
    pub retry: strake_common::config::RetrySettings,
    pub max_concurrent_queries: usize,
}
