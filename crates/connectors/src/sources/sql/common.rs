use anyhow::Result;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct FetchedMetadata {
    pub table_description: Option<String>,
    pub columns: HashMap<String, String>,
}

#[async_trait]
pub trait SqlMetadataFetcher: Send + Sync {
    async fn fetch_metadata(&self, schema: &str, table: &str) -> Result<FetchedMetadata>;
}

#[async_trait]
pub trait SqlProviderFactory: Send + Sync {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: FetchedMetadata,
        cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>>;
}

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

pub struct SqlSourceParams<'a> {
    pub context: &'a SessionContext,
    pub catalog_name: &'a str,
    pub name: &'a str,
    pub connection_string: &'a str,
    pub pool_size: usize,
    pub cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    pub explicit_tables: &'a Option<Vec<strake_common::config::TableConfig>>,
    pub retry: strake_common::config::RetrySettings,
    pub max_concurrent_queries: usize,
}

pub struct SqlRegistrationOptions<'a> {
    pub context: &'a SessionContext,
    pub catalog_name: &'a str,
    pub name: &'a str,
    pub dialect: SqlDialect,
    pub connection_string: &'a str,
    pub pool_size: usize,
    pub explicit_tables: &'a Option<Vec<strake_common::config::TableConfig>>,
    pub retry: strake_common::config::RetrySettings,
    pub max_concurrent_queries: usize,
}
