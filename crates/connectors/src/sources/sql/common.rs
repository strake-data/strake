use anyhow::Result;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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

pub fn next_retry_delay(attempt: u32, base_ms: u64, max_ms: u64) -> Duration {
    let multiplier = 2_u64.saturating_pow(attempt);
    let delay = base_ms.saturating_mul(multiplier);
    let jitter = rand::random::<u64>() % 1000; // 1s jitter max
    let total = delay.saturating_add(jitter);
    Duration::from_millis(total.min(max_ms))
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
}
