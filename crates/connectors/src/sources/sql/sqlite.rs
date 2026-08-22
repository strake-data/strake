//! # SQLite Connector
//!
//! Provides integration for SQLite data sources, including table discovery,
//! extension loading, and federation support.

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::*;
use datafusion_table_providers::sqlite::SqliteTableFactory;
use secrecy::ExposeSecret;
use std::sync::Arc;
use std::time::Duration;

use super::base_connector::GenericSqlConnector;
use super::common::{
    GenericFederatedTableFactory, SchemaMappingRule, SqlSourceParams, TableFactory,
};
use super::sqlite_introspect::SqliteIntrospector;

#[async_trait]
impl TableFactory for SqliteTableFactory {
    async fn table_provider(&self, table_ref: TableReference) -> Result<Arc<dyn TableProvider>> {
        self.table_provider(table_ref)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

/// Registers a SQLite source into the provided context.
pub async fn register_sqlite(params: SqlSourceParams) -> Result<()> {
    let connection_string = params.connection_string.clone();

    let pool = SqliteConnectionPool::new(
        connection_string.expose_secret(),
        Mode::File,
        JoinPushDown::Disallow,
        vec![],
        Duration::from_secs(30),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))
    .context("Failed to create SQLite connection pool")?;
    let inner_factory = SqliteTableFactory::new(Arc::new(pool));

    let executor = super::sqlite_federation::SqliteExecutor::new(connection_string.clone());
    let federation_provider = executor.create_federation_provider();

    let factory = GenericFederatedTableFactory {
        inner_factory,
        federation_provider,
        schema_drift: true,
        max_concurrent_queries: params.max_concurrent_queries,
    };

    let connector = GenericSqlConnector {
        introspector: Arc::new(SqliteIntrospector {
            db_path: connection_string.clone(),
        }),
        factory: Arc::new(factory),
        schema_mapping: SchemaMappingRule::sqlite(&params.schema_mapping),
    };

    connector.register(params).await
}
