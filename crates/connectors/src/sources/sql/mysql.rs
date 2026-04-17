//! # MySQL Connector
//!
//! Provides integration for MySQL data sources, including table discovery
//! and provider creation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use mysql_async::params;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::sync::Arc;
use strake_common::circuit_breaker::AdaptiveCircuitBreaker;

use super::base_connector::GenericSqlConnector;
use super::common::{
    FetchedMetadata, SchemaMappingRule, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams,
};
use crate::introspect::{IntrospectError, SchemaIntrospector, TableRef};
use globset::GlobMatcher;

pub struct MySqlMetadataFetcher {
    pub connection_string: SecretString,
}

#[async_trait]
impl SqlMetadataFetcher for MySqlMetadataFetcher {
    async fn fetch_metadata(&self, _schema: &str, table: &str) -> Result<FetchedMetadata> {
        fetch_mysql_comments(self.connection_string.expose_secret(), table).await
    }
}

// FIXME: MySQL presently lacks federation support (join pushdown).
// It should be migrated to GenericFederatedTableFactory in the future.

pub struct MySQLTableFactoryWrapper {
    pub factory: MySQLTableFactory,
}

#[async_trait]
impl SqlProviderFactory for MySQLTableFactoryWrapper {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: Arc<FetchedMetadata>,
        cb: Arc<AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .factory
            .table_provider(table_ref)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Wrap with metadata and circuit breaker.
        // MySQL is usually a remote federated source (or at least treated as such),
        // so we enable schema drift detection.
        Ok(super::wrappers::wrap_provider(inner, cb, metadata, true))
    }
}

pub struct MySqlIntrospector {
    pub connection_string: SecretString,
}

#[async_trait]
impl SchemaIntrospector for MySqlIntrospector {
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError> {
        let tables = introspect_mysql_tables(self.connection_string.expose_secret())
            .await
            .map_err(|e| IntrospectError::Query(e.to_string()))?;

        let mut filtered = Vec::new();
        for table in tables {
            let table_ref = TableRef {
                schema: "public".into(), // MySQL doesn't have schemas in the same way, usually use Database()
                table,
            };
            if let Some(matcher) = pattern {
                if matcher.is_match(&table_ref.table) {
                    filtered.push(table_ref);
                }
            } else {
                filtered.push(table_ref);
            }
        }
        Ok(filtered)
    }

    async fn introspect_table(
        &self,
        table: &TableRef,
        _full: bool,
    ) -> Result<strake_common::schema::IntrospectedTable, IntrospectError> {
        // Basic introspection for MySQL
        Ok(strake_common::schema::IntrospectedTable {
            source: "mysql".to_string(),
            schema: table.schema.clone(),
            name: table.table.clone(),
            columns: vec![],
            db_comment: None,
            ai_description: None,
        })
    }
}

/// Registers a MySQL source into the provided context.
pub async fn register_mysql(params: SqlSourceParams) -> Result<()> {
    let connection_string = params.connection_string.clone();
    let pool_size = params.pool_size;

    let mut pool_params = HashMap::new();
    pool_params.insert(
        "connection_string".to_string(),
        SecretString::from(connection_string.clone()),
    );
    pool_params.insert(
        "max_pool_size".to_string(),
        SecretString::from(pool_size.to_string()),
    );
    let pool = MySQLConnectionPool::new(pool_params)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create MySQL connection pool")?;
    let factory = MySQLTableFactory::new(Arc::new(pool));
    let factory_wrapper = MySQLTableFactoryWrapper { factory };

    let connector = GenericSqlConnector {
        introspector: Arc::new(MySqlIntrospector {
            connection_string: SecretString::from(connection_string.clone()),
        }),
        factory: Arc::new(factory_wrapper),
        metadata_fetcher: Some(Arc::new(MySqlMetadataFetcher {
            connection_string: SecretString::from(connection_string.clone()),
        })),
        schema_mapping: SchemaMappingRule::Standard,
    };

    connector.register(params).await
}

pub async fn introspect_mysql_tables(connection_string: &str) -> Result<Vec<String>> {
    use mysql_async::prelude::Queryable;
    let pool = mysql_async::Pool::new(connection_string);
    let mut conn = pool
        .get_conn()
        .await
        .context("Failed to connect to MySQL for introspection")?;

    let rows: Vec<String> = conn
        .query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'")
        .await
        .context("Failed to query information_schema.tables in MySQL")?;

    Ok(rows)
}

pub async fn fetch_mysql_comments(connection_string: &str, table: &str) -> Result<FetchedMetadata> {
    use mysql_async::prelude::Queryable;
    let pool = mysql_async::Pool::new(connection_string);
    let mut conn = pool
        .get_conn()
        .await
        .context("Failed to connect to MySQL for metadata")?;

    // Table comment
    let table_desc: Option<String> = conn
        .exec_first(
            "SELECT table_comment FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :table",
            params! { "table" => table },
        )
        .await
        .context("Failed to query MySQL table metadata")?;

    // Column comments
    let col_rows: Vec<(String, String)> = conn
        .exec(
            "SELECT column_name, column_comment FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = :table",
            params! { "table" => table },
        )
        .await
        .context("Failed to query MySQL column metadata")?;

    let mut metadata = FetchedMetadata {
        table_description: table_desc.filter(|s| !s.is_empty()),
        columns: HashMap::new(),
    };

    for (name, comment) in col_rows {
        if !comment.is_empty() {
            metadata.columns.insert(name, comment);
        }
    }

    Ok(metadata)
}
