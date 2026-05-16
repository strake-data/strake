//! # Oracle Connector
//!
//! Provides the entry point and connector registration for Oracle databases.

use async_trait::async_trait;
use globset::GlobMatcher;
use secrecy::ExposeSecret;
use std::sync::Arc;

use crate::introspect::{IntrospectError, SchemaIntrospector, TableRef};
use strake_common::schema::IntrospectedTable;

use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;

pub mod arrow;
pub mod conn;
pub mod federation;
pub mod pool;
pub mod table;

use super::base_connector::GenericSqlConnector;
use super::common::{GenericFederatedTableFactory, SchemaMappingRule, SqlSourceParams};
use crate::sources::sql::oracle::pool::OracleConnectionPool;
use crate::sources::sql::oracle::table::OracleTableFactory;

/// Registers the Oracle data source with the given parameters.
pub async fn register_oracle(params: SqlSourceParams) -> anyhow::Result<()> {
    tracing::info!("Oracle: registering source {}", params.name);
    let pool = Arc::new(
        OracleConnectionPool::new(params.connection_string.expose_secret(), params.pool_size)
            .await?,
    );
    let inner_factory = OracleTableFactory::new(pool.clone());

    let federation_provider = inner_factory.create_federation_provider();
    let federated_factory = GenericFederatedTableFactory {
        inner_factory,
        federation_provider,
        schema_drift: true,
    };

    let connector = GenericSqlConnector {
        introspector: Arc::new(OracleIntrospector { pool: pool.clone() }),
        factory: Arc::new(federated_factory),
        metadata_fetcher: None,
        schema_mapping: SchemaMappingRule::Standard,
    };

    connector.register(params).await
}

/// Introspects Oracle database schemas to discover tables and columns.
pub struct OracleIntrospector {
    /// The Oracle connection pool used to connect to the database.
    pub pool: Arc<OracleConnectionPool>,
}

#[async_trait]
impl SchemaIntrospector for OracleIntrospector {
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError> {
        tracing::debug!("Oracle: list_tables start");
        let conn = self.pool.connect().await.map_err(|e| {
            tracing::debug!("Oracle: connect failed: {}", e);
            IntrospectError::Connection(e.to_string())
        })?;

        let async_conn = conn.as_async().ok_or_else(|| {
            IntrospectError::Connection("Connection does not support async".to_string())
        })?;

        // Query all tables in one go, excluding well-known system schemas
        let sql = "SELECT owner, table_name FROM all_tables \
                   WHERE owner NOT IN ('SYS', 'OUTLN', 'XDB', 'DBSNMP', 'APPQOSSYS', 'CTXSYS', 'MDSYS', 'ORDDATA', 'ORDSYS', 'LBACSYS', 'WMSYS', 'GGSYS', 'GSMADMIN_INTERNAL', 'DVSYS', 'AUDSYS', 'OLAPSYS', 'OJVMSYS', 'DVF', 'QS', 'QS_CB', 'QS_CBADM', 'QS_CS', 'QS_ES', 'QS_OS', 'QS_WS') \
                   ORDER BY owner, table_name";

        tracing::debug!("Oracle: running global tables query");
        let batches = async_conn.query_arrow(sql, &[], None).await.map_err(|e| {
            tracing::debug!("Oracle: global tables query failed: {}", e);
            IntrospectError::Query(e.to_string())
        })?;

        use futures::StreamExt;
        let mut all_tables = Vec::new();
        let mut stream = batches;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| IntrospectError::Query(e.to_string()))?;
            let owner_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    IntrospectError::Query("Expected string column for owner".to_string())
                })?;
            let table_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    IntrospectError::Query("Expected string column for table_name".to_string())
                })?;

            for i in 0..batch.num_rows() {
                let owner = owner_col.value(i).to_string();
                let table = table_col.value(i).to_string();

                // Construct fully qualified name for pattern matching
                let full_name = format!("{}.{}", owner, table);
                if let Some(matcher) = pattern
                    && !matcher.is_match(&full_name)
                {
                    continue;
                }

                all_tables.push(TableRef {
                    schema: owner,
                    table,
                });
            }
        }

        tracing::info!("Oracle: list_tables returning {} tables", all_tables.len());
        Ok(all_tables)
    }

    async fn introspect_table(
        &self,
        table_ref: &TableRef,
        _full: bool,
    ) -> Result<IntrospectedTable, IntrospectError> {
        let conn = self
            .pool
            .connect()
            .await
            .map_err(|e| IntrospectError::Connection(e.to_string()))?;

        let async_conn = conn.as_async().ok_or_else(|| {
            IntrospectError::Connection("Connection does not support async".to_string())
        })?;
        let table_name = table_ref.table.clone();
        let schema_name = table_ref.schema.clone();
        let tr = TableReference::Partial {
            schema: schema_name.clone().into(),
            table: table_name.clone().into(),
        };

        let schema_ref = async_conn
            .get_schema(&tr)
            .await
            .map_err(|e| IntrospectError::Query(e.to_string()))?;

        Ok(IntrospectedTable {
            source: "oracle".to_string(),
            schema: schema_name,
            name: table_name,
            columns: schema_ref
                .fields()
                .iter()
                .map(|f| strake_common::schema::IntrospectedColumn {
                    name: f.name().clone(),
                    type_str: f.data_type().to_string(),
                    nullable: f.is_nullable(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }
}
