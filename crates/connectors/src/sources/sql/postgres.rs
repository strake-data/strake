//! # Postgres Connector
//!
//! Provides integration for PostgreSQL data sources, including table discovery,
//! execution via `tokio-postgres`, and federation support.

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::Config;

use super::base_connector::GenericSqlConnector;
use super::common::{
    FetchedMetadata, GenericFederatedTableFactory, SchemaMappingRule, SqlMetadataFetcher,
    SqlSourceParams, TableFactory,
};
use super::postgres_introspect::PostgresIntrospector;

pub struct PostgresMetadataFetcher {
    pub connection_string: SecretString,
}

#[async_trait]
impl SqlMetadataFetcher for PostgresMetadataFetcher {
    async fn fetch_metadata(&self, schema: &str, table: &str) -> Result<FetchedMetadata> {
        fetch_postgres_comments(self.connection_string.expose_secret(), schema, table).await
    }
}

#[async_trait]
impl TableFactory for PostgresTableFactory {
    async fn table_provider(&self, table_ref: TableReference) -> Result<Arc<dyn TableProvider>> {
        self.table_provider(table_ref)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

/// Registers a PostgreSQL source into the provided context.
pub async fn register_postgres(params: SqlSourceParams) -> Result<()> {
    let connection_string = params.connection_string.clone();
    let pool_size = params.pool_size;

    let pool = create_pg_pool(connection_string.expose_secret(), pool_size).await?;
    let inner_factory = PostgresTableFactory::new(pool);

    let executor = super::postgres_federation::PostgresExecutor::new(connection_string.clone());
    let federation_provider = executor.create_federation_provider();

    let factory = GenericFederatedTableFactory {
        inner_factory,
        federation_provider,
        schema_drift: true,
    };

    let connector = GenericSqlConnector {
        introspector: Arc::new(PostgresIntrospector {
            connection_string: SecretString::from(connection_string.clone()),
        }),
        factory: Arc::new(factory),
        metadata_fetcher: Some(Arc::new(PostgresMetadataFetcher {
            connection_string: SecretString::from(connection_string.clone()),
        })),
        schema_mapping: SchemaMappingRule::Standard,
    };

    connector.register(params).await
}

async fn create_pg_pool(
    connection_string: &str,
    pool_size: usize,
) -> Result<Arc<PostgresConnectionPool>> {
    use secrecy::SecretString;
    use tokio_postgres::config::Host;

    let mut params = HashMap::new();
    let config = connection_string
        .parse::<Config>()
        .context("Failed to parse postgres connection string")?;

    for host in config.get_hosts() {
        match host {
            Host::Tcp(h) => {
                params.insert("host".to_string(), SecretString::from(h.clone()));
            }
            #[cfg(unix)]
            Host::Unix(_) => {
                // Unix sockets not handled in this map
            }
        }
    }

    params.insert(
        "user".to_string(),
        SecretString::from(config.get_user().unwrap_or("postgres").to_string()),
    );

    if let Some(password) = config.get_password() {
        let pass_str = std::str::from_utf8(password).context("Invalid password encoding")?;
        params.insert(
            "password".to_string(),
            SecretString::from(pass_str.to_string()),
        );
        params.insert("pass".to_string(), SecretString::from(pass_str.to_string()));
    }

    if let Some(dbname) = config.get_dbname() {
        params.insert("dbname".to_string(), SecretString::from(dbname.to_string()));
    }

    if let Some(port) = config.get_ports().first() {
        params.insert("port".to_string(), SecretString::from(port.to_string()));
    }

    params.insert(
        "max_pool_size".to_string(),
        SecretString::from(pool_size.to_string()),
    );
    let mut ssl_mode = match config.get_ssl_mode() {
        tokio_postgres::config::SslMode::Disable => "disable",
        tokio_postgres::config::SslMode::Prefer => "prefer",
        tokio_postgres::config::SslMode::Require => "require",
        _ => "require",
    };

    // If tokio-postgres doesn't expose the strict modes in its enum yet,
    // we check the connection string to avoid silent downgrades.
    if connection_string.contains("sslmode=verify-ca") {
        ssl_mode = "verify-ca";
    } else if connection_string.contains("sslmode=verify-full") {
        ssl_mode = "verify-full";
    }
    params.insert(
        "sslmode".to_string(),
        SecretString::from(ssl_mode.to_string()),
    );

    let pool = PostgresConnectionPool::new(params)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to create Postgres connection pool")?;
    Ok(Arc::new(pool))
}

pub async fn introspect_pg_tables(connection_string: &str) -> Result<Vec<String>> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("Failed to connect to Postgres for introspection")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Postgres connection error: {}", e);
        }
    });

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
            &[],
        )
        .await
        .context("Failed to query information_schema.tables")?;

    Ok(rows.iter().map(|row| row.get(0)).collect())
}

pub async fn fetch_postgres_comments(
    connection_string: &str,
    schema: &str,
    table: &str,
) -> Result<FetchedMetadata> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("Failed to connect to Postgres for metadata")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Postgres connection error: {}", e);
        }
    });

    // Query for both table (objsubid=0) and column (objsubid>0) descriptions
    let rows = client
        .query(
            "
        SELECT
            d.objsubid,
            d.description,
            a.attname as column_name
        FROM pg_description d
        JOIN pg_class c ON c.oid = d.objoid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
        WHERE c.relname = $2 AND n.nspname = $1
        ",
            &[&schema, &table],
        )
        .await
        .context("Failed to query postgres metadata")?;

    let mut metadata = FetchedMetadata::default();

    for row in rows {
        let objsubid: i32 = row.get(0);
        let desc: String = row.get(1);

        if objsubid == 0 {
            // Table description
            metadata.table_description = Some(desc);
        } else {
            // Column description
            if let Some(col_name) = row.get::<_, Option<String>>(2) {
                metadata.columns.insert(col_name, desc);
            }
        }
    }
    Ok(metadata)
}
