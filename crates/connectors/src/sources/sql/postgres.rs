use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::Config;

use super::common::{FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams};
use super::wrappers::register_tables;
use strake_common::config::TableConfig;
use strake_common::retry::retry_async;

pub struct PostgresMetadataFetcher {
    pub connection_string: String,
}

#[async_trait]
impl SqlMetadataFetcher for PostgresMetadataFetcher {
    async fn fetch_metadata(&self, schema: &str, table: &str) -> Result<FetchedMetadata> {
        fetch_postgres_comments(&self.connection_string, schema, table).await
    }
}

pub async fn register_postgres(params: SqlSourceParams<'_>) -> Result<()> {
    let context = params.context;
    let catalog_name = params.catalog_name;
    let name = params.name;
    let connection_string = params.connection_string;
    let pool_size = params.pool_size;
    let cb = params.cb.clone();
    let explicit_tables = params.explicit_tables;
    let retry_settings = params.retry;

    retry_async(
        &format!("register_postgres({})", name),
        retry_settings,
        move || {
            let cb = cb.clone();
            async move {
                try_register_postgres(
                    context,
                    catalog_name,
                    name,
                    connection_string,
                    pool_size,
                    cb,
                    explicit_tables,
                )
                .await
            }
        },
    )
    .await
}

async fn try_register_postgres(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    pool_size: usize,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let pool = create_pg_pool(connection_string, pool_size).await?;
    let inner_factory = PostgresTableFactory::new(pool);

    // Create federation provider
    let executor = super::postgres_federation::PostgresExecutor::new(connection_string.to_string());
    let federation_provider = executor.create_federation_provider();

    let factory = FederatedPostgresTableFactory {
        inner: inner_factory,
        federation_provider,
    };

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables
            .iter()
            .map(|t| {
                // FIX: Transform schema - use source name if empty or "public"
                let target_schema = if t.schema.is_empty() || t.schema == "public" {
                    name.to_string()
                } else {
                    t.schema.clone()
                };
                (t.name.clone(), target_schema)
            })
            .collect()
    } else {
        introspect_pg_tables(connection_string)
            .await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(PostgresMetadataFetcher {
        connection_string: connection_string.to_string(),
    }));

    register_tables(
        context,
        catalog_name,
        name,
        fetcher,
        &factory,
        cb,
        tables_to_register,
    )
    .await?;
    Ok(())
}

struct FederatedPostgresTableFactory {
    inner: PostgresTableFactory,
    federation_provider: Arc<datafusion_federation::sql::SQLFederationProvider>,
}

#[async_trait]
impl SqlProviderFactory for FederatedPostgresTableFactory {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        metadata: FetchedMetadata,
        cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner_provider = self
            .inner
            .table_provider(table_ref.clone())
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Wrap the inner provider with metadata and circuit breaker
        let wrapped_inner = super::wrappers::wrap_provider(inner_provider, cb, metadata);

        // Use SQLTableSource for federation logic
        let sql_source = datafusion_federation::sql::SQLTableSource::new_with_schema(
            self.federation_provider.clone(),
            table_ref.into(),
            wrapped_inner.schema(),
        );

        // Wrap with federation adaptor
        // First arg: Source (logical), Second arg: Provider (physical fallback)
        let adaptor = datafusion_federation::FederatedTableProviderAdaptor::new_with_provider(
            Arc::new(sql_source),
            wrapped_inner,
        );

        Ok(Arc::new(adaptor))
    }
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
    params.insert(
        "sslmode".to_string(),
        SecretString::from("disable".to_string()),
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
