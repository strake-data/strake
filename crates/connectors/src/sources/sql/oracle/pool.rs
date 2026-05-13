//! # Oracle Connection Pool
//!
//! Provides connection pool management using `bb8` and `rust_oracle`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bb8::CustomizeConnection;
use bb8_oracle::OracleConnectionManager;
use rust_oracle::{Connection, Connector};

use thiserror::Error;

use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::DbConnection;

#[derive(Debug, Error)]
pub enum OraclePoolError {
    #[error("Oracle connection failed: {0}")]
    ConnectionError(#[from] rust_oracle::Error),

    #[error("Unable to create Oracle connection pool: {0}")]
    PoolCreationError(#[from] bb8_oracle::Error),

    #[error("Unable to get Oracle connection from pool: {0}")]
    PoolRunError(String),
}

pub type Result<T, E = OraclePoolError> = std::result::Result<T, E>;

pub struct OracleConnectionPool {
    pool: Arc<bb8::Pool<OracleConnectionManager>>,
    connection_string: String,
}

impl std::fmt::Debug for OracleConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleConnectionPool").finish()
    }
}

/// Customizer that sets session timezone on connection acquire.
#[derive(Debug, Clone)]
pub struct SetTimezoneCustomizer {
    pub timezone: String,
}

impl CustomizeConnection<Arc<Connection>, bb8_oracle::Error> for SetTimezoneCustomizer {
    fn on_acquire<'a>(
        &'a self,
        conn: &'a mut Arc<Connection>,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<(), bb8_oracle::Error>> + Send + 'a>> {
        let sql = format!("ALTER SESSION SET TIME_ZONE = '{}'", self.timezone);
        tracing::debug!("Oracle: customizer executing {}", sql);
        Box::pin(async move {
            // conn.execute is a blocking Oracle OCI call — use block_in_place
            // to avoid stalling the async runtime.
            tokio::task::block_in_place(|| {
                conn.execute(&sql, &[])
                    .map(|_| {
                        tracing::debug!("Oracle: timezone customizer succeeded");
                    })
                    .map_err(|e| {
                        tracing::warn!("Oracle: timezone customizer failed: {}", e);
                        bb8_oracle::Error::Database(e)
                    })
            })?;
            Ok(())
        })
    }
}

impl OracleConnectionPool {
    pub async fn new(connection_string: &str, pool_size: usize) -> Result<Self> {
        tracing::info!("Oracle: creating connection pool");

        // Simple URI parsing for oracle://user:password@host:port/service_name
        let url = url::Url::parse(connection_string).map_err(|e| {
            rust_oracle::Error::new(rust_oracle::ErrorKind::InternalError, e.to_string())
        })?;

        let user = url.username();
        let password = url.password().unwrap_or("");
        let host = url.host_str().unwrap_or("localhost");
        let port = url.port().unwrap_or(1521);
        let service_name = url.path().trim_start_matches('/');

        tracing::debug!(
            "Oracle: connector params: user={}, host={}, port={}, service={}",
            user,
            host,
            port,
            service_name
        );

        let connector = Connector::new(
            user,
            password,
            format!("{}:{}/{}", host, port, service_name),
        );

        let manager = OracleConnectionManager::from_connector(connector);

        tracing::debug!("Oracle: building pool with size {}", pool_size);
        let pool = bb8::Pool::builder()
            .max_size(pool_size as u32)
            .connection_timeout(std::time::Duration::from_secs(30))
            .build(manager)
            .await
            .map_err(OraclePoolError::PoolCreationError)?;

        tracing::info!("Oracle: pool ready");
        Ok(Self {
            pool: Arc::new(pool),
            connection_string: connection_string.to_string(),
        })
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

#[async_trait]
impl
    DbConnectionPool<
        bb8::PooledConnection<'static, OracleConnectionManager>,
        rust_oracle::sql_type::OracleType,
    > for OracleConnectionPool
{
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<
            dyn DbConnection<
                    bb8::PooledConnection<'static, OracleConnectionManager>,
                    rust_oracle::sql_type::OracleType,
                >,
        >,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let conn = Arc::clone(&self.pool).get_owned().await.map_err(|e| {
            Box::new(OraclePoolError::PoolRunError(e.to_string()))
                as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(super::conn::OracleConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::Disallow
    }
}
