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

/// Errors that can occur when managing the Oracle connection pool.
#[derive(Debug, Error)]
pub enum OraclePoolError {
    /// An error occurred during a basic Oracle connection operation.
    #[error("Oracle connection failed: {0}")]
    ConnectionError(#[from] rust_oracle::Error),

    /// An error occurred while creating the connection pool.
    #[error("Unable to create Oracle connection pool: {0}")]
    PoolCreationError(#[from] bb8_oracle::Error),

    /// An error occurred while retrieving a connection from the pool.
    #[error("Unable to get Oracle connection from pool: {0}")]
    PoolRunError(String),
}

/// A specialized `Result` type for Oracle pool operations.
pub type Result<T, E = OraclePoolError> = std::result::Result<T, E>;

/// A connection pool for Oracle databases, using `bb8`.
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
    /// The timezone to set on the Oracle session.
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

fn parse_oracle_connect_string(
    connection_string: &str,
) -> std::result::Result<(String, String, String), rust_oracle::Error> {
    let url = url::Url::parse(connection_string).map_err(|e| {
        rust_oracle::Error::new(rust_oracle::ErrorKind::InternalError, e.to_string())
    })?;

    let user = url.username().to_string();
    let password = url.password().unwrap_or("").to_string();
    let host = url.host_str().unwrap_or("localhost");
    let port = url.port();
    let service_name = url.path().trim_start_matches('/');

    let connect_string = if port.is_none() && service_name.is_empty() {
        host.to_string()
    } else {
        let port = port.unwrap_or(1521);
        format!("{}:{}/{}", host, port, service_name)
    };

    Ok((user, password, connect_string))
}

impl OracleConnectionPool {
    /// Creates a new `OracleConnectionPool` with the given connection string and pool size.
    pub async fn new(connection_string: &str, pool_size: usize) -> Result<Self> {
        tracing::info!("Oracle: creating connection pool");

        let (user, password, connect_string) = parse_oracle_connect_string(connection_string)
            .map_err(OraclePoolError::ConnectionError)?;

        tracing::debug!(
            "Oracle: connector params: user={}, connect_string={}",
            user,
            connect_string
        );

        let connector = Connector::new(&user, &password, connect_string);

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

    /// Returns the connection string for this pool.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_oracle_connect_string_ezconnect() {
        let conn_str = "oracle://system:password123@localhost:1521/FREEPDB1";
        let (user, password, connect_string) = parse_oracle_connect_string(conn_str).unwrap();

        assert_eq!(user, "system");
        assert_eq!(password, "password123");
        assert_eq!(connect_string, "localhost:1521/FREEPDB1");
    }

    #[test]
    fn test_parse_oracle_connect_string_tns_alias() {
        let conn_str = "oracle://system:password123@MY_PROD_DB";
        let (user, password, connect_string) = parse_oracle_connect_string(conn_str).unwrap();

        assert_eq!(user, "system");
        assert_eq!(password, "password123");
        assert_eq!(connect_string, "MY_PROD_DB");
    }
}
