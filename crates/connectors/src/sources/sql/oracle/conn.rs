//! # Oracle Database Connection
//!
//! Implements connection pooling, query execution, and Arrow extraction for Oracle.

use async_trait::async_trait;
use bb8_oracle::OracleConnectionManager;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    config::ConfigExtension,
    execution::SendableRecordBatchStream,
    sql::TableReference,
};
use std::{any::Any, sync::Arc};
use tokio::task;

use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error,
};

/// Smallest accepted `fetch_array_size` / `prefetch_rows` value.
pub const MIN_FETCH: u32 = 1;
/// Largest accepted `fetch_array_size` / `prefetch_rows` value.
///
/// The fetch size is used to preallocate a `Vec` of rows. An unbounded value
/// turns a stray environment variable into a multi-gigabyte allocation, and
/// `Vec::with_capacity` failure is a process abort rather than a recoverable
/// error. OCI also rejects oversized array sizes (ORA-00939).
pub const MAX_FETCH: u32 = 1_000_000;

fn config_err(key: &str, detail: impl std::fmt::Display) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Configuration(format!("oracle.{key}: {detail}"))
}

/// Parses and range-checks a fetch-size setting.
///
/// # Errors
///
/// Returns a `Configuration` error if the value is not a `u32` or falls outside
/// `MIN_FETCH..=MAX_FETCH`. "Unparseable" and "out of range" are both rejected
/// rather than falling back to the default, so a typo surfaces instead of
/// silently reverting to 8192.
fn validated_fetch(key: &str, raw: &str) -> datafusion::common::Result<u32> {
    let n: u32 = raw
        .parse()
        .map_err(|e| config_err(key, format!("not a valid integer: {e} (got {raw:?})")))?;
    if !(MIN_FETCH..=MAX_FETCH).contains(&n) {
        return Err(config_err(
            key,
            format!("must be in {MIN_FETCH}..={MAX_FETCH}, got {n}"),
        ));
    }
    Ok(n)
}

/// Configuration options for Oracle connections and query execution.
#[derive(Debug, Clone)]
pub struct OracleSourceConfig {
    /// Number of rows to fetch per round-trip (defaults to 8192).
    pub fetch_array_size: u32,
    /// Number of rows to prefetch into client buffers (defaults to 8192).
    pub prefetch_rows: u32,
}

impl Default for OracleSourceConfig {
    fn default() -> Self {
        Self {
            fetch_array_size: 8192,
            prefetch_rows: 8192,
        }
    }
}

impl OracleSourceConfig {
    /// Constructs `OracleSourceConfig` from the `ORACLE_FETCH_ARRAY_SIZE`
    /// environment variable.
    ///
    /// The `SessionConfig` extension registered by
    /// [`ConfigExtension::with_extension`] takes precedence: this constructor is
    /// only the bootstrap source for the default extension value, and applies
    /// only when the variable is set.
    ///
    /// # Errors
    ///
    /// Returns a `Configuration` error if `ORACLE_FETCH_ARRAY_SIZE` is set to
    /// something that is not a `u32` in `MIN_FETCH..=MAX_FETCH`. The error is
    /// propagated rather than swallowed, which previously meant a typo silently
    /// produced the 8192 default.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// unsafe { std::env::set_var("ORACLE_FETCH_ARRAY_SIZE", "16384"); }
    /// let cfg = strake_connectors::sources::sql::oracle::conn::OracleSourceConfig::from_env()?;
    /// assert_eq!(cfg.fetch_array_size, 16384);
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_env() -> datafusion::common::Result<Self> {
        let size = match std::env::var("ORACLE_FETCH_ARRAY_SIZE") {
            Ok(raw) => validated_fetch("fetch_array_size", &raw)?,
            // An absent variable is a normal default, not an error.
            Err(std::env::VarError::NotPresent) => Self::default().fetch_array_size,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(config_err(
                    "fetch_array_size",
                    "ORACLE_FETCH_ARRAY_SIZE is not valid UTF-8",
                ));
            }
        };
        Ok(Self {
            fetch_array_size: size,
            prefetch_rows: size,
        })
    }
}

impl datafusion::common::config::ExtensionOptions for OracleSourceConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn datafusion::common::config::ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::common::Result<()> {
        match key {
            "fetch_array_size" => {
                self.fetch_array_size = validated_fetch(key, value)?;
                Ok(())
            }
            "prefetch_rows" => {
                self.prefetch_rows = validated_fetch(key, value)?;
                Ok(())
            }
            _ => Err(datafusion::common::DataFusionError::Configuration(format!(
                "OracleSourceConfig does not support key '{}'",
                key
            ))),
        }
    }

    fn entries(&self) -> Vec<datafusion::common::config::ConfigEntry> {
        vec![
            datafusion::common::config::ConfigEntry {
                key: "oracle.fetch_array_size".to_string(),
                value: Some(self.fetch_array_size.to_string()),
                description: "Number of rows to fetch per round-trip",
            },
            datafusion::common::config::ConfigEntry {
                key: "oracle.prefetch_rows".to_string(),
                value: Some(self.prefetch_rows.to_string()),
                description: "Number of rows to prefetch into client buffers",
            },
        ]
    }
}

impl ConfigExtension for OracleSourceConfig {
    const PREFIX: &'static str = "oracle";
}

/// A pooled connection to an Oracle database.
pub type OraclePooledConnection = bb8::PooledConnection<'static, OracleConnectionManager>;

/// A wrapper around a pooled Oracle database connection.
pub struct OracleConnection {
    /// The underlying pooled connection.
    pub conn: OraclePooledConnection,
    /// Configuration options for this connection.
    pub config: OracleSourceConfig,
}

impl OracleConnection {
    /// Creates a new `OracleConnection` from a pooled connection with default configuration.
    pub fn new(conn: OraclePooledConnection) -> Self {
        Self {
            conn,
            config: OracleSourceConfig::default(),
        }
    }

    /// Creates a new `OracleConnection` with custom configuration.
    pub fn with_config(conn: OraclePooledConnection, config: OracleSourceConfig) -> Self {
        Self { conn, config }
    }
}

impl DbConnection<OraclePooledConnection, rust_oracle::sql_type::OracleType> for OracleConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(
        &self,
    ) -> Option<&dyn AsyncDbConnection<OraclePooledConnection, rust_oracle::sql_type::OracleType>>
    {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<OraclePooledConnection, rust_oracle::sql_type::OracleType>
    for OracleConnection
{
    fn new(conn: OraclePooledConnection) -> Self {
        Self {
            conn,
            config: OracleSourceConfig::default(),
        }
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> std::result::Result<SchemaRef, Error> {
        let table_name = table_reference.table().to_uppercase();
        let schema_name = table_reference.schema().map(|s| s.to_uppercase());

        let conn = self.conn.clone();

        let rows = task::spawn_blocking(move || {
            if let Some(schema) = schema_name {
                let rows = conn.query(
                    "SELECT column_name, data_type, data_precision, data_scale, nullable 
                     FROM all_tab_columns 
                     WHERE owner = :1 AND table_name = :2 
                     ORDER BY column_id",
                    &[&schema, &table_name],
                )?;
                rows.collect::<std::result::Result<Vec<rust_oracle::Row>, _>>()
            } else {
                let rows = conn.query(
                    "SELECT column_name, data_type, data_precision, data_scale, nullable 
                     FROM all_tab_columns 
                     WHERE table_name = :1 
                     ORDER BY column_id",
                    &[&table_name],
                )?;
                rows.collect::<std::result::Result<Vec<rust_oracle::Row>, _>>()
            }
        })
        .await
        .map_err(|e| Error::UnableToGetSchema {
            source: Box::new(e),
        })?
        .map_err(|e| Error::UnableToGetSchema {
            source: Box::new(e),
        })?;

        let mut fields = Vec::new();

        for row in rows {
            let column_name: String = row.get(0).map_err(|e| Error::UnableToGetSchema {
                source: Box::new(e),
            })?;
            let data_type_str: String = row.get(1).map_err(|e| Error::UnableToGetSchema {
                source: Box::new(e),
            })?;
            let precision: Option<i32> = row.get(2).map_err(|e| Error::UnableToGetSchema {
                source: Box::new(e),
            })?;
            let scale: Option<i32> = row.get(3).map_err(|e| Error::UnableToGetSchema {
                source: Box::new(e),
            })?;
            let nullable_str: String = row.get(4).map_err(|e| Error::UnableToGetSchema {
                source: Box::new(e),
            })?;
            let nullable = nullable_str != "N";

            let arrow_type = map_oracle_type_to_arrow(&data_type_str, precision, scale);

            fields.push(Field::new(column_name, arrow_type, nullable));
        }

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Executes a SQL query against Oracle and streams the results as Arrow RecordBatches.
    ///
    /// ## Cancellation Safety
    ///
    /// Synchronous OCI fetching executes within `tokio::task::spawn_blocking`, offloading synchronous
    /// I/O operations from Tokio's async worker threads. Batches are transmitted across a bounded
    /// channel (`capacity = 4`). If the downstream stream consumer drops or cancels the stream future,
    /// the background task completes its current fetch chunk (up to `fetch_array_size` rows) and then
    /// `tx.blocking_send()` returns an error on the next send attempt, terminating the worker thread
    /// and dropping the OCI `Statement` and `ResultSet` to release server cursor resources.
    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[rust_oracle::sql_type::OracleType],
        projected_schema: Option<SchemaRef>,
    ) -> std::result::Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>>
    {
        let sql = sql.to_string();
        let conn = self.conn.clone();
        let projected_schema_clone = projected_schema.clone();
        let fetch_size = self.config.fetch_array_size;
        let prefetch_rows = self.config.prefetch_rows;

        tracing::debug!("Oracle: executing query: {}", sql);

        let (tx, rx) = tokio::sync::mpsc::channel::<
            std::result::Result<
                arrow::record_batch::RecordBatch,
                Box<dyn std::error::Error + Send + Sync>,
            >,
        >(4);

        task::spawn_blocking(move || {
            let process =
                || -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let mut stmt_builder = conn.statement(&sql);
                    stmt_builder.prefetch_rows(prefetch_rows);
                    stmt_builder.fetch_array_size(fetch_size);
                    let mut stmt = stmt_builder.build()?;
                    let result_set = stmt.query(&[])?;
                    let mut batch_rows = Vec::with_capacity(fetch_size as usize);
                    for row_res in result_set {
                        let row = row_res?;
                        batch_rows.push(row);
                        if batch_rows.len() >= fetch_size as usize {
                            let batch = super::arrow::rows_to_arrow(
                                std::mem::take(&mut batch_rows),
                                &projected_schema_clone,
                            )?;
                            if tx.blocking_send(Ok(batch)).is_err() {
                                return Ok(()); // Receiver dropped (cancellation safety)
                            }
                        }
                    }

                    if !batch_rows.is_empty() {
                        let batch =
                            super::arrow::rows_to_arrow(batch_rows, &projected_schema_clone)?;
                        let _ = tx.blocking_send(Ok(batch));
                    }
                    Ok(())
                };

            if let Err(e) = process() {
                let _ = tx.blocking_send(Err(e));
            }
        });

        let stream = futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Some(res) => {
                    let mapped_res = res.map_err(datafusion::error::DataFusionError::External);
                    Some((mapped_res, rx))
                }
                None => None,
            }
        });

        // We need the schema to build the RecordBatchStreamAdapter.
        // We can get it from the projected_schema, but if it's None, we might not have it upfront.
        // For Oracle, projected_schema is almost always provided. If not, we fall back to an empty schema.
        let stream_schema = projected_schema
            .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(stream_schema, stream),
        ))
    }

    async fn execute(
        &self,
        sql: &str,
        _params: &[rust_oracle::sql_type::OracleType],
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let sql = sql.to_string();
        let conn = self.conn.clone();

        let row_count = task::spawn_blocking(move || {
            let stmt = conn.execute(&sql, &[])?;
            stmt.row_count()
        })
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(row_count)
    }

    async fn tables(&self, schema: &str) -> std::result::Result<Vec<String>, Error> {
        let schema = schema.to_uppercase();
        let conn = self.conn.clone();
        tracing::debug!("Oracle: tables query for schema {}", schema);

        let schema_clone = schema.clone();
        let table_names = task::spawn_blocking(move || {
            let rows = conn.query(
                "SELECT table_name FROM all_tables WHERE owner = :1",
                &[&schema_clone],
            )?;
            let mut result = Vec::new();
            for row in rows {
                let row = row?;
                let val: String = row.get(0)?;
                result.push(val);
            }
            Ok::<Vec<String>, rust_oracle::Error>(result)
        })
        .await
        .map_err(|e| Error::UnableToGetTables {
            source: Box::new(e),
        })?
        .map_err(|e| Error::UnableToGetTables {
            source: Box::new(e),
        })?;

        tracing::debug!(
            "Oracle: tables query for schema {} returned {} tables",
            schema,
            table_names.len()
        );
        Ok(table_names)
    }

    async fn schemas(&self) -> std::result::Result<Vec<String>, Error> {
        let conn = self.conn.clone();
        tracing::debug!("Oracle: schemas query starting");

        let schemas = task::spawn_blocking(move || {
            let rows = conn.query("SELECT username FROM all_users", &[])?;
            let mut result = Vec::new();
            for row in rows {
                let row = row?;
                let val: String = row.get(0)?;
                result.push(val);
            }
            Ok::<Vec<String>, rust_oracle::Error>(result)
        })
        .await
        .map_err(|e| Error::UnableToGetSchemas {
            source: Box::new(e),
        })?
        .map_err(|e| Error::UnableToGetSchemas {
            source: Box::new(e),
        })?;

        tracing::debug!("Oracle: schemas query returned {} schemas", schemas.len());
        Ok(schemas)
    }
}

/// Maps an Oracle data type string and optional precision/scale to an Arrow `DataType`.
pub fn map_oracle_type_to_arrow(
    oracle_type: &str,
    precision: Option<i32>,
    scale: Option<i32>,
) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;

    let type_upper = oracle_type.to_uppercase();
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    match base_type {
        "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" => DataType::Utf8,
        "CLOB" | "NCLOB" | "LONG" => DataType::LargeUtf8,
        "NUMBER" | "NUMERIC" | "DECIMAL" | "DEC" => {
            // Oracle metadata is not directly usable as an Arrow
            // Decimal128(p, s): it reports data_precision = 0 for an
            // unconstrained NUMBER, and data_scale may be negative for exponent
            // notation. Normalize here so the Arrow builder never has to
            // silently substitute a different type behind the caller's back.
            match scale {
                // Exponent notation (e.g. NUMBER(5,-3)). Arrow decimals cannot
                // express a negative scale; every value is a multiple of
                // 10^|scale|, which f64 represents exactly at the precisions
                // Oracle permits. Mapping deliberately beats letting the scale
                // wrap into an unscaled value in a wrong-typed array.
                Some(s) if s < 0 => DataType::Float64,
                scale => {
                    let p = precision.filter(|p| *p > 0).unwrap_or(38);
                    let s = scale.filter(|s| (0..=p).contains(s)).unwrap_or(0);
                    let (p, s) = (p as u8, s as i8);
                    if p > 38 {
                        DataType::Decimal256(p, s)
                    } else {
                        DataType::Decimal128(p, s)
                    }
                }
            }
        }
        "INTEGER" | "INT" | "SMALLINT" => DataType::Int64,
        "FLOAT" | "REAL" | "DOUBLE PRECISION" => DataType::Float64,
        "BINARY_FLOAT" => DataType::Float32,
        "BINARY_DOUBLE" => DataType::Float64,
        "DATE" => {
            use datafusion::arrow::datatypes::TimeUnit;
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        _ if type_upper.contains("TIMESTAMP") => {
            use datafusion::arrow::datatypes::TimeUnit;
            let tz = if type_upper.contains("ZONE") {
                Some("UTC".into())
            } else {
                None
            };
            DataType::Timestamp(TimeUnit::Microsecond, tz)
        }
        "RAW" => DataType::Binary,
        "BLOB" | "LONG RAW" => DataType::LargeBinary,
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::config::ExtensionOptions;

    #[test]
    fn default_fetch_size_is_sane() {
        let cfg = OracleSourceConfig::default();
        assert!(cfg.fetch_array_size >= MIN_FETCH);
        assert!(cfg.fetch_array_size <= MAX_FETCH);
    }

    #[test]
    fn set_accepts_values_in_range() {
        let mut cfg = OracleSourceConfig::default();
        for v in [MIN_FETCH, 1024, 8192, MAX_FETCH] {
            cfg.set("fetch_array_size", &v.to_string()).unwrap();
            assert_eq!(cfg.fetch_array_size, v);
        }
    }

    #[test]
    fn set_rejects_out_of_range_values() {
        let mut cfg = OracleSourceConfig::default();
        // Would preallocate a ~96 GB Vec<Row>; Vec::with_capacity failure is a
        // process abort, so this must be rejected before it reaches the driver.
        let err = cfg.set("fetch_array_size", "4000000000").unwrap_err();
        assert!(
            err.to_string().contains("must be in"),
            "expected range error, got {err}"
        );
        assert_eq!(cfg.fetch_array_size, 8192, "must not mutate on error");

        assert!(cfg.set("fetch_array_size", "0").is_err());
        assert!(cfg.set("prefetch_rows", "0").is_err());
    }

    #[test]
    fn set_rejects_non_numeric_and_unknown_keys() {
        let mut cfg = OracleSourceConfig::default();
        assert!(cfg.set("fetch_array_size", "abc").is_err());
        assert!(cfg.set("fetch_array_size", "").is_err());
        assert!(cfg.set("fetch_array_size", "-1").is_err());
        assert!(cfg.set("nonsense", "1").is_err());
    }

    #[test]
    fn config_is_reachable_through_session_config() {
        // The bug this guards: the extension was implemented but never
        // registered, so SET oracle.fetch_array_size failed with
        // "unrecognized configuration parameter" and the value was inert.
        let mut cfg = datafusion::prelude::SessionConfig::new();
        cfg.options_mut()
            .extensions
            .insert(OracleSourceConfig::from_env().unwrap());
        cfg.options_mut()
            .set("oracle.fetch_array_size", "4096")
            .expect("SET oracle.fetch_array_size must be accepted");

        let resolved = cfg
            .options()
            .extensions
            .get::<OracleSourceConfig>()
            .expect("extension must be readable back");
        assert_eq!(resolved.fetch_array_size, 4096);
    }

    #[test]
    fn config_validation_rejects_out_of_range_directly() {
        assert!(validated_fetch("fetch_array_size", "1000001").is_err());
        assert_eq!(
            validated_fetch("fetch_array_size", "1000000").unwrap(),
            1_000_000
        );
    }
}
