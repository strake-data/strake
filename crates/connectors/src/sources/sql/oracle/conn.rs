//! # Oracle Database Connection
//!
//! Implements connection pooling, query execution, and Arrow extraction for Oracle.

use async_trait::async_trait;
use bb8_oracle::OracleConnectionManager;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    execution::SendableRecordBatchStream,
    sql::TableReference,
};
use std::{any::Any, sync::Arc};
use tokio::task;

use datafusion_table_providers::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error,
};

pub type OraclePooledConnection = bb8::PooledConnection<'static, OracleConnectionManager>;

pub struct OracleConnection {
    pub conn: OraclePooledConnection,
}

impl OracleConnection {
    pub fn new(conn: OraclePooledConnection) -> Self {
        Self { conn }
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
        Self { conn }
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
                    let result_set = conn.query(&sql, &[])?;
                    let mut batch_rows = Vec::with_capacity(8192);
                    for row_res in result_set {
                        let row = row_res?;
                        batch_rows.push(row);
                        if batch_rows.len() >= 8192 {
                            let batch = super::arrow::rows_to_arrow(
                                std::mem::take(&mut batch_rows),
                                &projected_schema_clone,
                            )?;
                            if tx.blocking_send(Ok(batch)).is_err() {
                                return Ok(()); // Receiver dropped
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
            let p = precision.unwrap_or(38) as u8;
            let s = scale.unwrap_or(0) as i8;
            if p > 38 {
                DataType::Decimal256(p, s)
            } else {
                DataType::Decimal128(p, s)
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
