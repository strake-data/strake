//! PostgreSQL Federation Executor
//!
//! Implements `SQLExecutor` for PostgreSQL to enable same-source join pushdown.
//! When multiple tables from the same PostgreSQL connection are joined, the
//! entire join can be pushed down to PostgreSQL instead of being executed by DataFusion.

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider};
use std::sync::Arc;

/// PostgreSQL Executor for federation
///
/// Implements `SQLExecutor` to execute federated SQL queries against a PostgreSQL database.
/// The `compute_context` is the connection string, allowing the federation optimizer to
/// identify tables from the same database and push down joins.
#[derive(Debug, Clone)]
pub struct PostgresExecutor {
    connection_string: String,
}

impl PostgresExecutor {
    pub fn new(connection_string: String) -> Self {
        Self { connection_string }
    }

    /// Create a SQLFederationProvider wrapping this executor
    pub fn create_federation_provider(self) -> Arc<SQLFederationProvider> {
        Arc::new(SQLFederationProvider::new(Arc::new(self)))
    }
}

#[async_trait]
impl SQLExecutor for PostgresExecutor {
    fn name(&self) -> &str {
        "postgres"
    }

    fn compute_context(&self) -> Option<String> {
        // Same connection string = same PostgreSQL instance = can push down joins
        Some(self.connection_string.clone())
    }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        Arc::new(datafusion::sql::unparser::dialect::PostgreSqlDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let connection_string = self.connection_string.clone();
        let query = query.to_string();
        let schema_for_task = schema.clone();

        tracing::info!(target: "federation", db = %connection_string, "PostgreSQL executing federated query: {}", query);

        let batch_stream = futures::stream::once(async move {
            let (client, connection) =
                tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // Spawn connection task
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("PostgreSQL connection error: {}", e);
                }
            });

            let rows = client
                .query(&query, &[])
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // Build Arrow arrays from rows
            let column_count = schema_for_task.fields().len();
            let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(column_count);

            for field in schema_for_task.fields() {
                match field.data_type() {
                    DataType::Int16 => builders.push(Box::new(Int16Builder::new())),
                    DataType::Int32 => builders.push(Box::new(Int32Builder::new())),
                    DataType::Int64 => builders.push(Box::new(Int64Builder::new())),
                    DataType::Float32 => builders.push(Box::new(Float32Builder::new())),
                    DataType::Float64 => builders.push(Box::new(Float64Builder::new())),
                    DataType::Utf8 | DataType::LargeUtf8 => {
                        builders.push(Box::new(StringBuilder::new()))
                    }
                    DataType::Boolean => builders.push(Box::new(BooleanBuilder::new())),
                    DataType::Date32 => builders.push(Box::new(Date32Builder::new())),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        builders.push(Box::new(TimestampMicrosecondBuilder::new()))
                    }
                    dt => {
                        return Err(datafusion::error::DataFusionError::NotImplemented(format!(
                            "PostgreSQL federation: Unsupported data type: {:?}",
                            dt
                        )))
                    }
                }
            }

            // Iterate over rows
            for row in &rows {
                for (i, builder) in builders.iter_mut().enumerate() {
                    append_value_to_builder(builder, row, i)?;
                }
            }

            // Finish builders
            let columns = builders.into_iter().map(|mut b| b.finish()).collect();
            let batch = RecordBatch::try_new(schema_for_task, columns)?;

            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batch_stream,
        )))
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        let rows = client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
                &[],
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(names)
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        let rows = client
            .query(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public' ORDER BY ordinal_position",
                &[&table_name],
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mut fields = Vec::new();
        for row in rows {
            let column_name: String = row.get(0);
            let data_type: String = row.get(1);
            let is_nullable: String = row.get(2);

            let dt = map_postgres_type(&data_type);
            let nullable = is_nullable.to_uppercase() == "YES";
            fields.push(Field::new(column_name, dt, nullable));
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

/// Append a value from a Postgres row to the appropriate Arrow builder
fn append_value_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    row: &tokio_postgres::Row,
    col_idx: usize,
) -> datafusion::error::Result<()> {
    // Try each builder type in turn
    if let Some(b) = builder.as_any_mut().downcast_mut::<Int16Builder>() {
        match row.try_get::<_, Option<i16>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
        match row.try_get::<_, Option<i32>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        match row.try_get::<_, Option<i64>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
        match row.try_get::<_, Option<f32>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        match row.try_get::<_, Option<f64>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        match row.try_get::<_, Option<String>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        match row.try_get::<_, Option<bool>>(col_idx) {
            Ok(Some(v)) => b.append_value(v),
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
        // Postgres DATE -> days since Unix epoch
        match row.try_get::<_, Option<chrono::NaiveDate>>(col_idx) {
            Ok(Some(v)) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (v - epoch).num_days() as i32;
                b.append_value(days);
            }
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<TimestampMicrosecondBuilder>()
    {
        // Postgres TIMESTAMP -> microseconds since Unix epoch
        match row.try_get::<_, Option<chrono::NaiveDateTime>>(col_idx) {
            Ok(Some(v)) => {
                let micros = v.and_utc().timestamp_micros();
                b.append_value(micros);
            }
            Ok(None) => b.append_null(),
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    } else {
        return Err(datafusion::error::DataFusionError::Internal(
            "Builder type mismatch".to_string(),
        ));
    }

    Ok(())
}

/// Map PostgreSQL data type strings to Arrow DataTypes
pub fn map_postgres_type(type_str: &str) -> DataType {
    let t = type_str.to_lowercase();

    // Integer types
    if t == "smallint" || t == "int2" {
        DataType::Int16
    } else if t == "integer" || t == "int" || t == "int4" || t == "serial" {
        DataType::Int32
    } else if t == "bigint" || t == "int8" || t == "bigserial" {
        DataType::Int64
    }
    // Floating point types
    else if t == "real" || t == "float4" {
        DataType::Float32
    }
    // Floating point and numeric/decimal types - all map to Float64
    else if t == "double precision"
        || t == "float8"
        || t.starts_with("numeric")
        || t.starts_with("decimal")
    {
        DataType::Float64
    }
    // Boolean
    else if t == "boolean" || t == "bool" {
        DataType::Boolean
    }
    // String types
    else if t == "text"
        || t.starts_with("varchar")
        || t.starts_with("character varying")
        || t.starts_with("char")
        || t.starts_with("character")
        || t == "name"
    {
        DataType::Utf8
    }
    // Date/Time types
    else if t == "date" {
        DataType::Date32
    } else if t.starts_with("timestamp") {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }
    // Fallback to string
    else {
        tracing::warn!("Unknown PostgreSQL type '{}', mapping to Utf8", type_str);
        DataType::Utf8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_compute_context() {
        let executor = PostgresExecutor::new("host=localhost dbname=test".to_string());
        assert_eq!(
            executor.compute_context(),
            Some("host=localhost dbname=test".to_string())
        );
        assert_eq!(executor.name(), "postgres");
    }

    #[test]
    fn test_map_postgres_types() {
        assert_eq!(map_postgres_type("smallint"), DataType::Int16);
        assert_eq!(map_postgres_type("integer"), DataType::Int32);
        assert_eq!(map_postgres_type("bigint"), DataType::Int64);
        assert_eq!(map_postgres_type("real"), DataType::Float32);
        assert_eq!(map_postgres_type("double precision"), DataType::Float64);
        assert_eq!(map_postgres_type("text"), DataType::Utf8);
        assert_eq!(map_postgres_type("varchar(100)"), DataType::Utf8);
        assert_eq!(map_postgres_type("boolean"), DataType::Boolean);
        assert_eq!(map_postgres_type("date"), DataType::Date32);
        assert_eq!(
            map_postgres_type("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }
}
