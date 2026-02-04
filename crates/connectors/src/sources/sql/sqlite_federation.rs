//! SQLite Federation Executor
//!
//! Implements `SQLExecutor` for SQLite to enable same-source join pushdown.

use arrow::array::{ArrayBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider};
use rusqlite::types::ValueRef;
use std::sync::Arc;

/// SQLite Executor
#[derive(Debug, Clone)]
pub struct SqliteExecutor {
    connection_string: String,
}

impl SqliteExecutor {
    pub fn new(connection_string: String) -> Self {
        Self { connection_string }
    }

    pub fn create_federation_provider(self) -> Arc<SQLFederationProvider> {
        Arc::new(SQLFederationProvider::new(Arc::new(self)))
    }
}

#[async_trait]
impl SQLExecutor for SqliteExecutor {
    fn name(&self) -> &str {
        "sqlite"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.connection_string.clone())
    }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        Arc::new(datafusion::sql::unparser::dialect::SqliteDialect {})
    }

    fn logical_optimizer(&self) -> Option<datafusion_federation::sql::LogicalOptimizer> {
        None
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let connection_string = self.connection_string.clone();
        let query = query.to_string();
        let schema_for_task = schema.clone();

        tracing::debug!(target: "federation", db = "[REDACTED]", "SQLite executing federated query: {}", query);

        let batch_stream = futures::stream::once(async move {
            tokio::task::spawn_blocking(move || {
                let conn = rusqlite::Connection::open(&connection_string)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                let mut stmt = conn
                    .prepare(&query)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                let column_count = schema_for_task.fields().len();
                let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(column_count);

                for field in schema_for_task.fields() {
                    match field.data_type() {
                        DataType::Int64 => builders.push(Box::new(Int64Builder::new())),
                        DataType::Float64 => builders.push(Box::new(Float64Builder::new())),
                        DataType::Utf8 | DataType::LargeUtf8 => {
                            builders.push(Box::new(StringBuilder::new()))
                        }
                        dt => {
                            return Err(datafusion::error::DataFusionError::NotImplemented(
                                format!("SQLite federation: Unsupported data type: {:?}", dt),
                            ))
                        }
                    }
                }

                // Let's use simpler rows iterator
                let mut rows = stmt
                    .query([])
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                // Iterate over rows
                while let Some(row) = rows
                    .next()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                {
                    for (i, builder) in builders.iter_mut().enumerate() {
                        let val_ref = row.get_ref(i).map_err(|e| {
                            datafusion::error::DataFusionError::External(Box::new(e))
                        })?;

                        match builder.as_any_mut().downcast_mut::<Int64Builder>() {
                            Some(b) => match val_ref {
                                ValueRef::Integer(v) => b.append_value(v),
                                ValueRef::Null => b.append_null(),
                                _ => {
                                    return Err(datafusion::error::DataFusionError::Execution(
                                        format!("Expected Int64 at col {}", i),
                                    ))
                                }
                            },
                            None => match builder.as_any_mut().downcast_mut::<Float64Builder>() {
                                Some(b) => match val_ref {
                                    ValueRef::Real(v) => b.append_value(v),
                                    ValueRef::Integer(v) => b.append_value(v as f64),
                                    ValueRef::Null => b.append_null(),
                                    _ => {
                                        return Err(datafusion::error::DataFusionError::Execution(
                                            format!("Expected Float64 at col {}", i),
                                        ))
                                    }
                                },
                                None => {
                                    match builder.as_any_mut().downcast_mut::<StringBuilder>() {
                                        Some(b) => {
                                            match val_ref {
                                                ValueRef::Text(v) => {
                                                    let s = std::str::from_utf8(v).map_err(|e| {
                                            datafusion::error::DataFusionError::Execution(format!(
                                                "Invalid UTF-8 at col {}: {}",
                                                i, e
                                            ))
                                        })?;
                                                    b.append_value(s)
                                                }
                                                ValueRef::Null => b.append_null(),
                                                // Auto-cast other types to string if needed?
                                                ValueRef::Integer(v) => {
                                                    b.append_value(v.to_string())
                                                }
                                                ValueRef::Real(v) => b.append_value(v.to_string()),
                                                _ => return Err(
                                                    datafusion::error::DataFusionError::Execution(
                                                        format!("Expected String at col {}", i),
                                                    ),
                                                ),
                                            }
                                        }
                                        None => {
                                            return Err(
                                                datafusion::error::DataFusionError::Internal(
                                                    "Builder type mismatch".to_string(),
                                                ),
                                            )
                                        }
                                    }
                                }
                            },
                        }
                    }
                }

                // Finish builders
                let columns = builders.into_iter().map(|mut b| b.finish()).collect();
                let batch = RecordBatch::try_new(schema_for_task, columns)?;

                Ok(batch)
            })
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Join error: {}", e))
            })?
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batch_stream,
        )))
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        let connection_string = self.connection_string.clone();
        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&connection_string)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows = stmt.query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let names: Vec<String> = rows.filter_map(|r| r.ok()).collect();
            Ok(names)
         })
         .await
         .map_err(|e| datafusion::error::DataFusionError::Execution(format!("Task error: {}", e)))?
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let connection_string = self.connection_string.clone();
        let table_name = table_name.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&connection_string)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn
                .prepare(&format!("PRAGMA table_info('{}')", table_name))
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows = stmt
                .query_map([], |row| {
                    let name: String = row.get("name")?;
                    let type_str: String = row.get("type")?;
                    let notnull: bool = row.get("notnull")?;
                    Ok((name, type_str, notnull))
                })
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut fields = Vec::new();
            for row in rows {
                let (name, type_str, notnull) =
                    row.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                let dt = map_sqlite_type(&type_str);
                fields.push(Field::new(name, dt, !notnull));
            }

            Ok(Arc::new(Schema::new(fields)) as SchemaRef)
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("Task error: {}", e)))?
    }
}

fn map_sqlite_type(type_str: &str) -> DataType {
    let t = type_str.to_uppercase();
    if t.contains("INT") {
        DataType::Int64
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        DataType::Utf8
    } else if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        DataType::Float64
    } else {
        DataType::Utf8 // Fallback
    }
}
