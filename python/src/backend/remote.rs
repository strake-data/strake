use arrow::array::{Array, StringBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetTables;
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

use super::StrakeQueryExecutor;

pub struct RemoteBackend {
    client: FlightSqlServiceClient<Channel>,
}

impl RemoteBackend {
    pub async fn new(dsn: String, api_key: Option<String>) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(dsn.clone())
            .map_err(|e| anyhow::anyhow!("Invalid DSN: {}", e))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", dsn, e))?;

        let mut client = FlightSqlServiceClient::new(channel);

        if let Some(key) = api_key {
            client.set_token(key);
        }

        Ok(Self { client })
    }
}

#[async_trait]
impl StrakeQueryExecutor for RemoteBackend {
    async fn execute(&mut self, query: &str) -> anyhow::Result<(Arc<Schema>, Vec<RecordBatch>)> {
        let info = self.client.execute(query.to_string(), None).await?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let schema = info.clone().try_decode_schema()?;

        for endpoint in info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let mut stream = self.client.do_get(ticket).await?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }
        }
        Ok((Arc::new(schema), batches))
    }

    async fn trace(&mut self, query: &str) -> anyhow::Result<String> {
        let explain_query = format!("EXPLAIN {}", query);
        let info = self.client.execute(explain_query, None).await?;
        let mut explain_batches = Vec::new();
        for endpoint in info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let mut stream = self.client.do_get(ticket).await?;
                while let Some(batch) = stream.next().await {
                    explain_batches.push(batch?);
                }
            }
        }
        let pretty_explain =
            arrow::util::pretty::pretty_format_batches(&explain_batches)?.to_string();
        Ok(pretty_explain)
    }

    async fn describe(&mut self, table_name: Option<String>) -> anyhow::Result<String> {
        let (name_filter, include_schema) = if let Some(name) = table_name {
            (Some(name), true)
        } else {
            (None, false)
        };

        // Get Tables
        let cmd = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: name_filter,
            table_types: vec![],
            include_schema,
        };

        // Part 1: Get FlightInfo via high-level API
        let flight_info = self.client.get_tables(cmd).await?;

        // Part 2: DoGet for each endpoint
        let mut batches: Vec<RecordBatch> = Vec::new();
        for endpoint in flight_info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let mut batch_stream = self.client.do_get(ticket).await?;

                while let Some(batch_res) = batch_stream.next().await {
                    batches.push(batch_res?);
                }
            }
        }

        // Formatting Logic
        if include_schema {
            let output_schema = Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new(
                    "column_name",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                arrow::datatypes::Field::new("data_type", arrow::datatypes::DataType::Utf8, false),
            ]));

            let mut col_names = StringBuilder::new();
            let mut col_types = StringBuilder::new();
            let mut row_count: usize = 0;

            for batch in &batches {
                let schema_col_idx = batch.schema().index_of("table_schema")?;
                let schema_col = batch
                    .column(schema_col_idx)
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .ok_or_else(|| anyhow::anyhow!("table_schema column is not a BinaryArray"))?;

                // Columnar iteration over the binary column
                for bytes in schema_col.iter().flatten() {
                    // SAFETY: IPC framing: first 4 bytes are continuation marker [0xFF, 0xFF, 0xFF, 0xFF]
                    // per Arrow IPC spec. Following 4 bytes are message length (little-endian).
                    let slice = if bytes.len() >= 8 && bytes[0..4] == [0xff, 0xff, 0xff, 0xff] {
                        &bytes[8..]
                    } else {
                        bytes
                    };

                    // Use direct flatbuffer access without intermediate Buffer allocation
                    let fb_schema = if let Ok(msg) = arrow::ipc::root_as_message(slice) {
                        msg.header_as_schema().ok_or_else(|| {
                            anyhow::anyhow!("IPC message does not contain a schema")
                        })?
                    } else {
                        arrow::ipc::root_as_schema(slice).map_err(|e| {
                            anyhow::anyhow!("Failed to parse bytes as direct IPC schema: {}", e)
                        })?
                    };

                    let arrow_schema = arrow::ipc::convert::fb_to_schema(fb_schema);
                    for field in arrow_schema.fields() {
                        col_names.append_value(field.name());
                        col_types.append_value(field.data_type().to_string());
                        row_count += 1;
                    }
                }
            }

            if row_count == 0 {
                return Ok("Table found but no schema information available.".to_string());
            }

            let rb = RecordBatch::try_new(
                output_schema,
                vec![Arc::new(col_names.finish()), Arc::new(col_types.finish())],
            )?;

            Ok(arrow::util::pretty::pretty_format_batches(&[rb])?.to_string())
        } else {
            Ok(arrow::util::pretty::pretty_format_batches(&batches)?.to_string())
        }
    }

    async fn explain_tree(&mut self, query: &str) -> anyhow::Result<String> {
        // For remote backend, we don't have direct access to the execution plan,
        // so we fall back to returning the EXPLAIN output.
        // Full tree visualization requires the physical plan which is only
        // available in embedded mode.
        self.trace(query).await
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        tracing::info!("RemoteBackend shutting down...");
        // Drop the client to close the connection
        Ok(())
    }
}
