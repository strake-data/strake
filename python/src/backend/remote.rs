use arrow::array::Array;
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
            let mut pretty_columns = Vec::new();

            let output_schema = Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new(
                    "column_name",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                arrow::datatypes::Field::new("data_type", arrow::datatypes::DataType::Utf8, false),
            ]));

            for batch in &batches {
                let schema_col_idx = batch.schema().index_of("table_schema")?;
                let schema_col = batch
                    .column(schema_col_idx)
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .ok_or(anyhow::anyhow!("table_schema not binary"))?;

                for i in 0..batch.num_rows() {
                    if schema_col.is_valid(i) {
                        let bytes = schema_col.value(i);

                        // Bytes from server might be an IPC Message (with prefix) or raw Schema.
                        // Check for CONTINUATION token [0xFF, 0xFF, 0xFF, 0xFF] at start.
                        let slice = if bytes.len() >= 8 && bytes[0..4] == [0xff, 0xff, 0xff, 0xff] {
                            &bytes[8..]
                        } else {
                            bytes
                        };

                        let buffer = arrow::buffer::Buffer::from(slice); // Ensure 64-byte alignment

                        // Try to read as Message first (most likely if it had prefix)
                        // But we need to handle potential parsing errors defensively
                        let schema = if let Ok(msg) = arrow::ipc::root_as_message(buffer.as_slice())
                        {
                            if let Some(s) = msg.header_as_schema() {
                                s
                            } else {
                                // Header is not schema, maybe it's raw schema?
                                arrow::ipc::root_as_schema(buffer.as_slice()).map_err(|e| {
                                    anyhow::anyhow!(
                                        "Invalid IPC schema (message header mismatch): {}",
                                        e
                                    )
                                })?
                            }
                        } else {
                            // Not a message, try as raw schema
                            arrow::ipc::root_as_schema(buffer.as_slice())
                                .map_err(|e| anyhow::anyhow!("Invalid IPC schema (raw): {}", e))?
                        };

                        let arrow_schema = arrow::ipc::convert::fb_to_schema(schema);

                        let mut col_names = Vec::new();
                        let mut col_types = Vec::new();

                        for field in arrow_schema.fields() {
                            col_names.push(field.name().clone());
                            col_types.push(format!("{}", field.data_type()));
                        }

                        let name_array = arrow::array::StringArray::from(col_names);
                        let type_array = arrow::array::StringArray::from(col_types);

                        let rb = RecordBatch::try_new(
                            output_schema.clone(),
                            vec![Arc::new(name_array), Arc::new(type_array)],
                        )?;
                        pretty_columns.push(rb);
                    }
                }
            }
            if pretty_columns.is_empty() {
                return Ok("Table found but no schema information available.".to_string());
            }
            Ok(arrow::util::pretty::pretty_format_batches(&pretty_columns)?.to_string())
        } else {
            Ok(arrow::util::pretty::pretty_format_batches(&batches)?.to_string())
        }
    }
}
