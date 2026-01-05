use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::FlightService as gRPCFlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Command, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTables, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    SchemaAsIpc, Ticket,
};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use futures::Stream;
use prost::Message;
use strake_core::error::StrakeError;
use strake_core::federation::FederationEngine;
use tonic::{Request, Response, Status, Streaming};

/// Constants for SQL Info (avoid magic numbers in build_sql_info_batch)
const SQL_INFO_SERVER_NAME: u32 = 0;
const SQL_INFO_SERVER_VERSION: u32 = 1;
const SQL_INFO_DRIVER_VERSION: u32 = 2;
const SQL_INFO_FLIGHT_SQL_SERVER_READY: u32 = 3;

// Version constants
const SERVER_VERSION: &str = "1.0.0";
const DRIVER_VERSION: &str = "17.0.0";

/// Type alias to reduce repetition
type FlightResult<T> = Result<Response<T>, Status>;

/// FlightSQL service implementation for Strake.
///
/// Implements the Arrow FlightSQL protocol, enabling high-performance
/// database connectivity via gRPC. Supports:
/// - Statement execution (ad-hoc and prepared)
/// - Metadata queries (catalogs, schemas, tables, type info)
/// - SQL info capabilities reporting
pub struct StrakeFlightSqlService {
    pub engine: Arc<FederationEngine>,
    pub server_name: String,
}

impl StrakeFlightSqlService {
    fn ctx(&self) -> &SessionContext {
        self.engine.context()
    }

    fn get_user<T>(&self, request: &Request<T>) -> Option<strake_core::auth::AuthenticatedUser> {
        request
            .extensions()
            .get::<strake_core::auth::AuthenticatedUser>()
            .cloned()
    }

    /// Convert an anyhow error to a tonic Status with JSON metadata.
    fn to_status_with_metadata(e: anyhow::Error) -> Status {
        let strake_error = StrakeError::from(datafusion::error::DataFusionError::Execution(
            format!("{:#}", e).replace('\n', " || "),
        ));
        let json_error = serde_json::to_string(&strake_error).unwrap_or_default();

        let mut status = Status::internal(strake_error.to_string());
        if let Ok(metadata_value) = tonic::metadata::MetadataValue::try_from(json_error.as_str()) {
            status
                .metadata_mut()
                .insert("x-strake-error-json", metadata_value);
        }
        status
    }

    /// Simpler conversion for planning errors
    fn to_plan_error(e: datafusion::error::DataFusionError) -> Status {
        let strake_error = StrakeError::from(datafusion::error::DataFusionError::Execution(
            format!("{:#}", e).replace('\n', " || "),
        ));
        Status::internal(strake_error.to_string())
    }

    fn catalogs_schema() -> Schema {
        Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)])
    }

    fn schemas_schema() -> Schema {
        Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ])
    }

    fn tables_schema() -> Schema {
        Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_schema", DataType::Binary, false),
        ])
    }

    fn xdbc_type_info_schema() -> Schema {
        Schema::new(vec![
            Field::new("type_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Int32, false),
        ])
    }

    async fn execute_and_stream<T>(
        &self,
        sql: &str,
        request: &Request<T>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        tracing::info!(sql = %sql, "Executing Flight SQL query");

        let user = self.get_user(request);
        let (schema, batches, warnings) = self
            .engine
            .execute_query(sql, user)
            .await
            .map_err(|e| Self::to_status_with_metadata(e))?;

        let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, batches)
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        let stream: Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>> =
            Box::pin(stream);
        let mut response = Response::new(stream);

        for warning in warnings {
            if let Some((k, v)) = warning.split_once(": ") {
                if let (Ok(key), Ok(val)) = (
                    tonic::metadata::MetadataKey::from_str(k),
                    tonic::metadata::MetadataValue::try_from(v),
                ) {
                    response.metadata_mut().append(key, val);
                }
            }
        }

        Ok(response)
    }
}

#[tonic::async_trait]
impl FlightSqlService for StrakeFlightSqlService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: vec![].into(),
        };
        let stream = futures::stream::iter(vec![Ok(result)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let sql = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.execute_and_stream(sql, &request).await
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.ctx();
        let plan: LogicalPlan = ctx
            .state()
            .create_logical_plan(&query.query)
            .await
            .map_err(Self::to_plan_error)?;

        let arrow_schema = plan.schema().as_arrow().as_ref().clone();

        let fd = request.into_inner();
        let ticket = TicketStatementQuery {
            statement_handle: query.query.into(),
        };
        let command = Command::TicketStatementQuery(ticket);
        let ticket_bytes = command.into_any().encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(
                arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes)),
            );

        Ok(Response::new(info))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| Status::invalid_argument(format!("Invalid handle: {}", e)))?;

        let ctx = self.ctx();
        let plan: LogicalPlan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(Self::to_plan_error)?;

        let arrow_schema = plan.schema().as_arrow().as_ref().clone();

        let fd = request.into_inner();
        let command = Command::CommandPreparedStatementQuery(query);
        let ticket_bytes = command.into_any().encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(
                arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes)),
            );

        Ok(Response::new(info))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let sql = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| Status::invalid_argument(format!("Invalid handle: {}", e)))?;

        // Log explicitly for prepared statements as before
        tracing::info!(sql = %sql, "Executing Prepared Flight SQL query");
        self.execute_and_stream(sql, &request).await
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let schema = Self::catalogs_schema();
        let catalogs = StringArray::from(vec![self.engine.catalog_name.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(catalogs)])
            .map_err(|e| Status::internal(e.to_string()))?;

        let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Self::catalogs_schema();
        let fd = request.into_inner();

        let ticket = Command::CommandGetCatalogs(query)
            .into_any()
            .encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket)));

        Ok(Response::new(info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let catalog = query
            .catalog
            .as_deref()
            .unwrap_or(&self.engine.catalog_name);
        let batch = self
            .build_schemas_batch(catalog)
            .map_err(|e| Status::internal(e.to_string()))?;

        let flight_data = arrow_flight::utils::batches_to_flight_data(&batch.schema(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Self::schemas_schema();
        let fd = request.into_inner();

        let ticket = Command::CommandGetDbSchemas(query)
            .into_any()
            .encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket)));

        Ok(Response::new(info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let batch = self
            .build_tables_batch(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let flight_data = arrow_flight::utils::batches_to_flight_data(&batch.schema(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Self::tables_schema();
        let fd = request.into_inner();

        let ticket = Command::CommandGetTables(query).into_any().encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket)));

        Ok(Response::new(info))
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        _message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Fallback not implemented"))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        let union_fields = UnionFields::new(
            vec![0, 1, 2, 3, 4, 5],
            vec![
                Field::new("string_value", DataType::Utf8, false),
                Field::new("bool_value", DataType::Boolean, false),
                Field::new("bigint_value", DataType::Int64, false),
                Field::new("int32_value", DataType::Int32, false),
                Field::new(
                    "string_list",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    false,
                ),
                Field::new("int32_bitmask", DataType::Int32, false),
            ],
        );
        let schema = Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new(
                "value",
                DataType::Union(union_fields, UnionMode::Dense),
                true,
            ),
        ]);
        let ticket = Command::CommandGetSqlInfo(query).into_any().encode_to_vec();
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket)));
        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let batch = self
            .build_sql_info_batch(query.info)
            .map_err(|e| Status::internal(e.to_string()))?;
        let flight_data = arrow_flight::utils::batches_to_flight_data(&batch.schema(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        let schema = Self::xdbc_type_info_schema();
        let ticket = Command::CommandGetXdbcTypeInfo(query)
            .into_any()
            .encode_to_vec();
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(fd)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(Ticket::new(ticket)));
        Ok(Response::new(info))
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as gRPCFlightService>::DoGetStream>, Status> {
        let batch = self
            .build_xdbc_type_info_batch(query.data_type)
            .map_err(|e| Status::internal(e.to_string()))?;
        let flight_data = arrow_flight::utils::batches_to_flight_data(&batch.schema(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = futures::stream::iter(flight_data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let sql = query.query;
        tracing::info!(sql = %sql, "Creating prepared statement");

        // For this simple implementation, the handle is just the SQL string itself
        let dataset_schema = self
            .ctx()
            .state()
            .create_logical_plan(&sql)
            .await
            .map_err(Self::to_plan_error)?
            .schema()
            .as_arrow()
            .as_ref()
            .clone();

        let options = IpcWriteOptions::default();
        let IpcMessage(serialized_schema) = SchemaAsIpc::new(&dataset_schema, &options)
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;

        let result = ActionCreatePreparedStatementResult {
            prepared_statement_handle: sql.into(),
            dataset_schema: serialized_schema,
            parameter_schema: vec![].into(), // No parameters supported yet
        };

        Ok(result)
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        // Stateless, so nothing to close
        Ok(())
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        // This is called by some clients (e.g. DBeaver) even for queries if they think it's an update.
        // Or if the client uses executeUpdate() API.
        // For now, Strake is primarily read-only or doesn't support updates via FlightSQL yet.
        // However, to prevent crashes/errors, we return a success with "unknown" (-1) modified rows
        // or 0 if appropriate.

        let sql = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| Status::invalid_argument(format!("Invalid handle: {}", e)))?;

        tracing::info!(sql = %sql, "Executing Prepared Flight SQL update (no-op/mock)");

        // Technically we could try to execute it if it was an INSERT, but for now we just acknowledge.
        // Since the user is seeing this for "SELECT", it implies the client is confused or probing.
        // We'll return 0 to indicate no rows modified.

        Ok(-1)
    }
}

impl StrakeFlightSqlService {
    fn build_schemas_batch(&self, catalog: &str) -> anyhow::Result<RecordBatch> {
        let mut builder = StringBuilder::new();

        if catalog == self.engine.catalog_name {
            let catalog_provider = self
                .ctx()
                .catalog(&self.engine.catalog_name)
                .ok_or(anyhow::anyhow!("Catalog not found"))?;
            for schema_name in catalog_provider.schema_names() {
                builder.append_value(schema_name);
            }
        }

        let schema = Self::schemas_schema();

        let db_schemas = builder.finish();
        let catalogs = StringArray::from(vec![Some(catalog); db_schemas.len()]);

        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(catalogs), Arc::new(db_schemas)],
        )?)
    }

    async fn build_tables_batch(&self, query: &CommandGetTables) -> anyhow::Result<RecordBatch> {
        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut table_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut table_schema_builder = arrow::array::BinaryBuilder::new();

        let target_catalog = query
            .catalog
            .as_deref()
            .unwrap_or(&self.engine.catalog_name);

        // Match SQL LIKE patterns (% = any, _ = single char)
        let matches_like_pattern = |value: &str, pattern: Option<&String>| -> bool {
            match pattern {
                None => true,
                Some(p) if p == "%" => true,
                Some(p) => {
                    // Convert SQL LIKE to regex: % -> .*, _ -> .
                    let regex_pattern = format!(
                        "^{}$",
                        regex::escape(p).replace(r"\%", ".*").replace(r"\_", ".")
                    );
                    regex::Regex::new(&regex_pattern)
                        .map(|re| re.is_match(value))
                        .unwrap_or(false)
                }
            }
        };

        if matches_like_pattern(&self.engine.catalog_name, Some(&target_catalog.to_string())) {
            let catalog_provider = self
                .ctx()
                .catalog(&self.engine.catalog_name)
                .ok_or(anyhow::anyhow!("Catalog not found"))?;

            for schema_name in catalog_provider.schema_names() {
                if !matches_like_pattern(&schema_name, query.db_schema_filter_pattern.as_ref()) {
                    continue;
                }

                if let Some(schema_provider) = catalog_provider.schema(&schema_name) {
                    for table_name in schema_provider.table_names() {
                        if !matches_like_pattern(
                            &table_name,
                            query.table_name_filter_pattern.as_ref(),
                        ) {
                            continue;
                        }

                        // Filter by table_types if provided
                        if !query.table_types.is_empty()
                            && !query.table_types.contains(&"TABLE".to_string())
                        {
                            continue;
                        }

                        catalog_builder.append_value(&self.engine.catalog_name);
                        schema_builder.append_value(&schema_name);
                        table_builder.append_value(&table_name);
                        type_builder.append_value("TABLE");

                        if query.include_schema {
                            if let Ok(Some(table)) = schema_provider.table(&table_name).await {
                                let schema = table.schema();
                                let options = IpcWriteOptions::default();
                                let data = SchemaAsIpc::new(&schema, &options).try_into();
                                match data {
                                    Ok(IpcMessage(bytes)) => {
                                        table_schema_builder.append_value(bytes)
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to serialize schema for {}.{}: {}",
                                            schema_name,
                                            table_name,
                                            e
                                        );
                                        table_schema_builder.append_value(&[]);
                                    }
                                }
                            } else {
                                table_schema_builder.append_value(&[]);
                            }
                        } else {
                            table_schema_builder.append_value(&[]);
                        }
                    }
                }
            }
        }

        let output_schema = Self::tables_schema();

        Ok(RecordBatch::try_new(
            Arc::new(output_schema),
            vec![
                Arc::new(catalog_builder.finish()),
                Arc::new(schema_builder.finish()),
                Arc::new(table_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(table_schema_builder.finish()),
            ],
        )?)
    }
    fn build_sql_info_batch(&self, info: Vec<u32>) -> anyhow::Result<RecordBatch> {
        let entries = vec![
            (SQL_INFO_SERVER_NAME, self.server_name.as_str()),
            (SQL_INFO_SERVER_VERSION, SERVER_VERSION),
            (SQL_INFO_DRIVER_VERSION, DRIVER_VERSION),
            (SQL_INFO_FLIGHT_SQL_SERVER_READY, "false"),
        ];

        let mut key_builder = arrow::array::UInt32Builder::new();

        // We will build child arrays separately
        let mut type_ids = Vec::<i8>::new();
        let mut offsets = Vec::<i32>::new();

        let mut string_builder = StringBuilder::new();
        let mut bool_builder = arrow::array::BooleanBuilder::new();
        // int64, int32, list, bitmask builders not needed for this minimal set but must exist for schema
        let mut bigint_builder = arrow::array::Int64Builder::new();
        let mut int32_builder = arrow::array::Int32Builder::new();
        // list builder complex, let's make empty list array directly later
        // bitmask
        let mut bitmask_builder = arrow::array::Int32Builder::new();

        for (code, val) in entries {
            if info.is_empty() || info.contains(&code) {
                key_builder.append_value(code);

                if code == SQL_INFO_FLIGHT_SQL_SERVER_READY {
                    // Type 1: Boolean
                    type_ids.push(1);
                    offsets.push(bool_builder.len() as i32);
                    bool_builder.append_value(false); // "false"
                } else {
                    // Type 0: String
                    type_ids.push(0);
                    offsets.push(string_builder.len() as i32);
                    string_builder.append_value(val);
                }
            }
        }
        let type_id_buffer = arrow::buffer::ScalarBuffer::from(type_ids);
        let offset_buffer = arrow::buffer::ScalarBuffer::from(offsets);

        let string_array = Arc::new(string_builder.finish());
        let bool_array = Arc::new(bool_builder.finish());
        let bigint_array = Arc::new(bigint_builder.finish());
        let int32_array = Arc::new(int32_builder.finish());
        let bitmask_array = Arc::new(bitmask_builder.finish());

        // Empty string list array
        let list_item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let string_list_array = Arc::new(arrow::array::ListArray::new(
            list_item_field.clone(),
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0; 1])), // Empty
            Arc::new(StringArray::from(vec![] as Vec<&str>)),
            None,
        ));

        let union_fields = UnionFields::new(
            vec![0, 1, 2, 3, 4, 5],
            vec![
                Field::new("string_value", DataType::Utf8, false),
                Field::new("bool_value", DataType::Boolean, false),
                Field::new("bigint_value", DataType::Int64, false),
                Field::new("int32_value", DataType::Int32, false),
                Field::new(
                    "string_list",
                    DataType::List(list_item_field.clone()),
                    false,
                ),
                Field::new("int32_bitmask", DataType::Int32, false),
            ],
        );

        let children: Vec<Arc<dyn Array>> = vec![
            string_array,
            bool_array,
            bigint_array,
            int32_array,
            string_list_array,
            bitmask_array,
        ];

        let union_array = arrow::array::UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            Some(offset_buffer),
            children,
        )?;

        let schema = Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new(
                "value",
                DataType::Union(union_fields, UnionMode::Dense),
                true,
            ),
        ]);

        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(key_builder.finish()), Arc::new(union_array)],
        )?)
    }

    fn build_xdbc_type_info_batch(&self, _data_type: Option<i32>) -> anyhow::Result<RecordBatch> {
        let type_name_builder = StringArray::from(vec!["INTEGER", "VARCHAR"]);
        let data_type_builder = arrow::array::Int32Array::from(vec![4, 12]);

        let schema = Self::xdbc_type_info_schema();

        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(type_name_builder), Arc::new(data_type_builder)],
        )?)
    }
}
