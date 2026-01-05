use std::sync::Arc;

use crate::config::{RetrySettings, SourceConfig, TableConfig};
use crate::sources::SourceProvider;
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde::Deserialize;
use tonic::{
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    Status,
};
// use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct GrpcSourceConfig {
    pub url: String,
    pub service: String,
    pub method: String,
    // JSON body to send as request
    #[serde(default)]
    pub request_body: Option<String>,
    #[serde(default)]
    pub descriptor_set: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<ColumnConfig>>,
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String, // e.g. "Utf8", "Int64"
    #[serde(default)]
    pub nullable: bool,
}

pub struct GrpcSourceProvider {
    pub global_retry: RetrySettings,
}

#[async_trait]
impl SourceProvider for GrpcSourceProvider {
    fn type_name(&self) -> &'static str {
        "grpc"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        let grpc_config: GrpcSourceConfig = serde_yaml::from_value(config.config.clone())
            .context("Failed to parse gRPC source configuration")?;

        // Registration Logic
        // 1. Connect using tonic
        // 2. Reflect service/method to get output schema
        // 3. Register GrpcTableProvider

        // let url = grpc_config.url.clone();

        // MVP: Validation that we can parse config
        // In real impl: fetch schema via reflection
        // MVP: Validation that we can parse config
        // In real impl: fetch schema via reflection
        // MVP: Validation that we can parse config
        // In real impl: fetch schema via reflection
        let schema = create_schema_from_config(&grpc_config)?;

        use datafusion::catalog::MemorySchemaProvider;
        let catalog = context
            .catalog(catalog_name)
            .ok_or(anyhow::anyhow!("Catalog not found"))?;

        if catalog.schema("public").is_none() {
            catalog.register_schema("public", Arc::new(MemorySchemaProvider::new()))?;
        }

        let provider = Arc::new(GrpcTableProvider::new(grpc_config, schema));

        context.register_table(
            datafusion::sql::TableReference::full(catalog_name, "public", &*config.name),
            provider,
        )?;

        Ok(())
    }
}

use datafusion::datasource::TableProvider;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::{DisplayAs, PlanProperties};
// use crate::config::GrpcSourceConfig;

#[derive(Debug)]
struct GrpcTableProvider {
    config: GrpcSourceConfig,
    schema: arrow::datatypes::SchemaRef,
}

impl GrpcTableProvider {
    fn new(config: GrpcSourceConfig, schema: arrow::datatypes::SchemaRef) -> Self {
        Self { config, schema }
    }
}

#[async_trait]
impl TableProvider for GrpcTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let exec = GrpcExec::new(
            self.config.clone(),
            self.schema.clone(),
            _projection.cloned(),
            _limit,
        );
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct GrpcExec {
    config: GrpcSourceConfig,
    schema: arrow::datatypes::SchemaRef,
    #[allow(dead_code)] // TODO: Implement projection pushdown
    projection: Option<Vec<usize>>,
    #[allow(dead_code)] // TODO: Implement limit pushdown
    limit: Option<usize>,
    cache: PlanProperties,
}

impl GrpcExec {
    fn new(
        config: GrpcSourceConfig,
        schema: arrow::datatypes::SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = if let Some(proj) = &projection {
            Arc::new(schema.project(proj).unwrap())
        } else {
            schema.clone()
        };

        let cache = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            config,
            schema: projected_schema,
            projection,
            limit,
            cache,
        }
    }
}

impl DisplayAs for GrpcExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "GrpcExec: url={}/{}",
            self.config.url, self.config.method
        )
    }
}

impl ExecutionPlan for GrpcExec {
    fn name(&self) -> &str {
        "GrpcExec"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let config = self.config.clone();
        let schema = self.schema.clone();

        // Return a stream that executes gRPC in background
        let stream = futures::stream::once(async move {
            // 1. Load Descriptor Pool
            let descriptor_path = config.descriptor_set.ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "No descriptor_set provided. Reflection not yet supported.".to_string(),
                )
            })?;

            let bytes = std::fs::read(&descriptor_path).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to read descriptor file: {}",
                    e
                ))
            })?;

            let pool = DescriptorPool::decode(bytes.as_slice()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to decode descriptor pool: {}",
                    e
                ))
            })?;

            // 2. Resolve Service and Method
            let service_name = &config.service;
            let method_name = &config.method;

            let service = pool.get_service_by_name(service_name).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Service '{}' not found in descriptor",
                    service_name
                ))
            })?;

            let method = service
                .methods()
                .find(|m| m.name() == method_name)
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Method '{}' not found in service '{}'",
                        method_name, service_name
                    ))
                })?;

            let input_desc = method.input();
            let output_desc = method.output();

            // 3. Prepare Request
            let mut request_msg = DynamicMessage::new(input_desc.clone());
            if let Some(json_body) = &config.request_body {
                let json_val: serde_json::Value = serde_json::from_str(json_body).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Invalid JSON request body: {}",
                        e
                    ))
                })?;

                // Need to convert JSON to DynamicMessage.
                // prost-reflect has `transcode_key_to_descriptor` helper? No.
                // It has `deserialize` for `DynamicMessage` via serde if `serde` feature is enabled.
                // Currently `strake-core` relies on `prost-reflect`. Let's assume serde integration is active or we manually map.
                // Use serde::de::DeserializeSeed trait implemented by MessageDescriptor
                use serde::de::DeserializeSeed;
                use serde::de::IntoDeserializer;

                let deserializer = json_val.into_deserializer();
                request_msg = input_desc.clone().deserialize(deserializer).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to map JSON to Protobuf message: {}",
                        e
                    ))
                })?;
            }

            // 4. Connect and Execute
            let url = config.url.clone();
            // tonic::transport::Endpoint::from_shared(url)
            let endpoint = tonic::transport::Endpoint::from_shared(url).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Invalid URL: {}", e))
            })?;

            let channel = endpoint.connect().await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Failed to connect: {}", e))
            })?;

            let mut client = tonic::client::Grpc::new(channel);

            let codec = DynamicCodec::new(input_desc, output_desc);
            let path_str = format!("/{}/{}", service_name, method_name);
            let path = path_str
                .parse::<tonic::transport::Uri>()
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Invalid path URI: {}",
                        e
                    ))
                })?
                .into_parts()
                .path_and_query
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Missing path in URI".to_string())
                })?;

            // Assuming Unary for now
            let request = tonic::Request::new(request_msg);

            let response = client.unary(request, path, codec).await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("gRPC call failed: {}", e))
            })?;

            let response_msg = response.into_inner();

            // 5. Convert to Arrow logic: Response (Protobuf) -> JSON -> Arrow
            //
            // TRADEOFF:
            // We serialize the Protobuf DynamicMessage to [JSON] to leverage Arrow's robust
            // `json::Reader` for handling complex nested schemas without a custom converter.
            //
            // PERFORMANCE NOTE:
            // This double-serialization is a bottleneck. A future optimization should implement
            // a direct `DynamicMessage -> Arrow` transcoder to skip the intermediate JSON step.
            let json_val = serde_json::to_value(&response_msg).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to serialize response to JSON: {}",
                    e
                ))
            })?;

            // Treat as single record or list?
            // If response is a list-like wrapper needed...
            let records = vec![json_val]; // Wrap single response object

            // Infer/use schema
            // We use `schema` (the Arrow one).

            // Convert to batch (Reuse logic from REST or similar)
            let json_str = serde_json::to_string(&records)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            let _cursor = std::io::Cursor::new(json_str);

            let json_str = serde_json::to_string(&records)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            let cursor = std::io::Cursor::new(json_str);

            let final_schema = if schema.fields().is_empty() {
                let values = records.iter().map(|v| Ok(v.clone()));
                let inferred = arrow::json::reader::infer_json_schema_from_iterator(values)
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to infer schema: {}",
                            e
                        ))
                    })?;
                Arc::new(inferred)
            } else {
                schema.clone()
            };

            let mut reader = arrow::json::ReaderBuilder::new(final_schema).build(cursor)?;

            let mut batches = Vec::new();
            while let Some(res) = reader.next() {
                batches.push(res?);
            }

            let batch = arrow::compute::concat_batches(&schema, &batches)?;
            Ok(batch)
        });

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            ),
        ))
    }

    fn statistics(&self) -> datafusion::error::Result<datafusion::physical_plan::Statistics> {
        Ok(datafusion::physical_plan::Statistics::new_unknown(
            &self.schema,
        ))
    }
}

// Custom Codec for DynamicMessage
#[derive(Debug, Clone)]
struct DynamicCodec {
    request_desc: MessageDescriptor,
    response_desc: MessageDescriptor,
}

impl DynamicCodec {
    fn new(request_desc: MessageDescriptor, response_desc: MessageDescriptor) -> Self {
        Self {
            request_desc,
            response_desc,
        }
    }
}

impl Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder {
            desc: self.request_desc.clone(),
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            desc: self.response_desc.clone(),
        }
    }
}

struct DynamicEncoder {
    #[allow(dead_code)]
    desc: MessageDescriptor,
}

impl Encoder for DynamicEncoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        use prost::Message;
        item.encode(buf)
            .map_err(|e| Status::internal(format!("Failed to encode: {}", e)))
    }
}

struct DynamicDecoder {
    desc: MessageDescriptor,
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        use prost::Message;
        let mut msg = DynamicMessage::new(self.desc.clone());
        msg.merge(buf)
            .map_err(|e| Status::internal(format!("Failed to decode: {}", e)))?;
        Ok(Some(msg))
    }
}

fn create_schema_from_config(config: &GrpcSourceConfig) -> Result<arrow::datatypes::SchemaRef> {
    if let Some(cols) = &config.columns {
        let fields: Result<Vec<arrow::datatypes::Field>, _> = cols
            .iter()
            .map(|c| {
                let dt = match c.data_type.as_str() {
                    "Utf8" => arrow::datatypes::DataType::Utf8,
                    "Int64" => arrow::datatypes::DataType::Int64,
                    "Float64" => arrow::datatypes::DataType::Float64,
                    "Boolean" => arrow::datatypes::DataType::Boolean,
                    _ => return Err(anyhow::anyhow!("Unsupported data type: {}", c.data_type)),
                };
                Ok(arrow::datatypes::Field::new(&c.name, dt, c.nullable))
            })
            .collect();
        Ok(Arc::new(arrow::datatypes::Schema::new(fields?)))
    } else {
        Ok(Arc::new(arrow::datatypes::Schema::empty()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parsing_full() {
        let yaml = r#"
            url: "http://localhost:50051"
            service: "my.package.Service"
            method: "GetData"
            descriptor_set: "/path/to/descriptor.bin"
            request_body: '{"id": 1}'
            columns:
              - name: "id"
                type: "Int64"
                nullable: false
              - name: "name"
                type: "Utf8"
        "#;

        let config: GrpcSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.url, "http://localhost:50051");
        assert_eq!(config.service, "my.package.Service");
        assert_eq!(config.method, "GetData");
        assert_eq!(
            config.descriptor_set,
            Some("/path/to/descriptor.bin".to_string())
        );
        assert_eq!(config.request_body, Some("{\"id\": 1}".to_string()));

        let cols = config.columns.as_ref().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "Int64");
        assert_eq!(cols[0].nullable, false);
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].nullable, false); // Default is false? No, struct bool default is false.
    }

    #[test]
    fn test_schema_creation() {
        let config = GrpcSourceConfig {
            url: "http://localhost".to_string(),
            service: "S".to_string(),
            method: "M".to_string(),
            request_body: None,
            descriptor_set: None,
            columns: Some(vec![
                ColumnConfig {
                    name: "col1".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                },
                ColumnConfig {
                    name: "col2".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                },
            ]),
            tables: None,
        };

        let schema = create_schema_from_config(&config).expect("Schema creation failed");
        assert_eq!(schema.fields().len(), 2);

        let f1 = schema.field(0);
        assert_eq!(f1.name(), "col1");
        assert_eq!(f1.data_type(), &arrow::datatypes::DataType::Utf8);
        assert!(f1.is_nullable());

        let f2 = schema.field(1);
        assert_eq!(f2.name(), "col2");
        assert_eq!(f2.data_type(), &arrow::datatypes::DataType::Int64);
        assert!(!f2.is_nullable());
    }

    #[test]
    fn test_schema_creation_empty() {
        let config = GrpcSourceConfig {
            url: "http://localhost".to_string(),
            service: "S".to_string(),
            method: "M".to_string(),
            request_body: None,
            descriptor_set: None,
            columns: None,
            tables: None,
        };

        let schema = create_schema_from_config(&config).expect("Schema creation failed");
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn test_invalid_type() {
        let config = GrpcSourceConfig {
            url: "http://localhost".to_string(),
            service: "S".to_string(),
            method: "M".to_string(),
            request_body: None,
            descriptor_set: None,
            columns: Some(vec![ColumnConfig {
                name: "col1".to_string(),
                data_type: "UnknownType".to_string(),
                nullable: true,
            }]),
            tables: None,
        };

        assert!(create_schema_from_config(&config).is_err());
    }
}
