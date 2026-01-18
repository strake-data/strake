//! REST API data source.
//!
//! Fetches data from HTTP JSON APIs, with support for pagination, authentication (Basic, Bearer, OAuth),
//! and JMESPath-based data extraction.
use crate::sources::SourceProvider;
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use strake_common::config::{RetrySettings, SourceConfig, TableConfig};

#[derive(Debug, Deserialize, Clone)]
pub struct RestSourceConfig {
    pub base_url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub pagination: Option<PaginationConfig>,
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>,
    #[serde(default)]
    pub pushdown: Option<Vec<PushdownConfig>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PushdownConfig {
    pub column: String,
    pub operator: String, // "=", ">", "<", ">=", "<="
    pub param: String,
}

fn default_method() -> String {
    "GET".to_string()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AuthConfig {
    Basic {
        username: String,
        password: Option<String>,
    },
    Bearer {
        token: String,
    },
    #[serde(rename = "oauth_client_credentials")]
    OAuthClientCredentials {
        client_id: String,
        client_secret: String,
        token_url: String,
        #[serde(default)]
        scopes: Vec<String>,
    },
    /// Self-signed JWT for service account authentication (Google, GitHub Apps).
    #[serde(rename = "jwt_assertion")]
    JwtAssertion {
        issuer: String,
        audience: String,
        private_key_pem: String,
        #[serde(default = "default_jwt_algorithm")]
        algorithm: String,
        #[serde(default = "default_jwt_expiry")]
        expiry_secs: u64,
        #[serde(default)]
        subject: Option<String>,
        #[serde(default)]
        claims: std::collections::HashMap<String, serde_json::Value>,
    },
}

fn default_jwt_algorithm() -> String {
    "RS256".to_string()
}

fn default_jwt_expiry() -> u64 {
    3600
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PaginationConfig {
    /// Next URL provided in a response header (e.g. 'Link')
    Header {
        header_name: String,
        // Regex to extract URL? explicit for Link header parsing?
        // simple implementation assumes header value IS the url for now or standard Link header
    },
    /// Next URL found in the JSON response body at path
    BodyUrl {
        path: String, // e.g. "meta.next_url"
    },
    /// Continuation token found in response, injected into next request param
    Token {
        token_path: String, // Path in response json
        param_name: String, // Parameter name in next request
    },
    /// Offset/Limit calculation
    Indices {
        param_offset: String,
        param_limit: String,
        limit: usize,
        #[serde(default)]
        initial_offset: usize,
    },
}

pub struct RestSourceProvider {
    pub global_retry: RetrySettings,
}

#[async_trait]
impl SourceProvider for RestSourceProvider {
    fn type_name(&self) -> &'static str {
        "rest"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        // ... existing config parsing ...
        let rest_config: RestSourceConfig = serde_yaml::from_value(config.config.clone())
            .context("Failed to parse REST source configuration")?;

        let mut client_builder = reqwest::Client::builder();

        // Apply authentication configuration
        if let Some(auth) = &rest_config.auth {
            let mut headers = reqwest::header::HeaderMap::new();
            use reqwest::header::HeaderValue;

            match auth {
                AuthConfig::Basic { username, password } => {
                    // Create Basic auth header
                    let credentials = match password {
                        Some(pwd) => format!("{}:{}", username, pwd),
                        None => username.clone(),
                    };
                    let encoded = base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        credentials.as_bytes(),
                    );
                    if let Ok(val) = HeaderValue::from_str(&format!("Basic {}", encoded)) {
                        headers.insert("Authorization", val);
                    }
                }
                AuthConfig::Bearer { token } => {
                    if let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", token)) {
                        headers.insert("Authorization", val);
                    }
                }
                AuthConfig::OAuthClientCredentials {
                    client_id,
                    client_secret,
                    token_url,
                    scopes,
                } => {
                    // Fetch OAuth token with caching
                    let token = crate::sources::rest_auth::fetch_oauth_token(
                        client_id,
                        client_secret,
                        token_url,
                        scopes,
                    )
                    .await
                    .context("Failed to fetch OAuth access token")?;

                    if let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", token)) {
                        headers.insert("Authorization", val);
                    }
                }
                AuthConfig::JwtAssertion {
                    issuer,
                    audience,
                    private_key_pem,
                    algorithm,
                    expiry_secs,
                    subject,
                    claims,
                } => {
                    // Generate self-signed JWT assertion
                    let jwt_config = crate::sources::rest_auth::JwtAssertionConfig {
                        issuer: issuer.clone(),
                        audience: audience.clone(),
                        private_key_pem: private_key_pem.clone(),
                        algorithm: algorithm.clone(),
                        expiry_secs: *expiry_secs,
                        subject: subject.clone(),
                        claims: claims.clone(),
                    };
                    let token = crate::sources::rest_auth::generate_jwt_assertion(&jwt_config)
                        .context("Failed to generate JWT assertion")?;

                    if let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", token)) {
                        headers.insert("Authorization", val);
                    }
                }
            }

            if !headers.is_empty() {
                client_builder = client_builder.default_headers(headers);
            }
        }

        let client = client_builder
            .build()
            .context("Failed to build HTTP client")?;

        // Simple registration for now - assuming 1 table matching the endpoint
        // TODO: Support multiple tables if list provided

        let schema = infer_schema(&client, &rest_config).await?;

        // Create Provider
        use datafusion::catalog::MemorySchemaProvider;
        let catalog = context
            .catalog(catalog_name)
            .ok_or(anyhow::anyhow!("Catalog not found"))?;

        if catalog.schema("public").is_none() {
            catalog.register_schema("public", Arc::new(MemorySchemaProvider::new()))?;
        }

        let provider = Arc::new(RestTableProvider::new(
            Arc::new(client),
            rest_config,
            schema,
        ));

        // Assuming single table "api_response" if not named in config
        // In real usage, this should come from config.name or explicit table list
        context.register_table(
            datafusion::sql::TableReference::full(catalog_name, "public", &*config.name),
            provider,
        )?;

        Ok(())
    }
}

use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;

#[derive(Debug)]
struct RestTableProvider {
    client: Arc<reqwest::Client>,
    config: RestSourceConfig,
    schema: arrow::datatypes::SchemaRef,
}

impl RestTableProvider {
    fn new(
        client: Arc<reqwest::Client>,
        config: RestSourceConfig,
        schema: arrow::datatypes::SchemaRef,
    ) -> Self {
        Self {
            client,
            config,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for RestTableProvider {
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
        // Return a custom ExecutionPlan that iterates pages
        Ok(Arc::new(RestExec::new(
            self.client.clone(),
            self.config.clone(),
            self.schema.clone(),
            _projection.cloned(),
            _limit,
            _filters.to_vec(),
        )))
    }
}

use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{DisplayAs, PlanProperties};

#[derive(Debug)]
struct RestExec {
    client: Arc<reqwest::Client>,
    config: RestSourceConfig,
    schema: arrow::datatypes::SchemaRef,
    #[allow(dead_code)]
    projection: Option<Vec<usize>>,
    #[allow(dead_code)]
    #[allow(dead_code)]
    limit: Option<usize>,
    filters: Vec<datafusion::logical_expr::Expr>,
    cache: PlanProperties,
}

impl RestExec {
    fn new(
        client: Arc<reqwest::Client>,
        config: RestSourceConfig,
        schema: arrow::datatypes::SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        filters: Vec<datafusion::logical_expr::Expr>,
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
            client,
            config,
            schema: projected_schema,
            projection,
            limit,
            filters,
            cache,
        }
    }
}

impl DisplayAs for RestExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RestExec: url={}", self.config.base_url)
    }
}

impl ExecutionPlan for RestExec {
    fn name(&self) -> &str {
        "RestExec"
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
        // This is where real pagination logic happens
        // Ideally returns a stream that yields RecordBatches page by page

        let client = self.client.clone();
        let config = self.config.clone();
        let schema = self.schema.clone();
        let base_url = config.base_url.clone();
        let filters = self.filters.clone();

        // Use try_unfold to create an async stream
        let stream = futures::stream::try_unfold(
            (client, config, schema, Some(base_url), 0, 0, filters),
            move |(client, config, schema, next_url, current_offset, current_page, filters)| async move {
                if let Some(mut url_str) = next_url {
                    // FIRST PAGE OVERRIDE: Apply pushdowns if not already present
                    if current_page == 0 {
                        if let Some(pushdowns) = &config.pushdown {
                            if let Ok(mut url_obj) = url::Url::parse(&url_str) {
                                for filter in &filters {
                                    if let datafusion::logical_expr::Expr::BinaryExpr(
                                        datafusion::logical_expr::BinaryExpr { left, op, right },
                                    ) = filter
                                    {
                                        if let (
                                            datafusion::logical_expr::Expr::Column(col),
                                            datafusion::logical_expr::Expr::Literal(val, _),
                                        ) = (left.as_ref(), right.as_ref())
                                        {
                                            let op_str = match op {
                                                datafusion::logical_expr::Operator::Eq => "=",
                                                datafusion::logical_expr::Operator::Gt => ">",
                                                datafusion::logical_expr::Operator::Lt => "<",
                                                datafusion::logical_expr::Operator::GtEq => ">=",
                                                datafusion::logical_expr::Operator::LtEq => "<=",
                                                _ => continue,
                                            };

                                            for p in pushdowns {
                                                if p.column == col.name && p.operator == op_str {
                                                    let val_str = match val {
                                                         datafusion::scalar::ScalarValue::Utf8(Some(s)) => s.clone(),
                                                         datafusion::scalar::ScalarValue::Int64(Some(i)) => i.to_string(),
                                                         datafusion::scalar::ScalarValue::Float64(Some(f)) => f.to_string(),
                                                         datafusion::scalar::ScalarValue::Boolean(Some(b)) => b.to_string(),
                                                         _ => val.to_string(), // Fallback
                                                     };
                                                    url_obj
                                                        .query_pairs_mut()
                                                        .append_pair(&p.param, &val_str);
                                                }
                                            }
                                        }
                                    }
                                }
                                url_str = url_obj.to_string();
                            }
                        }
                    }

                    if current_page >= 50 {
                        // Safety break
                        tracing::warn!("Hit max pages limit (50) for REST source");
                        return Ok(None);
                    }

                    let (batch, next_url_opt, new_offset) = fetch_page(
                        &client,
                        &config,
                        &schema,
                        &url_str,
                        current_offset,
                        current_page,
                    )
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                    Ok(Some((
                        batch,
                        (
                            client,
                            config,
                            schema,
                            next_url_opt,
                            new_offset,
                            current_page + 1,
                            filters,
                        ),
                    )))
                } else {
                    Ok(None)
                }
            },
        );

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

async fn infer_schema(
    client: &reqwest::Client,
    config: &RestSourceConfig,
) -> Result<arrow::datatypes::SchemaRef> {
    // 1. Fetch first page
    let mut req = client.request(
        config.method.parse().unwrap_or(reqwest::Method::GET),
        &config.base_url,
    );

    for (k, v) in &config.headers {
        req = req.header(k, v);
    }

    let resp = req
        .send()
        .await
        .context("Failed to fetch API for schema inference")?;
    let json: serde_json::Value = resp.json().await.context("Failed to parse JSON")?;

    // 2. Identify Root
    // For now simple root inference: array or object?
    let records = match json {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Object(_) => vec![json],
        _ => anyhow::bail!("Unsupported JSON root type"),
    };

    // 3. Infer from records
    if records.is_empty() {
        anyhow::bail!("No records found to infer schema");
    }

    // Use arrow-json infer

    use arrow::datatypes::{DataType, Field, Schema};

    let first = &records[0];
    let mut fields = Vec::new();

    if let serde_json::Value::Object(map) = first {
        for (k, v) in map {
            let dt = match v {
                serde_json::Value::String(_) => DataType::Utf8,
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                }
                serde_json::Value::Bool(_) => DataType::Boolean,
                serde_json::Value::Array(_) => DataType::Utf8, // Flatten or list?
                serde_json::Value::Object(_) => DataType::Utf8, // Nested?
                serde_json::Value::Null => DataType::Utf8,
            };
            fields.push(Field::new(k, dt, true));
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}

// Single page fetch
async fn fetch_page(
    client: &reqwest::Client,
    config: &RestSourceConfig,
    schema: &arrow::datatypes::SchemaRef,
    url: &str,
    current_offset: usize,
    _current_page: usize,
) -> Result<(arrow::record_batch::RecordBatch, Option<String>, usize)> {
    // Prepare request
    let mut req = client.request(config.method.parse().unwrap_or(reqwest::Method::GET), url);
    for (k, v) in &config.headers {
        req = req.header(k, v);
    }

    let mut next_url = None;
    let mut new_offset = current_offset;
    // Handle Indices param injection
    if let Some(PaginationConfig::Indices {
        param_offset,
        param_limit,
        limit: l,
        ..
    }) = &config.pagination
    {
        let mut url_obj = url::Url::parse(url)?;
        url_obj
            .query_pairs_mut()
            .append_pair(param_offset, &current_offset.to_string())
            .append_pair(param_limit, &l.to_string());

        req = client.request(
            config.method.parse().unwrap_or(reqwest::Method::GET),
            url_obj.clone(),
        );
        for (k, v) in &config.headers {
            req = req.header(k, v);
        }

        new_offset += l;
        next_url = Some(config.base_url.clone()); // Loop base (Indices calculates new params next time)
    }

    let resp = req.send().await.context("Failed to fetch API")?;

    // Header Pagination
    if let Some(PaginationConfig::Header { header_name }) = &config.pagination {
        if let Some(val) = resp.headers().get(header_name) {
            if let Ok(val_str) = val.to_str() {
                // Handle standard Link header format: <url>; rel="next"
                let next_url_candidate = if val_str.starts_with('<') {
                    val_str.split('>').next().map(|s| s.trim_start_matches('<'))
                } else {
                    Some(val_str)
                };

                if let Some(candidate) = next_url_candidate {
                    if candidate.starts_with("http") {
                        next_url = Some(candidate.to_string());
                    }
                }
            }
        }
    }

    let json: serde_json::Value = resp.json().await.context("Failed to parse JSON")?;

    let records = match &json {
        serde_json::Value::Array(arr) => arr.clone(),
        serde_json::Value::Object(_) => vec![json.clone()],
        _ => vec![],
    };

    if records.is_empty() {
        return Ok((
            arrow::record_batch::RecordBatch::new_empty(schema.clone()),
            None,
            new_offset,
        ));
    }
    if records.is_empty() {
        return Ok((
            arrow::record_batch::RecordBatch::new_empty(schema.clone()),
            None,
            new_offset,
        ));
    }
    tracing::debug!(
        "RestExec: Sent request to {}, got {} records",
        url,
        records.len()
    );

    // Body Pagination
    if let Some(PaginationConfig::BodyUrl { path }) = &config.pagination {
        if let serde_json::Value::Object(map) = &json {
            if let Some(serde_json::Value::String(next)) = map.get(path) {
                next_url = Some(next.clone());
            }
        }
    }

    // Serialize to NDJSON (each record on new line) for Arrow JSON Reader
    let json_vals: Result<Vec<String>, _> = records.iter().map(serde_json::to_string).collect();
    let json_str = json_vals?.join("\n");
    let cursor = std::io::Cursor::new(json_str);
    let reader = arrow::json::ReaderBuilder::new(schema.clone()).build(cursor)?;

    let mut batches = Vec::new();
    for batch in reader {
        match batch {
            Ok(b) => batches.push(b),
            Err(e) => {
                tracing::error!("RestExec: Reader error: {}", e);
                return Err(anyhow::anyhow!("Arrow Reader error: {}", e));
            }
        }
    }
    tracing::debug!("RestExec: Reader produced {} batches", batches.len());

    let batch =
        arrow::compute::concat_batches(schema, &batches).context("Concat batches failed")?;
    tracing::debug!("RestExec: Concat success, rows: {}", batch.num_rows());

    Ok((batch, next_url, new_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_infer_schema() {
        let mock_server: MockServer = MockServer::start().await;

        let response_body = serde_json::json!([
            { "id": 1, "name": "test", "active": true },
            { "id": 2, "name": "test2", "active": false }
        ]);

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
            .mount(&mock_server)
            .await;

        let config = RestSourceConfig {
            base_url: mock_server.uri(),
            method: "GET".to_string(),
            headers: HashMap::new(),
            auth: None,
            pagination: None,
            encoding: None,
            tables: None,
            pushdown: None,
        };

        let client = reqwest::Client::new();
        let schema = infer_schema(&client, &config)
            .await
            .expect("Infer schema failed");

        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("active").is_ok());
    }

    #[tokio::test]
    async fn test_fetch_page_pagination_header() {
        let mock_server: MockServer = MockServer::start().await;
        let next_url = format!("{}/page2", mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([{"id": 1}]))
                    .insert_header("Link", &next_url),
            )
            .mount(&mock_server)
            .await;

        let config = RestSourceConfig {
            base_url: mock_server.uri(),
            method: "GET".to_string(),
            headers: HashMap::new(),
            auth: None,
            pagination: Some(PaginationConfig::Header {
                header_name: "Link".to_string(),
            }),
            encoding: None,
            tables: None,
            pushdown: None,
        };

        let client = reqwest::Client::new();
        // pre-inferred schema
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
        ]));

        let (batch, next, _) = fetch_page(&client, &config, &schema, &mock_server.uri(), 0, 0)
            .await
            .expect("Fetch failed");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(next, Some(next_url));
    }

    #[tokio::test]
    async fn test_fetch_page_pagination_body() {
        let mock_server: MockServer = MockServer::start().await;
        let next_url = format!("{}/page2", mock_server.uri());

        let body = serde_json::json!({
            "data": [{"id": 1}],
            "next": next_url
        });

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&mock_server)
            .await;

        let config = RestSourceConfig {
            base_url: mock_server.uri(),
            method: "GET".to_string(),
            headers: HashMap::new(),
            auth: None,
            pagination: Some(PaginationConfig::BodyUrl {
                path: "next".to_string(),
            }),
            encoding: None,
            tables: None,
            pushdown: None,
        };

        // Limitation: Current implementation treats a root JSON object as a single record.
        // It does not yet support "Tuple Root" to flatten a nested array (e.g. "data": [...]).
        // Therefore, we verify that we can read the "next" field from the wrapper object itself.
        let client = reqwest::Client::new();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("next", arrow::datatypes::DataType::Utf8, true),
        ]));

        let (batch, next, _) = fetch_page(&client, &config, &schema, &mock_server.uri(), 0, 0)
            .await
            .expect("Fetch failed");

        assert_eq!(next, Some(next_url));
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_pushdown_filter() {
        let mock_server: MockServer = MockServer::start().await;

        // Expect a request with query param id=123
        Mock::given(method("GET"))
            .and(path("/"))
            .and(wiremock::matchers::query_param("id_param", "123"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{"id": 123}])),
            )
            .mount(&mock_server)
            .await;

        let config = RestSourceConfig {
            base_url: mock_server.uri(),
            method: "GET".to_string(),
            headers: HashMap::new(),
            auth: None,
            pagination: None,
            encoding: None,
            tables: None,
            pushdown: Some(vec![PushdownConfig {
                column: "id".to_string(),
                operator: "=".to_string(),
                param: "id_param".to_string(),
            }]),
        };

        let client = Arc::new(reqwest::Client::new());
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
        ]));

        // Construct filter: id = 123
        use datafusion::logical_expr::{col, lit};
        let filter = col("id").eq(lit(123i64));

        let exec = RestExec::new(client, config, schema, None, None, vec![filter]);

        let stream = exec
            .execute(0, Arc::new(datafusion::execution::TaskContext::default()))
            .expect("Execution failed");

        let batches: Vec<_> = futures::StreamExt::collect(stream).await;
        // Check results
        assert!(!batches.is_empty());
        match &batches[0] {
            Ok(batch) => assert_eq!(batch.num_rows(), 1),
            Err(e) => panic!("Batch error: {}", e),
        }
    }
}
