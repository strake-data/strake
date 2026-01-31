use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use std::sync::Arc;
use strake_common::models::{
    QueryRequest, QueryResponse, SourcesConfig, TableDiscovery, ValidationRequest,
    ValidationResponse,
};
use strake_runtime::federation::FederationEngine;

pub fn create_api_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .merge(create_validation_router(engine.clone()))
        .merge(create_introspection_router(engine.clone()))
        .merge(create_query_router(engine.clone()))
}

pub fn create_query_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .route("/sources", get(list_sources))
        .route("/query", post(execute_query))
        .with_state(engine)
}

pub fn create_validation_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .route("/validate", post(validate_config))
        .with_state(engine)
}

pub fn create_introspection_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .route("/introspect/{domain}/{source}", get(list_tables))
        .route(
            "/introspect/{domain}/{source}/tables",
            post(introspect_tables),
        )
        .with_state(engine)
}

async fn validate_config(
    State(_engine): State<Arc<FederationEngine>>,
    Json(payload): Json<ValidationRequest>,
) -> Json<ValidationResponse> {
    // Basic structural validation
    let mut errors = Vec::new();

    match serde_yaml::from_str::<SourcesConfig>(&payload.sources_yaml) {
        Ok(_config) => {
            // Success - structural
        }
        Err(e) => {
            errors.push(format!("YAML Parsing Error: {}", e));
        }
    }

    // Semantic validation (contracts) would go here or in enterprise override

    Json(ValidationResponse {
        valid: errors.is_empty(),
        errors,
    })
}

async fn list_tables(
    State(engine): State<Arc<FederationEngine>>,
    Path((_domain, source_name)): Path<(String, String)>,
) -> Json<Vec<TableDiscovery>> {
    let mut discovered = Vec::new();

    if let Some(catalog) = engine.context().catalog(&engine.catalog_name) {
        if let Some(schema) = catalog.schema(&source_name) {
            for table_name in schema.table_names() {
                discovered.push(TableDiscovery {
                    name: table_name,
                    schema: source_name.clone(),
                });
            }
        }
    }

    Json(discovered)
}

async fn introspect_tables(
    State(engine): State<Arc<FederationEngine>>,
    Path((domain, source_name)): Path<(String, String)>,
    Json(tables): Json<Vec<String>>,
) -> Json<SourcesConfig> {
    use strake_common::models::{ColumnConfig, SourceConfig, TableConfig};

    let mut config = SourcesConfig {
        domain: Some(domain.clone()),
        sources: vec![],
    };

    // Look up existing source URL in engine if possible
    let engine_source = engine.get_source_config(&source_name);

    let mut source = SourceConfig {
        name: source_name.clone(),
        source_type: engine_source
            .as_ref()
            .map(|s| s.source_type.clone())
            .unwrap_or_else(|| "sql".to_string()),
        url: engine_source.as_ref().and_then(|s| {
            // Internal config stores the connection string in the 'config' field for SQL sources
            s.config
                .get("connection")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        }),
        username: None,
        password: None,
        default_limit: None,
        cache: None,
        tables: vec![],
        config: serde_json::Value::Null,
    };

    for table_full in tables {
        let parts: Vec<&str> = table_full.split('.').collect();
        let (schema, name) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", parts[0])
        };

        // Try to fetch real columns from DataFusion catalog
        let mut columns = vec![];
        // Use the engine's catalog name and SOURCE NAME as the schema (Strake convention)
        let table_ref = format!(
            "\"{}\".\"{}\".\"{}\"",
            engine.catalog_name, source_name, name
        );

        match engine.context().table_provider(table_ref.clone()).await {
            Ok(provider) => {
                let schema = provider.schema();
                for field in schema.fields().iter() {
                    columns.push(ColumnConfig {
                        name: field.name().clone(),
                        data_type: field.data_type().to_string(),
                        length: None, // DataFusion doesn't easily expose this as a simple i32
                        primary_key: false, // Metadata doesn't expose PK directly
                        unique: false,
                        not_null: !field.is_nullable(),
                    });
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch schema for table {}: {}", table_ref, e);
                // Fallback to minimal placeholder if DataFusion lookup fails
                columns.push(ColumnConfig {
                    name: "id".into(),
                    data_type: "integer".into(),
                    length: None,
                    primary_key: true,
                    unique: false,
                    not_null: true,
                });
            }
        }

        source.tables.push(TableConfig {
            name: name.to_string(),
            schema: schema.to_string(),
            partition_column: None,
            columns,
        });
    }

    config.sources.push(source);
    Json(config)
}

async fn list_sources(State(engine): State<Arc<FederationEngine>>) -> Json<SourcesConfig> {
    let sources = engine.list_sources();

    // Map internal config::SourceConfig to models::SourceConfig
    let api_sources = sources
        .into_iter()
        .map(|s| {
            strake_common::models::SourceConfig {
                name: s.name,
                source_type: s.source_type,
                url: s
                    .config
                    .get("connection")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                username: None,
                password: None,
                default_limit: None,
                cache: None,
                tables: vec![], // Details fetched via introspection
                config: serde_json::Value::Null,
            }
        })
        .collect();

    Json(SourcesConfig {
        domain: Some(engine.catalog_name.clone()),
        sources: api_sources,
    })
}

async fn execute_query(
    State(engine): State<Arc<FederationEngine>>,
    Json(payload): Json<QueryRequest>,
) -> Json<QueryResponse> {
    tracing::info!("Executing query: {}", payload.sql);

    match engine.execute_query(&payload.sql, None).await {
        Ok((_schema, batches, _warnings)) => {
            // Convert RecordBatches to JSON using ArrayWriter
            let mut buf = Vec::new();
            {
                let mut writer = arrow_json::ArrayWriter::new(&mut buf);
                if let Err(e) = writer.write_batches(&batches.iter().collect::<Vec<_>>()) {
                    return Json(QueryResponse {
                        status: "error".to_string(),
                        data: None,
                        message: Some(format!("JSON serialization error: {}", e)),
                    });
                }
                if let Err(e) = writer.finish() {
                    return Json(QueryResponse {
                        status: "error".to_string(),
                        data: None,
                        message: Some(format!("JSON writer finish error: {}", e)),
                    });
                }
            }

            match serde_json::from_slice::<serde_json::Value>(&buf) {
                Ok(data) => Json(QueryResponse {
                    status: "success".to_string(),
                    data: Some(data),
                    message: None,
                }),
                Err(e) => Json(QueryResponse {
                    status: "error".to_string(),
                    data: None,
                    message: Some(format!("JSON parse error: {}", e)),
                }),
            }
        }
        Err(e) => Json(QueryResponse {
            status: "error".to_string(),
            data: None,
            message: Some(e.to_string()),
        }),
    }
}
