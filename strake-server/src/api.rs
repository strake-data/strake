use axum::{routing::{get, post}, Router, Json, extract::{Path, State}};
use std::sync::Arc;
use strake_core::federation::FederationEngine;
use strake_core::models::{SourcesConfig, ValidationRequest, ValidationResponse, TableDiscovery};

pub fn create_api_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .merge(create_validation_router(engine.clone()))
        .merge(create_introspection_router(engine.clone()))
}

pub fn create_validation_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .route("/validate", post(validate_config))
        .with_state(engine)
}

pub fn create_introspection_router(engine: Arc<FederationEngine>) -> Router {
    Router::new()
        .route("/introspect/{domain}/{source}", get(list_tables))
        .route("/introspect/{domain}/{source}/tables", post(introspect_tables))
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
    State(_engine): State<Arc<FederationEngine>>,
    Path((_domain, _source_name)): Path<(String, String)>,
) -> Json<Vec<TableDiscovery>> {
    // For now, return a placeholder or real discovery from engine if possible
    // Discovering tables requires connecting to the source.
    // FederationEngine might already have some sources registered.
    
    let mut discovered = Vec::new();
    
    // logic to get tables from engine...
    // Simplified:
    discovered.push(TableDiscovery { name: "users".into(), schema: "public".into() });
    
    Json(discovered)
}

async fn introspect_tables(
    State(engine): State<Arc<FederationEngine>>,
    Path((domain, source_name)): Path<(String, String)>,
    Json(tables): Json<Vec<String>>,
) -> Json<SourcesConfig> {
    use strake_core::models::{SourceConfig, TableConfig, ColumnConfig};
    
    let mut config = SourcesConfig { domain: Some(domain.clone()), sources: vec![] };
    
    // Look up existing source URL in engine if possible
    let engine_source = engine.get_source_config(&source_name);
    
    let mut source = SourceConfig {
        name: source_name.clone(),
        source_type: engine_source.as_ref().map(|s| s.r#type.clone()).unwrap_or_else(|| "sql".to_string()),
        url: engine_source.as_ref().and_then(|s| {
            // Internal config stores the connection string in the 'config' field for SQL sources
            s.config.get("connection").and_then(|v| v.as_str()).map(|s| s.to_string())
        }),
        username: None,
        password: None,
        tables: vec![],
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
        let table_ref = format!("\"{}\".\"{}\".\"{}\"", engine.catalog_name, source_name, name);
        
        match engine.context().table_provider(table_ref.clone()).await {
            Ok(provider) => {
                let schema = provider.schema();
                for (idx, field) in schema.fields().iter().enumerate() {
                    columns.push(ColumnConfig {
                        name: field.name().clone(),
                        data_type: field.data_type().to_string(),
                        length: None, // DataFusion doesn't easily expose this as a simple i32
                        primary_key: false, // Metadata doesn't expose PK directly
                        unique: false,
                        is_not_null: !field.is_nullable(),
                    });
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch schema for table {}: {}", table_ref, e);
                // Fallback to minimal placeholder if DataFusion lookup fails
                columns.push(ColumnConfig { name: "id".into(), data_type: "integer".into(), length: None, primary_key: true, unique: false, is_not_null: true });
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
