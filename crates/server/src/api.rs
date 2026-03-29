//! # Strake REST API
#![allow(clippy::field_reassign_with_default)]
//!
//! Implementation of the Strake control plane and query API via Axum.
//!
//! ## Overview
//!
//! Handles authentication, source introspection, and query execution
//! via a set of modular Axum routers.
//!
//! ## Usage
//!
//! Generally managed via the `strake-server` binary which mounts these routes.
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use chrono::Utc;
use std::sync::Arc;
use strake_common::models::{
    QueryRequest, QueryResponse, SourceName, SourceType, SourcesConfig, TableDiscovery,
    ValidationRequest, ValidationResponse,
};
use strake_runtime::federation::FederationEngine;

use crate::license::{LicenseCache, LicenseState};

#[derive(Clone)]
pub struct QueryState {
    pub engine: Arc<FederationEngine>,
    pub license_cache: Arc<LicenseCache>,
}

pub fn create_api_router(
    engine: Arc<FederationEngine>,
    license_cache: Arc<LicenseCache>,
) -> Router {
    Router::new()
        .merge(create_validation_router(engine.clone()))
        .merge(create_introspection_router(engine.clone()))
        .merge(create_query_router(engine, license_cache))
}

pub fn create_query_router(
    engine: Arc<FederationEngine>,
    license_cache: Arc<LicenseCache>,
) -> Router {
    let state = Arc::new(QueryState {
        engine,
        license_cache,
    });

    Router::new()
        .route("/sources", get(list_sources))
        .route("/query", post(execute_query))
        .with_state(state)
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

    let mut resp = ValidationResponse::default();
    resp.valid = errors.is_empty();
    resp.errors = errors;
    Json(resp)
}

async fn list_tables(
    State(engine): State<Arc<FederationEngine>>,
    Path((_domain, source_name)): Path<(String, String)>,
) -> Json<Vec<TableDiscovery>> {
    let mut discovered = Vec::new();

    if let Some(catalog) = engine.context().catalog(&engine.catalog_name)
        && let Some(schema) = catalog.schema(&source_name)
    {
        for table_name in schema.table_names() {
            let mut td = TableDiscovery::default();
            td.name = table_name;
            td.schema = source_name.clone();
            discovered.push(td);
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

    let mut config = SourcesConfig::default();
    config.domain = Some(domain.into());
    config.sources = vec![];

    // Look up existing source URL in engine if possible
    let source_name_newtype = SourceName::from(source_name.clone());
    let engine_source = engine.get_source_config(&source_name_newtype);

    let mut source = SourceConfig::default();
    source.name = source_name.clone().into();
    source.source_type = engine_source
        .as_ref()
        .map(|s| s.source_type.clone())
        .unwrap_or_else(|| SourceType::Other("sql".to_string()));
    source.url = engine_source.as_ref().and_then(|s| {
        // Internal config stores the connection string in the 'config' field for SQL sources
        s.config
            .get("connection")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    });
    source.config = serde_json::Value::Null;

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
                    let mut col = ColumnConfig::default();
                    col.name = field.name().clone();
                    col.data_type = field.data_type().to_string();
                    col.not_null = !field.is_nullable();
                    columns.push(col);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch schema for table {}: {}", table_ref, e);
                // Fallback to minimal placeholder if DataFusion lookup fails
                let mut col = ColumnConfig::default();
                col.name = "id".into();
                col.data_type = "integer".into();
                col.primary_key = true;
                col.not_null = true;
                columns.push(col);
            }
        }

        let mut t_cfg = TableConfig::default();
        t_cfg.name = name.to_string();
        t_cfg.schema = schema.to_string();
        t_cfg.column_definitions = columns;
        source.tables.push(t_cfg);
    }

    config.sources.push(source);
    Json(config)
}

async fn list_sources(State(state): State<Arc<QueryState>>) -> Json<SourcesConfig> {
    let sources = state.engine.list_sources();

    // Map internal config::SourceConfig to models::SourceConfig
    let api_sources = sources
        .into_iter()
        .map(|s| {
            let mut sc = strake_common::models::SourceConfig::default();
            sc.name = s.name;
            sc.source_type = s.source_type;
            sc.url = s
                .config
                .get("connection")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            sc.config = serde_json::Value::Null;
            sc
        })
        .collect();

    let mut sc = SourcesConfig::default();
    sc.domain = Some(state.engine.catalog_name.clone().into());
    sc.sources = api_sources;
    Json(sc)
}

async fn execute_query(
    State(state): State<Arc<QueryState>>,
    Extension(user): Extension<strake_common::auth::AuthenticatedUser>,
    Json(payload): Json<QueryRequest>,
) -> Json<QueryResponse> {
    // License Check
    if state.license_cache.current_state() == LicenseState::Invalid {
        let mut resp = QueryResponse::default();
        resp.status = "error".to_string();
        resp.message = Some("License invalid. Please renew subscription.".to_string());
        resp.data = None;
        return Json(resp);
    }

    let user_id = user.id.clone();
    let scrubbed_sql = strake_common::scrubber::scrub(&payload.sql);

    tracing::info!(
        target: "audit",
        event = "rest_query",
        user_id = %user_id,
        sql = %scrubbed_sql,
        timestamp = %Utc::now().to_rfc3339(),
    );

    match state.engine.execute_query(&payload.sql, Some(user)).await {
        Ok((_schema, batches, _warnings)) => {
            // Convert RecordBatches to JSON using ArrayWriter
            let mut buf = Vec::new();
            {
                let mut writer = arrow_json::ArrayWriter::new(&mut buf);
                if let Err(e) = writer.write_batches(&batches.iter().collect::<Vec<_>>()) {
                    let mut resp = QueryResponse::default();
                    resp.status = "error".to_string();
                    resp.data = None;
                    resp.message = Some(format!("JSON serialization error: {}", e));
                    return Json(resp);
                }
                if let Err(e) = writer.finish() {
                    let mut resp = QueryResponse::default();
                    resp.status = "error".to_string();
                    resp.data = None;
                    resp.message = Some(format!("JSON writer finish error: {}", e));
                    return Json(resp);
                }
            }

            match serde_json::from_slice::<serde_json::Value>(&buf) {
                Ok(data) => {
                    let mut resp = QueryResponse::default();
                    resp.status = "success".to_string();
                    resp.data = Some(data);
                    resp.message = None;
                    Json(resp)
                }
                Err(e) => {
                    let mut resp = QueryResponse::default();
                    resp.status = "error".to_string();
                    resp.data = None;
                    resp.message = Some(format!("JSON parse error: {}", e));
                    Json(resp)
                }
            }
        }
        Err(e) => {
            let mut resp = QueryResponse::default();
            resp.status = "error".to_string();
            resp.data = None;
            resp.message = Some(e.to_string());
            Json(resp)
        }
    }
}
