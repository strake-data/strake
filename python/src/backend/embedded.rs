//! # Embedded Backend
//!
//! Provides a local query executor powered by DataFusion.
//!
//! ## Overview
//! This module contains the `EmbeddedBackend` which directly drives a `FederationEngine`
//! within the same process.
//!
//! ## Errors
//! Methods return `anyhow::Result` mapped to Python exceptions on failure.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use strake_common::config::{Config, QueryLimits, ResourceConfig};
use strake_runtime::federation::FederationEngine;

use super::StrakeQueryExecutor;

/// A local DataFusion-powered backend running inside the same process.
pub struct EmbeddedBackend {
    engine: Arc<FederationEngine>,
}

impl EmbeddedBackend {
    /// Create a new embedded execution engine with the given DSN and source configuration.
    pub async fn new(
        config_path_str: &str,
        sources_config: Option<String>,
    ) -> anyhow::Result<Self> {
        // Use provided sources path OR fall back to same directory as strake.yaml
        let config_path_final = if let Some(p) = sources_config {
            Path::new(&p).to_path_buf()
        } else {
            Path::new(config_path_str)
                .parent()
                .ok_or_else(|| {
                    anyhow::anyhow!("Config path '{}' has no parent directory", config_path_str)
                })?
                .join("sources.yaml")
        };

        let config_str = config_path_final.to_str().ok_or_else(|| {
            anyhow::anyhow!("Invalid character in config path: {:?}", config_path_final)
        })?;

        let config = Config::from_file(config_str).map_err(|e| {
            anyhow::anyhow!("Failed to load sources config from {}: {}", config_str, e)
        })?;

        let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: QueryLimits::default(),
            resource_config: ResourceConfig::default(),
            datafusion_config: HashMap::new(),
            global_budget: 100,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: Default::default(),
        })
        .await?;

        Ok(Self {
            engine: Arc::new(engine),
        })
    }
}

#[async_trait]
impl StrakeQueryExecutor for EmbeddedBackend {
    async fn execute(&mut self, query: &str) -> anyhow::Result<(Arc<Schema>, Vec<RecordBatch>)> {
        let (schema, batches, _warnings) = self.engine.execute_query(query, None).await?;
        Ok((schema, batches))
    }

    async fn trace(&mut self, query: &str) -> anyhow::Result<String> {
        // Start with simple EXPLAIN (Logical Plan)
        // Note: we construct the EXPLAIN query text because we want to reuse the engine's parser/plan logic
        let explain_sql = format!("EXPLAIN {}", query);
        let (_, batches, _) = self.engine.execute_query(&explain_sql, None).await?;
        let pretty = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
        Ok(pretty)
    }

    async fn describe(&mut self, table_name: Option<String>) -> anyhow::Result<String> {
        let query = if let Some(name) = table_name {
            if name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
            {
                format!("SHOW COLUMNS FROM {}", name)
            } else {
                return Err(anyhow::anyhow!("Invalid table identifier: {}", name));
            }
        } else {
            "SHOW TABLES".to_string()
        };

        let (_, batches, _) = self.engine.execute_query(&query, None).await?;
        let pretty = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
        Ok(pretty)
    }

    async fn explain_tree(&mut self, query: &str) -> anyhow::Result<String> {
        self.engine.explain_tree(query).await
    }

    async fn list_sources(&mut self) -> anyhow::Result<String> {
        let sources = self.engine.list_sources();
        serde_json::to_string(&sources)
            .map_err(|e| anyhow::anyhow!("Failed to serialize sources: {}", e))
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        // FederationEngine does not expose an explicit shutdown API.
        // Resources are released when the Arc reference count drops to zero.
        tracing::info!("EmbeddedBackend shutdown: deferring to Arc drop.");
        Ok(())
    }
}
