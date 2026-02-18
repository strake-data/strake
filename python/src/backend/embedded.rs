use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use strake_common::config::{AppConfig, Config, QueryLimits, ResourceConfig};
use strake_runtime::federation::FederationEngine;

use super::StrakeQueryExecutor;

pub struct EmbeddedBackend {
    engine: Arc<FederationEngine>,
}

impl EmbeddedBackend {
    pub async fn new(
        config_path_str: &str,
        sources_config: Option<String>,
    ) -> anyhow::Result<Self> {
        // Load AppConfig (for side effects? original code loaded it but didn't use it except to map error)
        let _app_config = AppConfig::from_file(config_path_str)
            .map_err(|e| anyhow::anyhow!("Failed to load app config: {}", e))?;

        // Use provided sources path OR fall back to same directory as strake.yaml
        let config_path_final = if let Some(p) = sources_config {
            Path::new(&p).to_path_buf()
        } else {
            Path::new(config_path_str)
                .parent()
                .unwrap_or(Path::new(""))
                .join("sources.yaml")
        };

        let config = Config::from_file(config_path_final.to_str().unwrap_or("config/sources.yaml"))
            .map_err(|e| anyhow::anyhow!("Failed to load sources config: {}", e))?;

        let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: QueryLimits::default(),
            resource_config: ResourceConfig::default(),
            datafusion_config: HashMap::new(),
            global_budget: 100,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: _app_config.retry,
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
            format!("SHOW COLUMNS FROM {}", name)
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
}
