//! # Generic SQL Connector
//!
//! Provides a standardized way to register SQL-based data sources into DataFusion,
//! handling table discovery, metadata enrichment, and registration.

use anyhow::Result;
use datafusion::sql::TableReference;
use std::sync::Arc;
use strake_common::retry::retry_async;

use super::common::{
    FetchedMetadata, SchemaMappingRule, SqlMetadataFetcher, SqlProviderFactory, SqlSourceParams,
};
use super::wrappers::wrap_concurrent;
use crate::introspect::{IntrospectError, SchemaIntrospector};
use datafusion::catalog::MemorySchemaProvider;

/// A generic connector for SQL data sources.
///
/// This struct implements the logic for discovering tables via an [`SchemaIntrospector`],
/// creating [`TableProvider`]s via an [`SqlProviderFactory`], and registering them
/// into a DataFusion [`SessionContext`].
#[derive(Clone)]
pub struct GenericSqlConnector {
    /// Strategy for discovering tables and their schemas.
    pub introspector: Arc<dyn SchemaIntrospector>,
    /// Factory for creating dialect-specific table providers.
    pub factory: Arc<dyn SqlProviderFactory>,
    /// Optional fetcher for table and column metadata (comments/descriptions).
    pub metadata_fetcher: Option<Arc<dyn SqlMetadataFetcher>>,
    /// Rule for mapping source schemas to DataFusion schemas.
    pub schema_mapping: SchemaMappingRule,
}

impl GenericSqlConnector {
    /// Registers all discovered or explicit tables from the source into DataFusion.
    ///
    /// This method uses retries to handle transient connectivity issues during registration.
    pub async fn register(&self, params: SqlSourceParams) -> Result<()> {
        let name = params.name.clone();
        let retry_settings = params.retry;

        retry_async(
            format!("register_sql_source({})", name),
            retry_settings,
            move || {
                let this = self.clone();
                let params = params.clone();

                async move { this.try_register(params).await }
            },
        )
        .await
    }

    async fn try_register(&self, params: SqlSourceParams) -> Result<()> {
        let tables_to_register = if let Some(config_tables) = params.explicit_tables.as_ref() {
            config_tables
                .iter()
                .map(|t| {
                    let target_schema = self
                        .schema_mapping
                        .map_schema(&t.schema, &params.name)
                        .into_owned();
                    (t.name.clone(), target_schema)
                })
                .collect()
        } else {
            // discovery
            let tables = self
                .introspector
                .list_tables(None)
                .await
                .map_err(|e| match e {
                    IntrospectError::Connection(msg) => {
                        anyhow::anyhow!("Connection failed: {}", msg)
                    }
                    IntrospectError::Permission(msg) => {
                        anyhow::anyhow!("Permission denied: {}", msg)
                    }
                    IntrospectError::NotFound(msg) => anyhow::anyhow!("Table not found: {}", msg),
                    other => anyhow::anyhow!("Introspection failed: {}", other),
                })?;

            tables
                .into_iter()
                .map(|t| {
                    let target_schema = self
                        .schema_mapping
                        .map_schema(&t.schema, &params.name)
                        .into_owned();
                    (t.table, target_schema)
                })
                .collect::<Vec<_>>()
        };

        let catalog = params
            .context
            .catalog(&params.catalog_name)
            .ok_or_else(|| anyhow::anyhow!("Catalog '{}' not found", params.catalog_name))?;

        for (table_name, target_schema) in tables_to_register {
            let metadata = if let Some(fetcher) = &self.metadata_fetcher {
                match fetcher.fetch_metadata(&target_schema, &table_name).await {
                    Ok(m) => Arc::new(m),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to fetch metadata for {}.{}: {}",
                            target_schema,
                            table_name,
                            e
                        );
                        Arc::new(FetchedMetadata::default())
                    }
                }
            } else {
                Arc::new(FetchedMetadata::default())
            };

            let table_ref = TableReference::bare(table_name.as_str());
            match self
                .factory
                .create_table_provider(table_ref, metadata, params.cb.clone())
                .await
            {
                Ok(provider) => {
                    let provider = wrap_concurrent(provider, params.max_concurrent_queries);
                    let target_schema_ref = target_schema.as_str();
                    let table_name_ref = table_name.as_str();

                    // Ensure the schema exists before registration
                    if catalog.schema(target_schema_ref).is_none() {
                        catalog.register_schema(
                            target_schema_ref,
                            Arc::new(MemorySchemaProvider::new()),
                        )?;
                    }

                    let qualified = TableReference::full(
                        params.catalog_name.as_str(),
                        target_schema_ref,
                        table_name_ref,
                    );
                    match params.context.register_table(qualified, provider) {
                        Ok(_) => tracing::info!(
                            "Registered {}.{}.{}",
                            params.catalog_name,
                            target_schema,
                            table_name
                        ),
                        Err(e) => tracing::warn!(
                            "Failed to register table {}.{}: {}",
                            target_schema,
                            table_name,
                            e
                        ),
                    }
                }
                Err(e) => {
                    tracing::warn!("Skipping table {} due to error: {}", table_name, e);
                }
            }
        }

        Ok(())
    }
}
