use std::sync::Arc;
use std::any::Any;
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::Expr;
use datafusion::sql::TableReference;

use super::common::{SqlMetadataFetcher, SqlProviderFactory, FetchedMetadata};

pub async fn register_tables(
    context: &SessionContext,
    target_catalog: &str,
    _source_name: &str,
    metadata_fetcher: Option<Box<dyn SqlMetadataFetcher>>,
    factory: &dyn SqlProviderFactory,
    cb: Arc<crate::query::circuit_breaker::AdaptiveCircuitBreaker>,
    tables: Vec<(String, String)>, // (table_name, target_schema_name)
) -> Result<()> {
    use datafusion::catalog::MemorySchemaProvider;
    use crate::query::circuit_breaker::CircuitBreakerTableProvider;
    
    // Ensure catalog exists
    let catalog = context.catalog(target_catalog).ok_or(anyhow::anyhow!("Catalog {} not found", target_catalog))?;
    
    for (table_name, target_schema) in tables {
        // Ensure the schema exists
        if catalog.schema(&target_schema).is_none() {
            catalog.register_schema(
                &target_schema,
                Arc::new(MemorySchemaProvider::new()),
            )?;
        }

        let table_ref = TableReference::bare(table_name.as_str());
        match factory.create_table_provider(table_ref).await {
            Ok(provider) => {
                let metadata = if let Some(fetcher) = &metadata_fetcher {
                    match fetcher.fetch_metadata("public", &table_name).await {
                         Ok(c) => c,
                         Err(e) => {
                             tracing::warn!("Failed to fetch metadata for {}.{}: {}", target_schema, table_name, e);
                             FetchedMetadata::default()
                         }
                    }
                } else {
                    FetchedMetadata::default()
                };

                // Enrich Schema
                let existing_schema = provider.schema();
                let mut new_fields = Vec::new();
                for field in existing_schema.fields() {
                    if let Some(desc) = metadata.columns.get(field.name()) {
                        let mut new_metadata = field.metadata().clone();
                        new_metadata.insert("ARROW:FLIGHT:SQL:REMARKS".to_string(), desc.clone());
                        new_metadata.insert("description".to_string(), desc.clone());
                        new_metadata.insert("comment".to_string(), desc.clone());
                        new_metadata.insert("remarks".to_string(), desc.clone());
                        let new_field = field.as_ref().clone().with_metadata(new_metadata);
                        new_fields.push(Arc::new(new_field));
                    } else {
                        new_fields.push(field.clone());
                    }
                }
                
                let mut schema_metadata = existing_schema.metadata().clone();
                if let Some(table_desc) = metadata.table_description {
                    schema_metadata.insert("ARROW:FLIGHT:SQL:REMARKS".to_string(), table_desc.clone());
                    schema_metadata.insert("description".to_string(), table_desc.clone());
                    schema_metadata.insert("comment".to_string(), table_desc.clone());
                    schema_metadata.insert("remarks".to_string(), table_desc.clone());
                }
                
                let new_schema = Arc::new(Schema::new_with_metadata(new_fields, schema_metadata));
                
                let enriched_provider = Arc::new(MetadataEnrichedTableProvider {
                    inner: provider,
                    schema: new_schema,
                });

                let cb_provider = Arc::new(CircuitBreakerTableProvider::new(enriched_provider, cb.clone()));
                
                let qualified = TableReference::full(target_catalog, &*target_schema, table_name.as_str());
                if let Err(e) = context.register_table(qualified, cb_provider) {
                     tracing::warn!("Failed to register table {}.{}: {}", target_schema, table_name, e);
                } else {
                     tracing::info!("Registered {}.{}.{}", target_catalog, target_schema, table_name);
                }
            }
            Err(e) => {
                tracing::warn!("Skipping table {} due to error: {}", table_name, e);
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub struct MetadataEnrichedTableProvider {
    pub inner: Arc<dyn TableProvider>,
    pub schema: SchemaRef,
}

#[async_trait]
impl TableProvider for MetadataEnrichedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}
