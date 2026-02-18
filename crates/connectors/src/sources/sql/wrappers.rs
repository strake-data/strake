use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use futures::stream::TryStreamExt;
// use futures::TryFutureExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

use super::common::{FetchedMetadata, SqlMetadataFetcher, SqlProviderFactory};

#[allow(clippy::too_many_arguments)]
pub async fn register_tables(
    context: &SessionContext,
    target_catalog: &str,
    _source_name: &str,
    metadata_fetcher: Option<Box<dyn SqlMetadataFetcher>>,
    factory: &dyn SqlProviderFactory,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    max_concurrent_queries: usize,
    tables: Vec<(String, String)>, // (table_name, target_schema_name)
) -> Result<()> {
    use datafusion::catalog::MemorySchemaProvider;

    // Ensure catalog exists
    let catalog = context
        .catalog(target_catalog)
        .ok_or(anyhow::anyhow!("Catalog {} not found", target_catalog))?;

    for (table_name, target_schema) in tables {
        // Ensure the schema exists
        if catalog.schema(&target_schema).is_none() {
            catalog.register_schema(&target_schema, Arc::new(MemorySchemaProvider::new()))?;
        }

        let metadata = if let Some(fetcher) = &metadata_fetcher {
            match fetcher.fetch_metadata("public", &table_name).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch metadata for {}.{}: {}",
                        target_schema,
                        table_name,
                        e
                    );
                    FetchedMetadata::default()
                }
            }
        } else {
            FetchedMetadata::default()
        };

        let table_ref = TableReference::bare(table_name.as_str());
        match factory
            .create_table_provider(table_ref, metadata, cb.clone())
            .await
        {
            Ok(provider) => {
                let provider = wrap_concurrent(provider, max_concurrent_queries);
                let qualified =
                    TableReference::full(target_catalog, &*target_schema, table_name.as_str());
                if let Err(e) = context.register_table(qualified, provider) {
                    tracing::warn!(
                        "Failed to register table {}.{}: {}",
                        target_schema,
                        table_name,
                        e
                    );
                } else {
                    tracing::info!(
                        "Registered {}.{}.{}",
                        target_catalog,
                        target_schema,
                        table_name
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Skipping table {} due to error: {}", table_name, e);
            }
        }
    }

    Ok(())
}

/// Helper to wrap a provider with metadata and circuit breaking
pub fn wrap_provider(
    provider: Arc<dyn TableProvider>,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    metadata: FetchedMetadata,
) -> Arc<dyn TableProvider> {
    use strake_common::circuit_breaker::CircuitBreakerTableProvider;

    let enriched = Arc::new(MetadataEnrichedTableProvider::new(provider, metadata));
    Arc::new(CircuitBreakerTableProvider::new(enriched, cb))
}

#[derive(Debug)]
pub struct MetadataEnrichedTableProvider {
    pub inner: Arc<dyn TableProvider>,
    pub metadata: FetchedMetadata,
    // Use std::sync::OnceLock because schema() is a synchronous trait method.
    // We cannot await tokio::sync::OnceCell here, and the computation is CPU-bound.
    pub schema: OnceLock<SchemaRef>,
}

impl MetadataEnrichedTableProvider {
    pub fn new(provider: Arc<dyn TableProvider>, metadata: FetchedMetadata) -> Self {
        Self {
            inner: provider,
            metadata,
            schema: OnceLock::new(),
        }
    }

    fn compute_schema(&self) -> SchemaRef {
        let existing_schema = self.inner.schema();
        let mut new_fields = Vec::new();

        for field in existing_schema.fields() {
            if let Some(desc) = self.metadata.columns.get(field.name()) {
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
        if let Some(table_desc) = &self.metadata.table_description {
            schema_metadata.insert("ARROW:FLIGHT:SQL:REMARKS".to_string(), table_desc.clone());
            schema_metadata.insert("description".to_string(), table_desc.clone());
            schema_metadata.insert("comment".to_string(), table_desc.clone());
            schema_metadata.insert("remarks".to_string(), table_desc.clone());
        }

        Arc::new(Schema::new_with_metadata(new_fields, schema_metadata))
    }
}

#[async_trait]
impl TableProvider for MetadataEnrichedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.get_or_init(|| self.compute_schema()).clone()
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
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        self.inner.supports_filters_pushdown(filters)
    }
}

pub fn wrap_concurrent(
    provider: Arc<dyn TableProvider>,
    max_concurrency: usize,
) -> Arc<dyn TableProvider> {
    if max_concurrency == 0 {
        return provider;
    }
    Arc::new(ConcurrencyLimitedTableProvider {
        inner: provider,
        semaphore: Arc::new(Semaphore::new(max_concurrency)),
    })
}

#[derive(Debug)]
pub struct ConcurrencyLimitedTableProvider {
    inner: Arc<dyn TableProvider>,
    semaphore: Arc<Semaphore>,
}

#[async_trait]
impl TableProvider for ConcurrencyLimitedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
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
        let inner_plan = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(ConcurrencyLimitedExec {
            inner: inner_plan,
            semaphore: self.semaphore.clone(),
        }))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        self.inner.supports_filters_pushdown(filters)
    }
}

#[derive(Debug)]
pub struct ConcurrencyLimitedExec {
    inner: Arc<dyn ExecutionPlan>,
    semaphore: Arc<Semaphore>,
}

impl datafusion::physical_plan::DisplayAs for ConcurrencyLimitedExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default
            | datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ConcurrencyLimitedExec: permits={}",
                    self.semaphore.available_permits()
                )
            }
            _ => Ok(()),
        }
    }
}

impl ExecutionPlan for ConcurrencyLimitedExec {
    fn name(&self) -> &str {
        "ConcurrencyLimitedExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ConcurrencyLimitedExec {
            inner: self.inner.clone().with_new_children(children)?,
            semaphore: self.semaphore.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let semaphore = self.semaphore.clone();
        let inner = self.inner.clone();

        let stream = futures::stream::once(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let stream = inner.execute(partition, context)?;
            Ok::<_, datafusion::error::DataFusionError>((stream, permit))
        })
        .try_filter_map(|(stream, permit)| {
            let permit_stream = PermitStream {
                inner: stream,
                _permit: permit,
            };
            // This logic is a bit convoluted to match map semantics, but essentially we want to return the stream
            // keeping the permit alive.
            // Using try_flatten with a stream of stream is one way, but maybe creating a new stream is easier.

            // Note: try_flat_map was removed/not available in recent futures or I'm misremembering.
            // Let's use map + flatten or similar.
            // Actually, let's just use then + flatten if possible or map.

            futures::future::ready(Ok(Some(
                Box::pin(permit_stream) as datafusion::physical_plan::SendableRecordBatchStream
            )))
        })
        .try_flatten();

        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            Box::pin(stream),
        )))
    }
}

struct PermitStream {
    inner: datafusion::physical_plan::SendableRecordBatchStream,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl futures::stream::Stream for PermitStream {
    type Item = datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::stream::StreamExt;
        self.inner.poll_next_unpin(cx)
    }
}

impl datafusion::execution::RecordBatchStream for PermitStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}
