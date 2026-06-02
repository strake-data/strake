//! # SQL Connector Wrappers
//!
//! Middleware and decorators for SQL-based TableProviders.
//!
//! This module provides wrappers for concurrency limiting, circuit breaking,
//! metadata enrichment, and schema drift detection.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::TryStreamExt;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::Semaphore;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};

/// Helper to wrap a provider with circuit breaking.
///
/// This function applies a decorator chain that provides circuit breaking protection.
/// Optionally enables schema drift detection.
pub fn wrap_provider(
    provider: Arc<dyn TableProvider>,
    cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    schema_drift: bool,
) -> Arc<dyn TableProvider> {
    use crate::resilience::circuit_breaker::CircuitBreakerTableProvider;

    let with_cb = Arc::new(CircuitBreakerTableProvider::new(provider, cb));

    if schema_drift {
        Arc::new(crate::sources::schema_drift::SchemaDriftTableProvider::new(
            with_cb,
        ))
    } else {
        with_cb
    }
}

/// Wraps a provider with a semaphore to limit concurrent query execution.
///
/// Permits are acquired during [`ExecutionPlan::execute`] and released when the
/// resulting stream is dropped.
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

/// A [`TableProvider`] that limits the number of concurrent scans.
#[derive(Debug)]
pub struct ConcurrencyLimitedTableProvider {
    /// The underlying provider.
    pub inner: Arc<dyn TableProvider>,
    /// Semaphore shared across all plans created by this provider.
    pub semaphore: Arc<Semaphore>,
}

impl ConcurrencyLimitedTableProvider {
    /// Returns the underlying [`TableProvider`] wrapped by this concurrency limiter.
    pub fn inner(&self) -> Arc<dyn TableProvider> {
        self.inner.clone()
    }
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let inner_plan = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(ConcurrencyLimitedExec::new(
            inner_plan,
            self.semaphore.clone(),
        )))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        self.inner.supports_filters_pushdown(filters)
    }
}

impl crate::sources::WrappingTableProvider for ConcurrencyLimitedTableProvider {
    fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

/// An [`ExecutionPlan`] that acquires a semaphore permit before executing the inner plan.
#[derive(Debug)]
pub struct ConcurrencyLimitedExec {
    inner: Arc<dyn ExecutionPlan>,
    semaphore: Arc<Semaphore>,
    metrics: ExecutionPlanMetricsSet,
}

impl ConcurrencyLimitedExec {
    /// Creates a new `ConcurrencyLimitedExec`.
    pub fn new(inner: Arc<dyn ExecutionPlan>, semaphore: Arc<Semaphore>) -> Self {
        Self {
            inner,
            semaphore,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
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
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let inner = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "ConcurrencyLimitedExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(ConcurrencyLimitedExec::new(
            inner,
            self.semaphore.clone(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<datafusion::physical_plan::Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let semaphore = self.semaphore.clone();
        let inner = self.inner.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_bytes = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .output_bytes(partition);
        let metrics_cloned = metrics.clone();

        let stream = futures::stream::once(async move {
            let permit = {
                let _timer = metrics_cloned.elapsed_compute().timer();
                semaphore
                    .acquire_owned()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            };

            let stream = inner.execute(partition, context)?;
            Ok::<_, datafusion::error::DataFusionError>(Box::pin(PermitStream {
                inner: stream,
                _permit: permit,
                metrics: metrics.clone(),
                output_bytes: output_bytes.clone(),
            })
                as datafusion::physical_plan::SendableRecordBatchStream)
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
    metrics: BaselineMetrics,
    output_bytes: datafusion::physical_plan::metrics::Count,
}

impl futures::stream::Stream for PermitStream {
    type Item = datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::stream::StreamExt;
        let poll = self.inner.poll_next_unpin(cx);
        if let std::task::Poll::Ready(Some(Ok(ref batch))) = poll {
            self.metrics.record_output(batch.num_rows());
            self.output_bytes.add(batch.get_array_memory_size());
        }
        poll
    }
}

impl datafusion::execution::RecordBatchStream for PermitStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

/// A [`TableProvider`] decorator that overrides the provider's schema with a custom schema.
#[derive(Debug)]
pub struct SchemaAdaptingTableProvider {
    /// The underlying provider.
    pub inner: Arc<dyn TableProvider>,
    /// The customized schema to return.
    pub custom_schema: SchemaRef,
}

impl SchemaAdaptingTableProvider {
    /// Creates a new `SchemaAdaptingTableProvider`.
    pub fn new(inner: Arc<dyn TableProvider>, custom_schema: SchemaRef) -> Self {
        Self {
            inner,
            custom_schema,
        }
    }
}

#[async_trait]
impl TableProvider for SchemaAdaptingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.custom_schema.clone()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;

        let inner_schema = self.inner.schema();

        // 1. Map projection indices from custom_schema to inner_schema by name
        let mapped_projection = projection
            .map(|proj| {
                proj.iter()
                    .map(|&idx| {
                        let field_name = self.custom_schema.field(idx).name();
                        inner_schema.index_of(field_name).map_err(|_| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "Column '{}' specified in sources.yaml not found in physical database schema",
                                field_name
                            ))
                        })
                    })
                    .collect::<datafusion::common::Result<Vec<usize>>>()
            })
            .transpose()?;

        // 2. Scan the inner table using the mapped projection
        let inner_plan = self
            .inner
            .scan(state, mapped_projection.as_ref(), filters, limit)
            .await?;

        // 3. Construct target schema
        let target_schema = match projection {
            Some(proj) => Arc::new(self.custom_schema.project(proj)?),
            None => self.custom_schema.clone(),
        };

        // 4. Construct projection expressions for ProjectionExec to project and cast columns to align to target_schema
        let inner_plan_schema = inner_plan.schema();
        let mut projection_exprs = Vec::with_capacity(target_schema.fields().len());

        for field in target_schema.fields() {
            let field_name = field.name();
            let inner_plan_idx = inner_plan_schema.index_of(field_name)?;
            let physical_col =
                Arc::new(Column::new(field_name, inner_plan_idx)) as Arc<dyn PhysicalExpr>;

            // If the data type differs, wrap it in a CastExpr to ensure type alignment
            let expr = if inner_plan_schema.field(inner_plan_idx).data_type() != field.data_type() {
                datafusion::physical_expr::expressions::cast(
                    physical_col,
                    &inner_plan_schema,
                    field.data_type().clone(),
                )?
            } else {
                physical_col
            };
            projection_exprs.push((expr, field_name.clone()));
        }

        // 5. Wrap inside ProjectionExec
        let adapted_plan = ProjectionExec::try_new(projection_exprs, inner_plan)?;

        Ok(Arc::new(adapted_plan))
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        self.inner.supports_filters_pushdown(filters)
    }
}

impl crate::sources::WrappingTableProvider for SchemaAdaptingTableProvider {
    fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}
