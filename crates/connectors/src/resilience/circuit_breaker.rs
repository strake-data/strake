//! # Adaptive Circuit Breaker (DataFusion Extensions)
//!
//! Provides the execution layer wrapper nodes to implement the adaptive
//! circuit breaker pattern inside DataFusion execution plans.
//!
//! [`CircuitBreakerTableProvider`] can wrap any base [`TableProvider`] to protect
//! it from query cascading failures.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use strake_common::circuit_breaker::{AdaptiveCircuitBreaker, CircuitState};

/// A [`TableProvider`] that wraps another [`TableProvider`] with a circuit breaker.
#[derive(Debug)]
pub struct CircuitBreakerTableProvider {
    inner: Arc<dyn TableProvider>,
    cb: Arc<AdaptiveCircuitBreaker>,
}

impl CircuitBreakerTableProvider {
    /// Creates a new provider that wraps `inner` with the provided `cb`.
    pub fn new(inner: Arc<dyn TableProvider>, cb: Arc<AdaptiveCircuitBreaker>) -> Self {
        Self { inner, cb }
    }

    /// Returns the underlying [`TableProvider`] wrapped by this circuit breaker.
    pub fn inner(&self) -> Arc<dyn TableProvider> {
        self.inner.clone()
    }
}

#[async_trait::async_trait]
impl TableProvider for CircuitBreakerTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Check circuit state
        let current_state = self.cb.state();
        if current_state == CircuitState::Open {
            return Err(datafusion::error::DataFusionError::External(
                anyhow::anyhow!(
                    "Circuit breaker is OPEN for source '{}'",
                    self.cb.config.name
                )
                .into(),
            ));
        }

        // Execute scan to get inner plan
        match self.inner.scan(state, projection, filters, limit).await {
            Ok(plan) => {
                // Wrap the plan to monitor execution and expose metrics
                Ok(Arc::new(CircuitBreakerExec::new(
                    plan,
                    Arc::clone(&self.cb),
                )))
            }
            Err(e) => {
                // Synchronously record early setup failures
                self.cb.record_failure();
                Err(e)
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

impl crate::sources::WrappingTableProvider for CircuitBreakerTableProvider {
    fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

/// Execution plan wrapper that monitors successes/failures and exposes DataFusion metrics.
#[derive(Debug)]
pub struct CircuitBreakerExec {
    inner: Arc<dyn ExecutionPlan>,
    cb: Arc<AdaptiveCircuitBreaker>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl CircuitBreakerExec {
    /// Creates a new execution plan wrapper.
    pub fn new(inner: Arc<dyn ExecutionPlan>, cb: Arc<AdaptiveCircuitBreaker>) -> Self {
        Self {
            properties: inner.properties().clone(),
            inner,
            cb,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for CircuitBreakerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CircuitBreakerExec(name={})", self.cb.config.name)
    }
}

impl ExecutionPlan for CircuitBreakerExec {
    fn name(&self) -> &'static str {
        "CircuitBreakerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let child = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "CircuitBreakerExec requires exactly one child".to_string(),
            )
        })?;

        Ok(Arc::new(Self::new(child, Arc::clone(&self.cb))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        Ok(Box::pin(CircuitBreakerStream {
            inner: stream,
            cb: Arc::clone(&self.cb),
            output_rows,
            output_bytes,
            elapsed_compute,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        // Optimization: propagate internal metrics instead of just wrapping them?
        // Let's combine child metrics if needed, or rely on execution propagation.
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DataFusionResult<datafusion::physical_plan::Statistics> {
        self.inner.partition_statistics(partition)
    }
}

struct CircuitBreakerStream {
    inner: SendableRecordBatchStream,
    cb: Arc<AdaptiveCircuitBreaker>,
    output_rows: datafusion::physical_plan::metrics::Count,
    output_bytes: datafusion::physical_plan::metrics::Count,
    elapsed_compute: datafusion::physical_plan::metrics::Time,
}

impl RecordBatchStream for CircuitBreakerStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for CircuitBreakerStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let started = Instant::now();
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch: RecordBatch = batch;
                let poll_duration = started.elapsed();
                self.elapsed_compute.add_duration(poll_duration);

                let rows = batch.num_rows();
                let bytes = batch.get_array_memory_size();
                self.output_rows.add(rows);
                self.output_bytes.add(bytes);

                // For simplicity, any batch counts as a success (even 0 rows),
                // removed previous `rows > 0` condition.
                self.cb.record_success();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                self.elapsed_compute.add_duration(started.elapsed());
                self.cb.record_failure();
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.elapsed_compute.add_duration(started.elapsed());
                self.cb.record_success();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
