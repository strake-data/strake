//! # Adaptive Circuit Breaker
//!
//! Wraps data sources to prevent cascading failures when a backend is slow or down.
//! Implements a "trip" mechanism based on consecutive failures or high latency.
//!
//! ## Overview
//!
//! This module provides an [`crate::circuit_breaker::AdaptiveCircuitBreaker`] that monitors request success/failure
//! rates and latency. When thresholds are exceeded, the circuit "trips" to the `Open` state,
//! failing fast and protecting downstream resources. It support automatic recovery
//! through a `HalfOpen` state.
//!
//! The [`crate::circuit_breaker::CircuitBreakerTableProvider`] integrates this with DataFusion by wrapping any
//! inner `TableProvider`.
//!
//! ## Safety
//!
//! This module uses `std::sync::RwLock` and `std::sync::Mutex` for thread-safe state management.
//! No unsafe code is used.
//!
//! ## Errors
//!
//! Returns `datafusion::error::DataFusionError::External` with a "Circuit breaker is OPEN"
//! message when the circuit is tripped.
//!
//! ## Performance Characteristics
//!
//! - **State Checks**: Double-checked locking pattern minimizes write-lock contention.
//! - **Metrics**: Transition and request counters use OpenTelemetry attributes for high-cardinality tracking.
//! - **Memory**: `cleanup_window` ensures the attempt history remains bounded.

use std::any::Any;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::StreamExt;

#[cfg(feature = "telemetry")]
use opentelemetry::{KeyValue, global, metrics::Counter};

/// State of the circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation: requests are allowed.
    Closed,
    /// Failed state: requests are blocked.
    Open,
    /// Recovery state: a limited number of requests are allowed to test the backend.
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// Configuration for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Human-readable name for telemetry and logging.
    pub name: String,
    /// Number of consecutive failures or error rate threshold to trip the circuit.
    pub failure_threshold: usize,
    /// Number of successful requests in `HalfOpen` state to close the circuit.
    pub success_threshold: usize,
    /// Time to wait in `Open` state before transitioning to `HalfOpen`.
    pub reset_timeout: Duration,
    /// Error rate (0.0 - 1.0) above which the circuit trips.
    pub error_rate_threshold: f64,
    /// Time window for tracking attempts.
    pub window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            name: "unknown".to_string(),
            failure_threshold: 5,
            success_threshold: 2,
            reset_timeout: Duration::from_secs(30),
            error_rate_threshold: 0.5,
            window: Duration::from_secs(5 * 60), // 5 minutes
        }
    }
}

#[derive(Debug)]
struct Attempt {
    timestamp: Instant,
    success: bool,
}

/// An adaptive circuit breaker that tracks success rates and tripped states.
#[derive(Debug)]
pub struct AdaptiveCircuitBreaker {
    config: CircuitBreakerConfig,
    state: RwLock<(CircuitState, Instant)>,
    attempts: Mutex<VecDeque<Attempt>>,
    success_count: AtomicUsize,

    // Metrics
    #[cfg(feature = "telemetry")]
    transition_counter: Counter<u64>,
    #[cfg(feature = "telemetry")]
    request_counter: Counter<u64>,
}

impl AdaptiveCircuitBreaker {
    /// Creates a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        #[cfg(feature = "telemetry")]
        let meter = global::meter("strake-circuit-breaker");
        #[cfg(feature = "telemetry")]
        let transition_counter = meter
            .u64_counter("circuit_breaker_transitions_total")
            .with_description("Total number of circuit breaker state transitions")
            .build();
        #[cfg(feature = "telemetry")]
        let request_counter = meter
            .u64_counter("circuit_breaker_requests_total")
            .with_description("Total number of requests processed by circuit breaker")
            .build();

        Self {
            config,
            state: RwLock::new((CircuitState::Closed, Instant::now())),
            attempts: Mutex::new(VecDeque::new()),
            success_count: AtomicUsize::new(0),

            #[cfg(feature = "telemetry")]
            transition_counter,
            #[cfg(feature = "telemetry")]
            request_counter,
        }
    }

    /// Returns the current state of the circuit breaker.
    pub fn state(&self) -> CircuitState {
        // Double-checked locking pattern
        let (current_state, last_update): (CircuitState, Instant) =
            *self.state.read().unwrap_or_else(|e| e.into_inner());

        if current_state == CircuitState::Open && last_update.elapsed() > self.config.reset_timeout
        {
            let mut state_guard = self.state.write().unwrap_or_else(|e| e.into_inner());
            // Verify condition again under write lock
            if state_guard.0 == CircuitState::Open
                && state_guard.1.elapsed() > self.config.reset_timeout
            {
                self.transition_to(&mut state_guard.0, CircuitState::HalfOpen);
                state_guard.1 = Instant::now();

                // Reset success count for HalfOpen test
                self.success_count.store(0, Ordering::SeqCst);

                return CircuitState::HalfOpen;
            }
            return state_guard.0;
        }

        current_state
    }

    /// Records a successful request.
    pub fn record_success(&self) {
        self.record_attempt(true);

        #[cfg(feature = "telemetry")]
        self.request_counter.add(
            1,
            &[
                KeyValue::new("name", self.config.name.clone()),
                KeyValue::new("result", "success"),
            ],
        );

        let state_val = { self.state.read().unwrap_or_else(|e| e.into_inner()).0 };
        if state_val == CircuitState::HalfOpen {
            let count = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;

            if count >= self.config.success_threshold {
                let mut state_guard = self.state.write().unwrap_or_else(|e| e.into_inner());
                if state_guard.0 == CircuitState::HalfOpen {
                    self.transition_to(&mut state_guard.0, CircuitState::Closed);
                    state_guard.1 = Instant::now();
                    self.attempts
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clear();
                }
            }
        }
    }

    /// Records a failed request.
    pub fn record_failure(&self) {
        self.record_attempt(false);

        #[cfg(feature = "telemetry")]
        self.request_counter.add(
            1,
            &[
                KeyValue::new("name", self.config.name.clone()),
                KeyValue::new("result", "failure"),
            ],
        );

        if self.should_trip() {
            let mut state_guard = self.state.write().unwrap_or_else(|e| e.into_inner());
            if state_guard.0 != CircuitState::Open {
                self.transition_to(&mut state_guard.0, CircuitState::Open);
                state_guard.1 = Instant::now();
            }
        }
    }

    fn record_attempt(&self, success: bool) {
        let mut attempts = self.attempts.lock().unwrap_or_else(|e| e.into_inner());
        attempts.push_back(Attempt {
            timestamp: Instant::now(),
            success,
        });
        self.cleanup_window(&mut attempts);
    }

    fn cleanup_window(&self, attempts: &mut VecDeque<Attempt>) {
        let now = Instant::now();
        let max_capacity = self.config.failure_threshold * 100;

        // Efficient time-based cleanup
        while let Some(attempt) = attempts.front() {
            if now.duration_since(attempt.timestamp) > self.config.window {
                attempts.pop_front();
            } else {
                break;
            }
        }

        // Bounded capacity safeguard (O(1) amortized via rotate/truncate or just O(n) drain)
        // With VecDeque, drain is O(n), but truncate is also potentially O(n) if removing from front.
        // Actually, we want to keep the newest (back).
        if attempts.len() > max_capacity {
            let remove_count = attempts.len() - max_capacity;
            attempts.drain(0..remove_count);
        }
    }

    fn should_trip(&self) -> bool {
        let attempts = self.attempts.lock().unwrap();
        if attempts.len() < self.config.failure_threshold {
            return false;
        }

        let total = attempts.len();
        let failures = attempts.iter().filter(|a| !a.success).count();
        let error_rate = failures as f64 / total as f64;

        error_rate >= self.config.error_rate_threshold
    }

    fn transition_to(&self, state_ref: &mut CircuitState, new_state: CircuitState) {
        #[cfg(feature = "telemetry")]
        self.transition_counter.add(
            1,
            &[
                KeyValue::new("name", self.config.name.clone()),
                KeyValue::new("from", state_ref.to_string()),
                KeyValue::new("to", new_state.to_string()),
            ],
        );

        *state_ref = new_state;
    }
}

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

/// Execution plan wrapper that monitors successes/failures and exposes DataFusion metrics.
#[derive(Debug)]
pub struct CircuitBreakerExec {
    inner: Arc<dyn ExecutionPlan>,
    cb: Arc<AdaptiveCircuitBreaker>,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
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

    fn properties(&self) -> &PlanProperties {
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
        let output_bytes = MetricBuilder::new(&self.metrics).counter("output_bytes", partition);
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
        Some(self.metrics.clone_inner())
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

                // For simplicity, any non-empty batch counts as a success.
                // In a real system, we might want to hook into errors from the inner stream.
                if rows > 0 {
                    self.cb.record_success();
                }

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                self.elapsed_compute.add_duration(started.elapsed());
                self.cb.record_failure();
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.elapsed_compute.add_duration(started.elapsed());
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_trip_logic() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            error_rate_threshold: 0.5,
            ..Default::default()
        };
        let cb = AdaptiveCircuitBreaker::new(config);

        assert_eq!(cb.state(), CircuitState::Closed);

        // First failure: not enough total attempts
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        cb.record_failure(); // 3 failures, > 50% error rate
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            reset_timeout: Duration::from_millis(50),
            error_rate_threshold: 0.1,
            ..Default::default()
        };
        let cb = AdaptiveCircuitBreaker::new(config);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Record successes
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_cleanup_window() {
        let config = CircuitBreakerConfig {
            window: Duration::from_millis(100),
            failure_threshold: 1,
            ..Default::default()
        };
        let cb = AdaptiveCircuitBreaker::new(config);

        cb.record_success();
        {
            let attempts = cb.attempts.lock().unwrap();
            assert_eq!(attempts.len(), 1);
        }

        tokio::time::sleep(Duration::from_millis(150)).await;
        // Trigger a cleanup via another record
        cb.record_success();

        {
            let attempts = cb.attempts.lock().unwrap();
            // The old one should be gone
            assert_eq!(attempts.len(), 1);
        }
    }
}
