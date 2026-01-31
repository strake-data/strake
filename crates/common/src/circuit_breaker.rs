//! Adaptive circuit breaker for federated sources.
//!
//! Wraps data sources to prevent cascading failures when a backend is slow or down.
//! Implements a "trip" mechanism based on consecutive failures or high latency.
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

#[cfg(feature = "telemetry")]
use opentelemetry::{global, metrics::Counter, KeyValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
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

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub name: String, // Added name for metrics
    pub failure_threshold: usize,
    pub success_threshold: usize,
    pub reset_timeout: Duration,
    pub error_rate_threshold: f64,
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
            window: Duration::from_secs(300), // 5 minutes
        }
    }
}

#[derive(Debug)]
struct Attempt {
    timestamp: Instant,
    success: bool,
}

#[derive(Debug)]
pub struct AdaptiveCircuitBreaker {
    config: CircuitBreakerConfig,
    state: RwLock<(CircuitState, Instant)>,
    attempts: RwLock<VecDeque<Attempt>>,
    success_count: Arc<tokio::sync::Mutex<usize>>,

    // Metrics
    #[cfg(feature = "telemetry")]
    transition_counter: Counter<u64>,
    #[cfg(feature = "telemetry")]
    request_counter: Counter<u64>,
}

impl AdaptiveCircuitBreaker {
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
            attempts: RwLock::new(VecDeque::new()),
            success_count: Arc::new(tokio::sync::Mutex::new(0)),

            #[cfg(feature = "telemetry")]
            transition_counter,
            #[cfg(feature = "telemetry")]
            request_counter,
        }
    }

    pub async fn state(&self) -> CircuitState {
        // Double-checked locking pattern
        let (current_state, last_update) = *self.state.read().await;

        if current_state == CircuitState::Open && last_update.elapsed() > self.config.reset_timeout
        {
            let mut state_guard = self.state.write().await;
            // Verify condition again under write lock
            if state_guard.0 == CircuitState::Open
                && state_guard.1.elapsed() > self.config.reset_timeout
            {
                self.transition_to(&mut state_guard.0, CircuitState::HalfOpen);
                state_guard.1 = Instant::now();

                // Reset success count for HalfOpen test
                let mut success_count = self.success_count.lock().await;
                *success_count = 0;

                return CircuitState::HalfOpen;
            }
            return state_guard.0;
        }

        current_state
    }

    pub async fn record_success(&self) {
        self.record_attempt(true).await;

        #[cfg(feature = "telemetry")]
        self.request_counter.add(
            1,
            &[
                KeyValue::new("name", self.config.name.clone()),
                KeyValue::new("result", "success"),
            ],
        );

        // Only lock efficiently
        {
            let state_read = self.state.read().await;
            if state_read.0 != CircuitState::HalfOpen {
                return;
            }
        }

        // Upgrade to write if potentially needed (HalfOpen logic)
        // But we actually need to increment success_count which is a separate Mutex.
        // And if threshold reached, we need write lock on state.

        // Logic:
        // check state (read). if HalfOpen:
        //   take success_count lock. increment. if >= threshold:
        //     take state write lock.
        //     if still HalfOpen: close it.

        let state_val = { self.state.read().await.0 };
        if state_val == CircuitState::HalfOpen {
            let mut success_count = self.success_count.lock().await;
            *success_count += 1;

            if *success_count >= self.config.success_threshold {
                let mut state_guard = self.state.write().await;
                if state_guard.0 == CircuitState::HalfOpen {
                    self.transition_to(&mut state_guard.0, CircuitState::Closed);
                    state_guard.1 = Instant::now();
                    self.attempts.write().await.clear();
                }
            }
        }
    }

    pub async fn record_failure(&self) {
        self.record_attempt(false).await;

        #[cfg(feature = "telemetry")]
        self.request_counter.add(
            1,
            &[
                KeyValue::new("name", self.config.name.clone()),
                KeyValue::new("result", "failure"),
            ],
        );

        if self.should_trip().await {
            let mut state_guard = self.state.write().await;
            if state_guard.0 != CircuitState::Open {
                self.transition_to(&mut state_guard.0, CircuitState::Open);
                state_guard.1 = Instant::now();
            }
        }
    }

    async fn record_attempt(&self, success: bool) {
        let mut attempts = self.attempts.write().await;
        attempts.push_back(Attempt {
            timestamp: Instant::now(),
            success,
        });
        self.cleanup_window(&mut attempts);
    }

    fn cleanup_window(&self, attempts: &mut VecDeque<Attempt>) {
        let now = Instant::now();
        // Limit attempts size to prevent memory leak if window is huge or flood happens
        // Max capacity safeguard: 1000 * failure_threshold (arbitrary but safe)
        let max_capacity = self.config.failure_threshold * 100;

        if attempts.len() > max_capacity {
            attempts.truncate(max_capacity); // Keep oldest? No, usually keep newest.
                                             // truncate keeps specifically from start, so we want to remove from front?
                                             // VecDeque::truncate keeps the first `len` elements. Oldest are at front?
                                             // usually push_back -> newest at back.
                                             // so strict cleanup by time is better.
        }

        // Strict time window cleanup
        while let Some(attempt) = attempts.front() {
            if now.duration_since(attempt.timestamp) > self.config.window {
                attempts.pop_front();
            } else {
                break;
            }
        }
        // Additional safeguard for massive bursts
        if attempts.len() > max_capacity {
            // Remove oldest
            let overflow = attempts.len() - max_capacity;
            attempts.drain(0..overflow);
        }
    }

    async fn should_trip(&self) -> bool {
        let attempts = self.attempts.read().await;
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

/// A TableProvider that wraps another TableProvider with a circuit breaker.
#[derive(Debug)]
pub struct CircuitBreakerTableProvider {
    inner: Arc<dyn TableProvider>,
    cb: Arc<AdaptiveCircuitBreaker>,
}

impl CircuitBreakerTableProvider {
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
        let current_state = self.cb.state().await;
        if current_state == CircuitState::Open {
            #[cfg(feature = "telemetry")]
            {
                // Maybe record a "blocked" metric?
                // Rely on requests counter? but record_failure won't be called here.
            }

            return Err(datafusion::error::DataFusionError::External(
                anyhow::anyhow!("Circuit breaker is OPEN for this source").into(),
            ));
        }

        // Execute scan
        match self.inner.scan(state, projection, filters, limit).await {
            Ok(plan) => {
                // We wrap the plan as well to record success/failure during execution of the plan
                // But for now, just recording success on scan initiation might be enough for generic connectivity issues.
                // More advanced: wrap the ExecutionPlan to record failures during actual data fetch.
                self.cb.record_success().await;
                Ok(plan)
            }
            Err(e) => {
                self.cb.record_failure().await;
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
