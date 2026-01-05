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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,
    pub success_threshold: usize,
    pub reset_timeout: Duration,
    pub error_rate_threshold: f64,
    pub window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
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
}

impl AdaptiveCircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: RwLock::new((CircuitState::Closed, Instant::now())),
            attempts: RwLock::new(VecDeque::new()),
            success_count: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    pub async fn state(&self) -> CircuitState {
        let mut state_guard = self.state.write().await;
        if state_guard.0 == CircuitState::Open
            && state_guard.1.elapsed() > self.config.reset_timeout
        {
            state_guard.0 = CircuitState::HalfOpen;
            let mut success_count = self.success_count.lock().await;
            *success_count = 0;
        }
        state_guard.0
    }

    pub async fn record_success(&self) {
        self.record_attempt(true).await;
        let mut state_guard = self.state.write().await;
        if state_guard.0 == CircuitState::HalfOpen {
            let mut success_count = self.success_count.lock().await;
            *success_count += 1;
            if *success_count >= self.config.success_threshold {
                state_guard.0 = CircuitState::Closed;
                state_guard.1 = Instant::now();
                self.attempts.write().await.clear();
            }
        }
    }

    pub async fn record_failure(&self) {
        self.record_attempt(false).await;
        let mut state_guard = self.state.write().await;
        if state_guard.0 == CircuitState::Closed || state_guard.0 == CircuitState::HalfOpen {
            if self.should_trip().await {
                state_guard.0 = CircuitState::Open;
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
        while let Some(attempt) = attempts.front() {
            if now.duration_since(attempt.timestamp) > self.config.window {
                attempts.pop_front();
            } else {
                break;
            }
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
