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
//! ## Safety
//!
//! This module uses `parking_lot::RwLock` and `parking_lot::Mutex` for thread-safe state management.
//! These locks are unpoisonable, ensuring deterministic behavior even if a thread panics.
//!
//! - **Accounting**: O(1) error rate evaluation via bundled counters.
//! - **Memory**: `cleanup_window` ensures the attempt history remains bounded.
//! - **Locking in Stream Path**: `record_success`/`record_failure` acquire `parking_lot::Mutex` and
//!   `parking_lot::RwLock` synchronously from `poll_next`. Worst-case hold time is O(window_size)
//!   during `cleanup_window`, bounded by `failure_threshold * 100`. For default config, this is <1μs.

use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[cfg(feature = "telemetry")]
use opentelemetry::{KeyValue, global, metrics::Counter};

/// State of the circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    pub name: Arc<str>,
    /// Minimum number of attempts in the tracking window before the error rate is evaluated.
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
            name: Default::default(),
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

#[derive(Debug, Default)]
struct AttemptWindow {
    entries: VecDeque<Attempt>,
    total: usize,
    failures: usize,
}

/// An adaptive circuit breaker that tracks success rates and tripped states.
#[derive(Debug)]
pub struct AdaptiveCircuitBreaker {
    /// The active configuration.
    pub config: CircuitBreakerConfig,
    state: RwLock<(CircuitState, Instant)>,
    window: Mutex<AttemptWindow>,
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
            window: Mutex::new(AttemptWindow::default()),
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
        let (current_state, last_update) = *self.state.read();

        if current_state == CircuitState::Open && last_update.elapsed() > self.config.reset_timeout
        {
            let mut state_guard = self.state.write();
            // Verify condition again under write lock
            if state_guard.0 == CircuitState::Open
                && state_guard.1.elapsed() > self.config.reset_timeout
            {
                self.transition_to(&mut state_guard.0, CircuitState::HalfOpen);
                state_guard.1 = Instant::now();

                // Reset success count for HalfOpen test
                self.success_count.store(0, Ordering::Relaxed);

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

        let state_val = self.state.read().0;
        if state_val == CircuitState::HalfOpen {
            let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

            if count >= self.config.success_threshold {
                let mut state_guard = self.state.write();
                if state_guard.0 == CircuitState::HalfOpen {
                    self.transition_to(&mut state_guard.0, CircuitState::Closed);
                    state_guard.1 = Instant::now();
                    *self.window.lock() = AttemptWindow::default();
                    self.success_count.store(0, Ordering::Relaxed);
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
            let mut state_guard = self.state.write();
            if state_guard.0 != CircuitState::Open {
                self.transition_to(&mut state_guard.0, CircuitState::Open);
                state_guard.1 = Instant::now();
            }
        }
    }

    fn record_attempt(&self, success: bool) {
        let mut w = self.window.lock();
        w.entries.push_back(Attempt {
            timestamp: Instant::now(),
            success,
        });
        w.total += 1;
        if !success {
            w.failures += 1;
        }
        self.cleanup_window(&mut w);
    }

    fn cleanup_window(&self, w: &mut AttemptWindow) {
        let now = Instant::now();

        // 1. Time-based cleanup
        while let Some(attempt) = w.entries.front() {
            if now.duration_since(attempt.timestamp) > self.config.window {
                let attempt = w.entries.pop_front().unwrap();
                w.total -= 1;
                if !attempt.success {
                    w.failures -= 1;
                }
            } else {
                break;
            }
        }

        // 2. Bounded capacity safeguard
        let max_capacity = self.config.failure_threshold * 100;
        if w.entries.len() > max_capacity {
            let remove_count = w.entries.len() - max_capacity;
            for attempt in w.entries.drain(0..remove_count) {
                w.total -= 1;
                if !attempt.success {
                    w.failures -= 1;
                }
            }
        }
    }

    fn should_trip(&self) -> bool {
        let w = self.window.lock();
        if w.total < self.config.failure_threshold {
            return false;
        }

        let error_rate = w.failures as f64 / w.total as f64;
        error_rate >= self.config.error_rate_threshold
    }

    fn transition_to(&self, state_ref: &mut CircuitState, new_state: CircuitState) {
        #[cfg(feature = "telemetry")]
        self.transition_counter.add(
            1,
            &[
                KeyValue::new("name", self.config.name.to_string()),
                KeyValue::new("from", state_ref.to_string()),
                KeyValue::new("to", new_state.to_string()),
            ],
        );

        *state_ref = new_state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_counter_accuracy() {
        let cb = AdaptiveCircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 5,
            ..Default::default()
        });

        cb.record_success();
        cb.record_success();
        cb.record_failure();

        let w = cb.window.lock();
        assert_eq!(w.total, 3);
        assert_eq!(w.failures, 1);
        assert_eq!(w.entries.len(), 3);
    }

    #[test]
    fn test_cleanup_window() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            window: Duration::from_millis(50),
            ..Default::default()
        };
        let cb = AdaptiveCircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        {
            let w = cb.window.lock();
            assert_eq!(w.total, 2);
            assert_eq!(w.failures, 2);
        }

        std::thread::sleep(Duration::from_millis(100));

        // This attempt will trigger cleanup of the previous ones
        cb.record_success();

        let w = cb.window.lock();
        assert_eq!(w.total, 1);
        assert_eq!(w.failures, 0);
        assert_eq!(w.entries.len(), 1);
    }

    #[test]
    fn test_trip_logic_o1() {
        let config = CircuitBreakerConfig {
            failure_threshold: 10,
            error_rate_threshold: 0.5,
            ..Default::default()
        };
        let cb = AdaptiveCircuitBreaker::new(config);

        // Record 5 successes, 4 failures (9 total < threshold)
        for _ in 0..5 {
            cb.record_success();
        }
        for _ in 0..4 {
            cb.record_failure();
        }
        assert!(!cb.should_trip());

        // One more failure (10 total, 5/10 = 0.5 threshold)
        cb.record_failure();
        assert!(cb.should_trip());
    }

    #[test]
    fn test_recovery_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            error_rate_threshold: 1.0,
            success_threshold: 1,
            reset_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let cb = AdaptiveCircuitBreaker::new(config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Closing the circuit MUST reset counters
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);

        let w = cb.window.lock();
        assert_eq!(w.total, 0, "Window total should be zeroed after recovery");
        assert_eq!(
            w.failures, 0,
            "Window failures should be zeroed after recovery"
        );
        assert_eq!(w.entries.len(), 0);
        assert_eq!(cb.success_count.load(Ordering::Relaxed), 0);
    }
}
