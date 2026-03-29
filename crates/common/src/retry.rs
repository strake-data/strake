//! # Retry Logic
//!
//! Exponential backoff with jitter for resilient asynchronous operations.
//!
//! This module provides the [`crate::retry::retry_async`] function to wrap fallible operations
//! with a configurable retry policy.
//!
//! ## Usage
//!
//! ```rust
//! use strake_common::retry::{retry_async, RetrySettings};
//! use std::time::Duration;
//!
//! # async fn run() {
//! let mut settings = RetrySettings::default();
//! settings.max_attempts = 3;
//! settings.base_delay_ms = 100;
//! settings.max_delay_ms = 1000;
//!
//! let result = retry_async("my_op", settings, || async {
//!     // Do something fallible
//!     Ok::<_, &str>("success")
//! }).await;
//! # }
//! ```

/// Re-export of retry settings.
pub use crate::config::RetrySettings;
use std::future::Future;
use std::time::Duration;
use tracing::{error, warn};

/// Calculate the delay for the next retry attempt with exponential backoff and jitter.
pub fn next_retry_delay(attempt: usize, base_ms: u64, max_ms: u64) -> Duration {
    next_retry_delay_with_rng(&mut rand::rng(), attempt, base_ms, max_ms)
}

/// Calculate the delay for the next retry attempt using a provided RNG.
pub fn next_retry_delay_with_rng(
    rng: &mut impl rand::Rng,
    attempt: usize,
    base_ms: u64,
    max_ms: u64,
) -> Duration {
    let multiplier = 2_u64.saturating_pow(attempt.saturating_sub(1) as u32);
    let delay = base_ms.saturating_mul(multiplier);

    // Add jitter up to 1000ms using a more robust range.
    let jitter = rng.random_range(0..1000);

    let total = delay.saturating_add(jitter);
    Duration::from_millis(total.min(max_ms))
}

/// Execute an async operation with retries using exponential backoff and jitter.
pub async fn retry_async<T, E, F, Fut, S>(
    operation_name: S,
    settings: RetrySettings,
    mut operation: F,
) -> std::result::Result<T, E>
where
    S: Into<String> + Send,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = std::result::Result<T, E>> + Send,
    T: Send,
    E: std::fmt::Display + Send,
{
    let operation_name = operation_name.into();
    let mut attempt = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempt += 1;
                if attempt >= settings.max_attempts as usize {
                    error!(
                        "Failed to execute '{}' after {} attempts: {}",
                        operation_name, settings.max_attempts, e
                    );
                    return Err(e);
                }
                let delay = {
                    let mut rng = rand::rng();
                    next_retry_delay_with_rng(
                        &mut rng,
                        attempt,
                        settings.base_delay_ms,
                        settings.max_delay_ms,
                    )
                };
                warn!(
                    "Operation '{}' failed. Retrying in {:?} (Attempt {}/{}): {}",
                    operation_name, delay, attempt, settings.max_attempts, e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_retry_delay_exponential() {
        let base = 100;
        let max = 5000;

        // Attempt 1: 100 * 2^0 = 100 + [0, 1000]
        let d1 = next_retry_delay(1, base, max).as_millis() as u64;
        assert!((100..=1100).contains(&d1));

        // Attempt 2: 100 * 2^1 = 200 + [0, 1000]
        let d2 = next_retry_delay(2, base, max).as_millis() as u64;
        assert!((200..=1200).contains(&d2));

        // Attempt 3: 100 * 2^2 = 400 + [0, 1000]
        let d3 = next_retry_delay(3, base, max).as_millis() as u64;
        assert!((400..=1400).contains(&d3));
    }

    #[test]
    fn test_next_retry_delay_max() {
        let base = 100;
        let max = 500;
        let d10 = next_retry_delay(10, base, max).as_millis() as u64;
        assert_eq!(d10, 500);
    }

    #[tokio::test]
    async fn test_retry_async_success() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let settings = RetrySettings {
            max_attempts: 3,
            base_delay_ms: 1,
            max_delay_ms: 10,
        };
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = count.clone();

        let res: Result<i32, &str> = retry_async("test", settings, move || {
            let c = count_clone.clone();
            async move {
                let current = c.fetch_add(1, Ordering::SeqCst) + 1;
                if current < 2 { Err("fail") } else { Ok(42) }
            }
        })
        .await;

        assert_eq!(res, Ok(42));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_async_exhaustion() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let settings = RetrySettings {
            max_attempts: 2,
            base_delay_ms: 1,
            max_delay_ms: 10,
        };
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = count.clone();

        let res: Result<i32, &str> = retry_async("test", settings, move || {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err("fail")
            }
        })
        .await;

        assert!(res.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
