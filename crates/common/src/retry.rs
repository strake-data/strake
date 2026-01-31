use crate::config::RetrySettings;
use std::future::Future;
use std::time::Duration;
use tracing::{error, warn};

/// Calculate the delay for the next retry attempt with exponential backoff.
pub fn next_retry_delay(attempt: usize, base_ms: u64, max_ms: u64) -> Duration {
    let multiplier = 2_u64.saturating_pow(attempt as u32);
    let delay = base_ms.saturating_mul(multiplier);
    // Add jitter up to 1000ms
    let jitter = rand::random::<u64>() % 1000;
    let total = delay.saturating_add(jitter);
    Duration::from_millis(total.min(max_ms))
}

/// Execute an async operation with retries.
pub async fn retry_async<T, E, F, Fut>(
    operation_name: &str,
    settings: RetrySettings,
    operation: F,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
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
                let delay =
                    next_retry_delay(attempt, settings.base_delay_ms, settings.max_delay_ms);
                warn!(
                    "Operation '{}' failed. Retrying in {:?} (Attempt {}/{}): {}",
                    operation_name, delay, attempt, settings.max_attempts, e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}
