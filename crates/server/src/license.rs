use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LicenseState {
    Valid = 0,
    Degraded = 1,
    Invalid = 2,
}

pub struct LicenseCache {
    state: AtomicU8,
    last_check: AtomicU64,
}

impl Default for LicenseCache {
    fn default() -> Self {
        Self::new()
    }
}

impl LicenseCache {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(LicenseState::Valid as u8),
            last_check: AtomicU64::new(0),
        }
    }

    /// Hot path: nanosecond-scale atomic read, no locks
    pub fn current_state(&self) -> LicenseState {
        match self.state.load(Ordering::Acquire) {
            0 => LicenseState::Valid,
            1 => LicenseState::Degraded,
            _ => LicenseState::Invalid,
        }
    }

    pub fn update_state(&self, state: LicenseState) {
        self.state.store(state as u8, Ordering::Release);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_check.store(now, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
pub trait LicenseValidator: Send + Sync {
    async fn validate(&self) -> anyhow::Result<LicenseState>;
}

/// Spawns a background task to poll the validator and update the cache
pub fn spawn_license_monitor(
    validator: Arc<dyn LicenseValidator>,
    cache: Arc<LicenseCache>,
    check_interval: std::time::Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);

        // Immediate first check
        match validator.validate().await {
            Ok(state) => cache.update_state(state),
            Err(e) => tracing::error!("Initial license validation failed: {}", e),
        }

        loop {
            interval.tick().await;
            match validator.validate().await {
                Ok(state) => {
                    let prev = cache.current_state();
                    if prev != state {
                        tracing::info!("License state transition: {:?} -> {:?}", prev, state);
                        cache.update_state(state);
                    }
                }
                Err(e) => {
                    tracing::error!("License re-validation failed: {}", e);
                    // Keep using cached state
                }
            }
        }
    });
}
