use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for predicate caching.
#[derive(Debug, Default)]
pub struct PredicateCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    rows_skipped: AtomicU64,
}

impl PredicateCacheMetrics {
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_skips(&self, rows: u64) {
        self.rows_skipped.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn rows_skipped(&self) -> u64 {
        self.rows_skipped.load(Ordering::Relaxed)
    }
}
