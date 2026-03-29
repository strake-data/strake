//! # Predicate Cache Metrics
//!
//! Observability for predicate caching performance.
//!
//! ## Overview
//!
//! Defines [`PredicateCacheMetrics`] to track hits, misses, and the number of
//! rows skipped due to caching.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::predicate_cache::metrics::PredicateCacheMetrics;
//! let metrics = PredicateCacheMetrics::default();
//! metrics.record_hit();
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Wait-free**: Uses `AtomicU64` for all metric counters to ensure
//!   non-blocking updates even under high contention.
//! - **Relaxed Ordering**: Uses `Ordering::Relaxed` as strict consistency
//!   is not required for performance monitoring.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! This module does not return errors.
//!

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for predicate caching.
#[derive(Debug, Default)]
pub struct PredicateCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    rows_skipped: AtomicU64,
}

impl PredicateCacheMetrics {
    /// Records a cache hit.
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cache miss.
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the number of rows skipped due to a cache hit.
    pub fn record_skips(&self, rows: u64) {
        self.rows_skipped.fetch_add(rows, Ordering::Relaxed);
    }

    /// Returns the total number of cache hits.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Returns the total number of cache misses.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Returns the total number of rows skipped.
    pub fn rows_skipped(&self) -> u64 {
        self.rows_skipped.load(Ordering::Relaxed)
    }
}
