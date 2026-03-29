//! # Predicate Cache Store
//!
//! Thread-safe storage for predicate match results.
//!
//! ## Overview
//!
//! This module provides the [`PredicateCache`] which uses a `DashMap` for
//! high-concurrency access to cached row-group results.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::predicate_cache::store::PredicateCache;
//! let cache = PredicateCache::new();
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Concurrency**: `DashMap` provides fine-grained locking, allowing
//!   concurrent reads and writes to different segments of the cache.
//! - **Scaling**: Performance scales linearly with the number of CPU cores
//!   for disjoint key sets.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! This module does not triggered errors directly, but underlying storage
//! failures in persistent implementations may do so.
//!

use crate::predicate_cache::{BlockKey, PredicateCacheMetrics};
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe in-memory cache for row-group predicate matches.
#[derive(Debug)]
pub struct PredicateCache {
    blocks: DashMap<BlockKey, bool>,
    /// Metrics for monitoring cache performance.
    pub metrics: Arc<PredicateCacheMetrics>,
}

impl PredicateCache {
    /// Creates a new, empty `PredicateCache`.
    pub fn new() -> Self {
        Self {
            blocks: DashMap::new(),
            metrics: Arc::new(PredicateCacheMetrics::default()),
        }
    }

    /// Returns the cached result for a specific block, if present.
    /// Records a hit/miss metric.
    pub fn get_block(&self, key: &BlockKey) -> Option<bool> {
        if let Some(entry) = self.blocks.get(key) {
            self.metrics.record_hit();
            Some(*entry)
        } else {
            self.metrics.record_miss();
            None
        }
    }

    /// Inserts a block match result into the cache.
    pub fn insert_block(&self, key: BlockKey, matched: bool) {
        self.blocks.insert(key, matched);
    }

    /// Returns true if any blocks are cached for the given snapshot ID.
    pub fn has_any_blocks_for_snapshot(&self, snapshot_id: i64) -> bool {
        self.blocks
            .iter()
            .any(|entry| entry.key().snapshot_id == snapshot_id)
    }

    /// Returns the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

impl Default for PredicateCache {
    fn default() -> Self {
        Self::new()
    }
}
