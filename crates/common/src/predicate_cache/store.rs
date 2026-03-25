use crate::predicate_cache::{BlockKey, PredicateCacheMetrics};
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe in-memory cache for row-group predicate matches.
#[derive(Debug)]
pub struct PredicateCache {
    blocks: DashMap<BlockKey, bool>,
    pub metrics: Arc<PredicateCacheMetrics>,
}

impl PredicateCache {
    pub fn new() -> Self {
        Self {
            blocks: DashMap::new(),
            metrics: Arc::new(PredicateCacheMetrics::default()),
        }
    }

    pub fn get_block(&self, key: &BlockKey) -> Option<bool> {
        if let Some(entry) = self.blocks.get(key) {
            self.metrics.record_hit();
            Some(*entry)
        } else {
            self.metrics.record_miss();
            None
        }
    }

    pub fn insert_block(&self, key: BlockKey, matched: bool) {
        self.blocks.insert(key, matched);
    }

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
