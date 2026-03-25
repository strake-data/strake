//! # Predicate Cache
//!
//! Query-driven secondary indexing for accelerating repetitive scans.
//! This module provides the core data structures for caching predicate
//! evaluation at the Parquet row-group level.

pub mod key;
pub mod metrics;
pub mod store;

pub use key::{BlockKey, PredicateKey};
pub use metrics::PredicateCacheMetrics;
pub use store::PredicateCache;
