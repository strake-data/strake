//! # Predicate Cache
//!
//! Query-driven secondary indexing for accelerating repetitive scans.
//!
//! ## Overview
//!
//! This module provides the core data structures for caching predicate
//! evaluation at the Parquet row-group level. It uses a multi-level cache
//! (in-memory and persistent) to store the results of expensive predicate
//! pushdowns.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::predicate_cache::PredicateCache;
//! let cache = PredicateCache::new();
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Lookup**: O(1) in-memory lookup via `dashmap`. Persistent lookups depend
//!   on the underlying storage (e.g., RocksDB or local filesystem).
//! - **Concurrency**: Thread-safe implementation using lock-free or fine-grained
//!   locking primitives.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! Errors during cache persistence are reported as `DataFusionError::External`.
//!

pub mod key;
pub mod metrics;
pub mod store;

pub use key::{BlockKey, PredicateKey};
pub use metrics::PredicateCacheMetrics;
pub use store::PredicateCache;
