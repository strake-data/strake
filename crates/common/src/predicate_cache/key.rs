//! # Predicate Cache Keys
//!
//! Cache keys for content-addressed predicate results.
//!
//! ## Overview
//!
//! This module defines [`BlockKey`] for identifying specific Parquet row groups
//! and [`PredicateKey`] for identifying normalized query predicates.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::predicate_cache::key::BlockKey;
//! # use std::sync::Arc;
//! let key = BlockKey::new(12345, Arc::from("data.parquet"), 0);
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Hashing**: Keys implement `Hash` and `Eq` for efficient O(1) lookup
//!   in hash maps. `snapshot_id` prevents key collisions after table updates.
//! - **Memory**: Uses `Arc<str>` for the file path to minimize allocations.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! This module does not return errors.
//!

use datafusion::logical_expr::Expr;
use std::sync::Arc;

/// Content-addressed identity for a single Parquet row group.
///
/// `snapshot_id` ensures entries are never reused after a table write.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct BlockKey {
    /// Unique snapshot ID for the table.
    pub snapshot_id: i64,
    /// Absolute or relative path to the Parquet file.
    pub file_path: Arc<str>,
    /// Index of the row group within the Parquet file.
    pub row_group_index: usize,
}

impl BlockKey {
    /// Creates a new `BlockKey`.
    pub fn new(snapshot_id: i64, file_path: Arc<str>, row_group_index: usize) -> Self {
        Self {
            snapshot_id,
            file_path,
            row_group_index,
        }
    }
}

/// Unique identifier for a normalized predicate within a partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct PredicateKey {
    /// Name of the table the predicate is applied to.
    pub table_name: String,
    /// String representation of the normalized predicate expression.
    pub expr_display: String,
    /// Partition index the predicate is executed on.
    pub partition: usize,
}

impl PredicateKey {
    /// Creates a new `PredicateKey` from a table name and DataFusion expression.
    pub fn new(table_name: &str, expr: &Expr, partition: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            expr_display: format!("{expr:?}"),
            partition,
        }
    }
}
