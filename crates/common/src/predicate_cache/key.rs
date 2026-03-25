use datafusion::logical_expr::Expr;
use std::sync::Arc;

/// Content-addressed identity for a single Parquet row group.
///
/// `snapshot_id` ensures entries are never reused after a table write.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct BlockKey {
    pub snapshot_id: i64,
    pub file_path: Arc<str>,
    pub row_group_index: usize,
}

impl BlockKey {
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
    pub table_name: String,
    pub expr_display: String,
    pub partition: usize,
}

impl PredicateKey {
    pub fn new(table_name: &str, expr: &Expr, partition: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            expr_display: format!("{expr:?}"),
            partition,
        }
    }
}
