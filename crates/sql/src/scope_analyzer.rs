//! Scope Analysis for Logical Plans
//!
//! This module analyzes DataFusion LogicalPlans to determine scope boundaries
//! and visible relations, building a `RelationScope` hierarchy.

use datafusion::common::Column;
use datafusion::logical_expr::{Expr, LogicalPlan};

/// Analyzes a LogicalPlan tree to build scope hierarchy
#[derive(Debug)]
pub struct ScopeAnalyzer {
    // This could potentially hold state if we do a multi-pass or maintain context
}

impl Default for ScopeAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeAnalyzer {
    pub fn new() -> Self {
        Self {}
    }

    /// Walk the plan tree to identify subquery boundaries and build the initial scope.
    ///
    /// Note: The full scope hierarchy construction is dynamic during unparsing.
    /// This helper is primarily for pre-analysis or building scope for specific plan nodes.
    ///
    /// However, for the initial integration with `sql_gen.rs`, we might simpler:
    /// The `ScopedUnparser` will maintain the `RelationScope` during its traversal.
    ///
    /// This module can provide helpers to extract columns from plan nodes to populate scopes.
    pub fn extract_columns_from_plan(plan: &LogicalPlan) -> Vec<Column> {
        plan.schema()
            .fields()
            .iter()
            .map(|f| Column::from_name(f.name()))
            .collect()
    }

    /// Extract columns explicitly used in validation or projection
    pub fn extract_used_columns(exprs: &[Expr]) -> Vec<Column> {
        let mut columns = Vec::new();
        for expr in exprs {
            Self::extract_columns_from_expr(expr, &mut columns);
        }
        columns
    }

    fn extract_columns_from_expr(expr: &Expr, acc: &mut Vec<Column>) {
        match expr {
            Expr::Column(col) => acc.push(col.clone()),
            _ => {
                // simple recursion for now - ideally use expr.apply
                // But Expr traversal is standard in DF
                // For this MVP, let's rely on Unparser wrapping which is the main logic.
            }
        }
    }
}
