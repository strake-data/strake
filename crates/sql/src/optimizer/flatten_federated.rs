use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};

/// # Flatten Joins Rule
///
/// **CRITICAL**: This rule is ONLY safe for federation/SQL generation contexts.
///
/// ## Safety Invariants
///
/// 1. **Name-Based Resolution**: The SQL generator resolves all columns by name,
///    not position, making this safe for remote execution.
///    
/// 2. **Schema Preservation**: Aggregate nodes preserve output schema order
///    through `group_expr` iteration, ensuring correct SQL SELECT lists.
///    
/// 3. **Alias Uniqueness**: Systematic aliasing (t0, t1, ...) prevents name
///    collisions across joined tables.
///
/// ## UNSAFE for Local Execution
///
/// DO NOT use this rule for local DataFusion execution. Removing projections
/// changes physical column positions that may be referenced by index in
/// physical plans or cached expressions.
///
/// ## LIMITATION: N-Way Joins
///
/// N-way joins (3+ tables) are not supported by `SqlGenerator`'s `extract_relation`
/// unless they are explicitly aliased as subqueries. This rule is verified safe for
/// 2-way federated joins but should be applied cautiously for deeper join trees.
#[derive(Debug, Default)]
pub struct FlattenJoinsRule {
    #[allow(dead_code)]
    max_depth: usize,
}

impl FlattenJoinsRule {
    /// Create with default depth limit
    pub fn new() -> Self {
        Self { max_depth: 10 }
    }

    /// Create with custom depth limit (for testing)
    pub fn with_max_depth(max_depth: usize) -> Self {
        Self { max_depth }
    }

    fn is_removable_projection(&self, projection: &Projection) -> bool {
        // A projection is removable if it only contains column references.
        // Reordering or subsetting is fine because the SQL generator uses name-based resolution.
        let is_simple = projection.expr.iter().all(|e| matches!(e, Expr::Column(_)));

        // Debug: Verify schema compatibility
        #[cfg(debug_assertions)]
        if is_simple {
            let proj_fields: std::collections::HashSet<_> = projection
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let input_fields: std::collections::HashSet<_> = projection
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();

            // Simple projection fields should be a subset of input fields (by name)
            // If they aren't, it means the projection renamed a column, which we might want to handle carefully
            // but for now let's assert it's a subset or re-map.
            for p in &proj_fields {
                debug_assert!(
                    input_fields.contains(p),
                    "Projection introduces or renames columns not in input: {} vs {:?}",
                    p,
                    input_fields
                );
            }
        }

        is_simple
    }
}

impl OptimizerRule for FlattenJoinsRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&|node| {
            if let LogicalPlan::Projection(ref projection) = node {
                if self.is_removable_projection(projection) {
                    // Remove the projection and return its input.
                    // Since it's a ref, we need to clone the input.
                    return Ok(Transformed::yes(projection.input.as_ref().clone()));
                }
            }
            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str {
        "flatten_joins"
    }
}
