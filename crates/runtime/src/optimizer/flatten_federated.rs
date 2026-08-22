use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_federation::FederatedPlanNode;

/// Optimizer rule to flatten nested `FederatedPlanNode`s.
///
/// This rule detects if a `Federated` node contains other `Federated` nodes
/// within its subtree. If so, it unwraps the inner `Federated` extension nodes
/// so that the outer `Federated` node holds a clean plan for SQL generation.
#[derive(Default, Debug)]
pub struct FlattenFederatedNodesRule {}

impl FlattenFederatedNodesRule {
    /// Create a new flattening rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FlattenFederatedNodesRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&|node| {
            if let LogicalPlan::Extension(ref ext) = node
                && let Some(fed_node) = ext.node.as_any().downcast_ref::<FederatedPlanNode>()
            {
                let inner = fed_node.plan();

                // Check if inner contains nested FederatedPlanNodes
                let mut has_nested_fed = false;
                let _ = inner.apply(|n| {
                    if let LogicalPlan::Extension(e) = n
                        && e.node.as_any().is::<FederatedPlanNode>()
                    {
                        has_nested_fed = true;
                    }
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                });

                if has_nested_fed {
                    // Unwrap the inner federated extension wrapper nodes to clean up the plan,
                    // keeping the outer FederatedPlanNode intact as the single pushdown root.
                    let cleaned_inner = inner.clone().transform_down(&|n| {
                        if let LogicalPlan::Extension(ref e) = n
                            && let Some(inner_fed) =
                                e.node.as_any().downcast_ref::<FederatedPlanNode>()
                        {
                            return Ok(Transformed::yes(inner_fed.plan().clone()));
                        }
                        Ok(Transformed::no(n))
                    })?;

                    let new_fed =
                        FederatedPlanNode::new(cleaned_inner.data, fed_node.planner.clone());
                    return Ok(Transformed::yes(LogicalPlan::Extension(
                        datafusion::logical_expr::Extension {
                            node: std::sync::Arc::new(new_fed),
                        },
                    )));
                }
            }
            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str {
        "flatten_federated_nodes"
    }
}
