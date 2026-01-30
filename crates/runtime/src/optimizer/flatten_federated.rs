use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_federation::FederatedPlanNode;

/// Optimizer rule to flatten nested `FederatedPlanNode`s.
///
/// This rule detects if a `Federated` node contains other `Federated` or `SchemaAdapter` nodes
/// within its subtree. If so, it unwraps the outer `Federated` node. This prevents failures in
/// `Unparser` which cannot handle Extension nodes when generating SQL for the outer Federated node.
#[derive(Default, Debug)]
pub struct FlattenFederatedNodesRule {}

impl FlattenFederatedNodesRule {
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
            if let LogicalPlan::Extension(ref ext) = node {
                if let Some(fed_node) = ext.node.as_any().downcast_ref::<FederatedPlanNode>() {
                    let inner = fed_node.plan();

                    // Check deep nesting
                    let node_check = inner.exists(|n| {
                        if let LogicalPlan::Extension(e) = n {
                            if e.node.name() == "Federated" || e.node.name() == "SchemaAdapter" {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    });

                    match node_check {
                        Ok(true) => {
                            // Unwrap and return the inner plan
                            return Ok(Transformed::yes(inner.clone()));
                        }
                        Ok(false) => {
                            // Keep
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str {
        "flatten_federated_nodes"
    }
}
