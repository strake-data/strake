use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchemaRef, Result as DFResult};
use datafusion::error::Result;
use datafusion::logical_expr::{
    Expr, InvariantLevel, JoinType, LogicalPlan, UserDefinedLogicalNode,
};
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};

/// Represents a branch in an N-way join.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinBranch {
    pub input: LogicalPlan,
    pub join_type: JoinType,
    pub on: Vec<(Expr, Expr)>,
    pub filter: Option<Expr>,
}

/// A custom logical node that represents an N-way join (flattened join tree).
///
/// This node is used during SQL generation to produce cleaner SQL without
/// excessive subqueries for nested joins.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NaryJoinNode {
    pub base: LogicalPlan,
    pub branches: Vec<JoinBranch>,
    pub schema: DFSchemaRef,
}

impl NaryJoinNode {
    pub fn new(base: LogicalPlan, branches: Vec<JoinBranch>, schema: DFSchemaRef) -> Self {
        Self {
            base,
            branches,
            schema,
        }
    }
}

impl UserDefinedLogicalNode for NaryJoinNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "NaryJoin"
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        let mut inputs = vec![&self.base];
        for branch in &self.branches {
            inputs.push(&branch.input);
        }
        inputs
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::new();
        for branch in &self.branches {
            for (l, r) in &branch.on {
                exprs.push(l.clone());
                exprs.push(r.clone());
            }
            if let Some(f) = &branch.filter {
                exprs.push(f.clone());
            }
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "NaryJoin: base={}, branches={}",
            self.base.display(),
            self.branches.len()
        )
    }

    fn check_invariants(&self, _check: InvariantLevel) -> DFResult<()> {
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Arc<dyn UserDefinedLogicalNode>> {
        if inputs.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "NaryJoin requires at least one input".to_string(),
            ));
        }

        let mut input_iter = inputs.into_iter();
        let base = input_iter.next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("NaryJoin: missing base input".to_string())
        })?;

        let mut expr_iter = exprs.into_iter();
        let mut branches = Vec::new();

        for original_branch in &self.branches {
            let input = input_iter.next().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Not enough inputs for NaryJoin".to_string(),
                )
            })?;

            let mut on = Vec::new();
            for _ in 0..original_branch.on.len() {
                let l = expr_iter.next().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "NaryJoin: not enough expressions for ON clause".to_string(),
                    )
                })?;
                let r = expr_iter.next().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "NaryJoin: not enough expressions for ON clause".to_string(),
                    )
                })?;
                on.push((l, r));
            }

            let filter = if original_branch.filter.is_some() {
                Some(expr_iter.next().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "NaryJoin: not enough expressions for filter".to_string(),
                    )
                })?)
            } else {
                None
            };

            branches.push(JoinBranch {
                input,
                join_type: original_branch.join_type,
                on,
                filter,
            });
        }

        Ok(Arc::new(NaryJoinNode {
            base,
            branches,
            schema: self.schema.clone(),
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map(|o| self.schema.to_string().cmp(&o.schema.to_string()))
    }
}

/// Optimizer rule that flattens nested binary joins into `NaryJoinNode`.
#[derive(Debug, Default)]
pub struct JoinTreeFlattener;

impl JoinTreeFlattener {
    pub fn new() -> Self {
        Self
    }

    fn can_flatten(join_type: JoinType, parent_type: JoinType) -> bool {
        // We only flatten if we can maintain the semantics in a simple JOIN chain.
        // Inner joins are always flattenable.
        // Left joins are flattenable if they are left-deep.
        match (join_type, parent_type) {
            (JoinType::Inner, JoinType::Inner) => true,
            (JoinType::Left, JoinType::Left) => true,
            (JoinType::Inner, JoinType::Left) => true,
            _ => false, // Mixed or right/full joins are more complex
        }
    }
}

impl OptimizerRule for JoinTreeFlattener {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&|node| {
            if let LogicalPlan::Join(join) = &node {
                // Check if left is NaryJoin
                if let LogicalPlan::Extension(ext) = join.left.as_ref() {
                    if let Some(nary) = ext.node.as_any().downcast_ref::<NaryJoinNode>() {
                        let left_join_type = nary
                            .branches
                            .last()
                            .map(|b| b.join_type)
                            .unwrap_or(JoinType::Inner);

                        if Self::can_flatten(left_join_type, join.join_type) {
                            let mut branches = nary.branches.clone();
                            branches.push(JoinBranch {
                                input: join.right.as_ref().clone(),
                                join_type: join.join_type,
                                on: join.on.clone(),
                                filter: join.filter.clone(),
                            });

                            return Ok(Transformed::yes(LogicalPlan::Extension(
                                datafusion::logical_expr::Extension {
                                    node: Arc::new(NaryJoinNode {
                                        base: nary.base.clone(),
                                        branches,
                                        schema: join.schema.clone(),
                                    }),
                                },
                            )));
                        }
                    }
                }

                // Check if left is Join
                if let LogicalPlan::Join(left_join) = join.left.as_ref() {
                    if Self::can_flatten(left_join.join_type, join.join_type) {
                        let branches = vec![
                            JoinBranch {
                                input: left_join.right.as_ref().clone(),
                                join_type: left_join.join_type,
                                on: left_join.on.clone(),
                                filter: left_join.filter.clone(),
                            },
                            JoinBranch {
                                input: join.right.as_ref().clone(),
                                join_type: join.join_type,
                                on: join.on.clone(),
                                filter: join.filter.clone(),
                            },
                        ];

                        return Ok(Transformed::yes(LogicalPlan::Extension(
                            datafusion::logical_expr::Extension {
                                node: Arc::new(NaryJoinNode {
                                    base: left_join.left.as_ref().clone(),
                                    branches,
                                    schema: join.schema.clone(),
                                }),
                            },
                        )));
                    }
                }
            }
            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str {
        "join_tree_flattener"
    }
}
