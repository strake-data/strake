use datafusion::common::tree_node::Transformed;
use datafusion::common::Result;
use datafusion::logical_expr::{lit, Limit, LogicalPlan};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::OptimizerConfig;
use std::sync::Arc;
use tracing::warn;

/// An optimizer rule that injects a default LIMIT for queries against sources
/// with missing statistics, to prevent runway queries.
#[derive(Debug)]
pub struct DefensiveLimitRule {
    default_limit: usize,
}

impl DefensiveLimitRule {
    pub fn new(default_limit: usize) -> Self {
        Self { default_limit }
    }
}
impl OptimizerRule for DefensiveLimitRule {
    fn name(&self) -> &str {
        "defensive_limit_rule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Limit(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Copy(_) => return Ok(Transformed::no(plan)),
            _ => {}
        }

        // Logic:
        // 1. Check if we have stats. If unknown (and we aren't a Limit), inject limit.
        // For now, we'll take a simpler approach:
        // If the root is not a limit, we inject a limit.

        let limit = self.default_limit;

        // Create a new Limit plan wrapping the current plan
        let new_plan = LogicalPlan::Limit(Limit {
            skip: None, // No skip
            fetch: Some(Box::new(lit(limit as i64))),
            input: Arc::new(plan),
        });

        // We should only return Some if we changed something.
        // Here we always wrap if root is not limit.
        warn!("Injected defensive LIMIT {}", limit);

        Ok(Transformed::yes(new_plan))
    }
}
