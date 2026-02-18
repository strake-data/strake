use datafusion::common::tree_node::{Transformed, TreeNode};
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
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
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
        // 1. Check if we have aggregates anywhere in the tree.
        // If so, we don't inject a limit because it might return partial results for aggregates.
        if has_aggregate(&plan) {
            return Ok(Transformed::no(plan));
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

fn has_aggregate(plan: &LogicalPlan) -> bool {
    plan.exists(|node| {
        Ok(matches!(
            node,
            LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_)
        ))
    })
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::{col, logical_plan::builder::LogicalPlanBuilder};
    use datafusion::optimizer::optimizer::OptimizerContext;

    #[test]
    fn test_defensive_limit_skips_aggregate() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let table_provider = Arc::new(EmptyTable::new(Arc::new(schema)));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let plan = LogicalPlanBuilder::scan("t1", table_source, None)?
            .aggregate(vec![col("id")], vec![count(col("id"))])?
            .build()?;

        let rule = DefensiveLimitRule::new(100);
        let result = rule.rewrite(plan.clone(), &OptimizerContext::default())?;

        assert!(!result.transformed);
        assert_eq!(result.data, plan);
        Ok(())
    }

    #[test]
    fn test_defensive_limit_skips_nested_aggregate() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let table_provider = Arc::new(EmptyTable::new(Arc::new(schema)));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let plan = LogicalPlanBuilder::scan("t1", table_source, None)?
            .aggregate(vec![col("id")], vec![count(col("id"))])?
            .project(vec![col("id")])?
            .build()?;

        let rule = DefensiveLimitRule::new(100);
        let result = rule.rewrite(plan.clone(), &OptimizerContext::default())?;

        assert!(!result.transformed);
        assert_eq!(result.data, plan);
        Ok(())
    }

    #[test]
    fn test_defensive_limit_injects_on_simple_scan() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let table_provider = Arc::new(EmptyTable::new(Arc::new(schema)));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let plan = LogicalPlanBuilder::scan("t1", table_source, None)?.build()?;

        let rule = DefensiveLimitRule::new(100);
        let result = rule.rewrite(plan.clone(), &OptimizerContext::default())?;

        assert!(result.transformed);
        match result.data {
            LogicalPlan::Limit(limit) => {
                assert_eq!(limit.fetch, Some(Box::new(lit(100i64))));
            }
            _ => panic!("Expected Limit node"),
        }
        Ok(())
    }

    #[test]
    fn test_defensive_limit_skips_distinct() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let table_provider = Arc::new(EmptyTable::new(Arc::new(schema)));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let plan = LogicalPlanBuilder::scan("t1", table_source, None)?
            .distinct()?
            .build()?;

        let rule = DefensiveLimitRule::new(100);
        let result = rule.rewrite(plan.clone(), &OptimizerContext::default())?;

        assert!(!result.transformed);
        assert_eq!(result.data, plan);
        Ok(())
    }
}
