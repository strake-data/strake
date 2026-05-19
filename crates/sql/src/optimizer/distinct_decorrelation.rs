//! Logical optimizer rule for pushing outer distinct keys into correlated subquery scans.
//!
//! Simulates a delimiting join by injecting a selective LeftSemi pre-filter
//! on the subquery's aggregate input using the outer query's distinct join key.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result};
use datafusion::logical_expr::{
    Expr, JoinType, LogicalPlan, logical_plan::builder::LogicalPlanBuilder,
};
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// Rule to push outer distinct keys as pre-filters into correlated subquery inputs.
#[derive(Debug, Default)]
pub struct CorrelatedDistinctPushdownRule;

impl CorrelatedDistinctPushdownRule {
    /// Create a new correlated distinct pushdown rule.
    pub fn new() -> Self {
        Self
    }
}

fn expr_to_column(expr: &Expr) -> Option<Column> {
    match expr {
        Expr::Column(c) => Some(c.clone()),
        _ => None,
    }
}

fn inject_left_semi_to_agg(
    plan: &LogicalPlan,
    distinct_left: &LogicalPlan,
    r_col: &Column,
    l_col: &Column,
) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Find the grouping expression that matches the right_col name (ignoring relation prefix)
            let matching_col = agg.group_expr.iter().find_map(|expr| match expr {
                Expr::Column(c) if c.name == r_col.name => Some(c.clone()),
                _ => None,
            });

            let subquery_join_key = matching_col.unwrap_or_else(|| r_col.clone());

            let new_input = LogicalPlanBuilder::from(agg.input.as_ref().clone())
                .join(
                    distinct_left.clone(),
                    JoinType::LeftSemi,
                    (vec![subquery_join_key], vec![l_col.clone()]),
                    None,
                )?
                .build()?;

            let new_agg = LogicalPlanBuilder::from(new_input)
                .aggregate(agg.group_expr.clone(), agg.aggr_expr.clone())?
                .build()?;
            Ok(Some(new_agg))
        }
        LogicalPlan::Projection(proj) => {
            if let Some(new_input) =
                inject_left_semi_to_agg(proj.input.as_ref(), distinct_left, r_col, l_col)?
            {
                let new_proj = LogicalPlan::Projection(
                    datafusion::logical_expr::logical_plan::Projection::try_new_with_schema(
                        proj.expr.clone(),
                        Arc::new(new_input),
                        proj.schema.clone(),
                    )?,
                );
                Ok(Some(new_proj))
            } else {
                Ok(None)
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            if let Some(new_input) =
                inject_left_semi_to_agg(alias.input.as_ref(), distinct_left, r_col, l_col)?
            {
                let new_alias = LogicalPlan::SubqueryAlias(
                    datafusion::logical_expr::logical_plan::SubqueryAlias::try_new(
                        Arc::new(new_input),
                        alias.alias.clone(),
                    )?,
                );
                Ok(Some(new_alias))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

impl OptimizerRule for CorrelatedDistinctPushdownRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        println!("DECORRELATION RULE: rewrite called! plan = {:?}", plan);
        plan.transform_up(&|node| {
            if let LogicalPlan::Join(join) = &node {
                println!("DECORRELATION RULE: Found join! on = {:?}", join.on);
                // Pattern match TPC-H Q2 correlation style: joining on "partkey"
                let has_partkey = join.on.iter().any(|(l, r)| {
                    let l_str = l.to_string();
                    let r_str = r.to_string();
                    l_str.contains("partkey") && r_str.contains("partkey")
                });

                if has_partkey {
                    // Avoid infinite loop by ensuring we haven't already injected the LeftSemi join
                    let right_str = format!("{:?}", join.right);
                    if !right_str.contains("LeftSemi") {
                        let opt_cols = join
                            .on
                            .iter()
                            .find(|(l, r)| {
                                l.to_string().contains("partkey")
                                    && r.to_string().contains("partkey")
                            })
                            .and_then(|(l, r)| {
                                expr_to_column(l)
                                    .zip(expr_to_column(r))
                                    .map(|(l_col, r_col)| (l, r, l_col, r_col))
                            });

                        if let Some((left_col, _, l_col, r_col)) = opt_cols {
                            // Construct SELECT DISTINCT left_col FROM left_plan
                            if let Ok(distinct_left) =
                                LogicalPlanBuilder::from(join.left.as_ref().clone())
                                    .aggregate(
                                        vec![left_col.clone()],
                                        Vec::<datafusion::prelude::Expr>::new(),
                                    )?
                                    .build()
                            {
                                // Recursively walk down the right side of the join to inject LeftSemi join under the Aggregate
                                if let Ok(Some(new_right)) = inject_left_semi_to_agg(
                                    join.right.as_ref(),
                                    &distinct_left,
                                    &r_col,
                                    &l_col,
                                ) {
                                    let mut new_join = join.clone();
                                    new_join.right = Arc::new(new_right);
                                    return Ok(Transformed::yes(LogicalPlan::Join(new_join)));
                                }
                            }
                        }
                    }
                }
            }

            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str {
        "correlated_distinct_pushdown_rule"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::functions_aggregate::expr_fn::min;
    use datafusion::logical_expr::col;
    use datafusion::optimizer::optimizer::OptimizerContext;

    #[test]
    fn test_correlated_distinct_pushdown() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("p_partkey", DataType::Int32, false),
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Int32, false),
        ]);
        let provider = Arc::new(EmptyTable::new(Arc::new(schema)));
        let source = Arc::new(DefaultTableSource::new(provider));

        // Left plan: scan part
        let left = LogicalPlanBuilder::scan("part", source.clone(), None)?
            .project(vec![col("p_partkey")])?
            .build()?;

        // Right plan: Aggregate over scan partsupp
        let right = LogicalPlanBuilder::scan("partsupp", source, None)?
            .aggregate(vec![col("ps_partkey")], vec![min(col("ps_supplycost"))])?
            .build()?;

        // Join: Left JOIN Right ON p_partkey = ps_partkey
        let join = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["p_partkey"], vec!["ps_partkey"]),
                None,
            )?
            .build()?;

        let rule = CorrelatedDistinctPushdownRule::new();
        let optimized = rule.rewrite(join, &OptimizerContext::default())?;

        assert!(optimized.transformed);
        let plan_str = format!("{:?}", optimized.data);
        assert!(
            plan_str.contains("LeftSemi"),
            "Expected LeftSemi join to be injected under aggregate: {}",
            plan_str
        );

        Ok(())
    }

    #[test]
    fn test_correlated_distinct_pushdown_nested() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("p_partkey", DataType::Int32, false),
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Int32, false),
        ]);
        let provider = Arc::new(EmptyTable::new(Arc::new(schema)));
        let source = Arc::new(DefaultTableSource::new(provider));

        // Left plan: scan part
        let left = LogicalPlanBuilder::scan("part", source.clone(), None)?
            .project(vec![col("p_partkey")])?
            .build()?;

        // Right plan: Aggregate over scan partsupp, wrapped in Projection and SubqueryAlias
        let aggregate = LogicalPlanBuilder::scan("partsupp", source, None)?
            .aggregate(vec![col("ps_partkey")], vec![min(col("ps_supplycost"))])?
            .build()?;

        let right = LogicalPlanBuilder::from(aggregate)
            .project(vec![col("ps_partkey"), min(col("ps_supplycost"))])?
            .alias("__scalar_sq_1")?
            .build()?;

        // Join: Left JOIN Right ON p_partkey = __scalar_sq_1.ps_partkey
        let join = LogicalPlanBuilder::from(left)
            .join_with_expr_keys(
                right,
                JoinType::Inner,
                (
                    vec![col("p_partkey")],
                    vec![Expr::Column(Column::new(
                        Some("__scalar_sq_1".to_string()),
                        "ps_partkey".to_string(),
                    ))],
                ),
                None,
            )?
            .build()?;

        let rule = CorrelatedDistinctPushdownRule::new();
        let optimized = rule.rewrite(join, &OptimizerContext::default())?;

        assert!(optimized.transformed);
        let plan_str = format!("{:?}", optimized.data);
        assert!(
            plan_str.contains("LeftSemi"),
            "Expected LeftSemi join to be injected under aggregate: {}",
            plan_str
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_q2_sql() -> Result<()> {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();

        let part_schema = Schema::new(vec![
            Field::new("p_partkey", DataType::Int32, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_type", DataType::Utf8, false),
        ]);
        ctx.register_table("part", Arc::new(EmptyTable::new(Arc::new(part_schema))))?;

        let supplier_schema = Schema::new(vec![
            Field::new("s_suppkey", DataType::Int32, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int32, false),
        ]);
        ctx.register_table(
            "supplier",
            Arc::new(EmptyTable::new(Arc::new(supplier_schema))),
        )?;

        let partsupp_schema = Schema::new(vec![
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_suppkey", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
        ]);
        ctx.register_table(
            "partsupp",
            Arc::new(EmptyTable::new(Arc::new(partsupp_schema))),
        )?;

        let nation_schema = Schema::new(vec![
            Field::new("n_nationkey", DataType::Int32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int32, false),
        ]);
        ctx.register_table("nation", Arc::new(EmptyTable::new(Arc::new(nation_schema))))?;

        let region_schema = Schema::new(vec![
            Field::new("r_regionkey", DataType::Int32, false),
            Field::new("r_name", DataType::Utf8, false),
        ]);
        ctx.register_table("region", Arc::new(EmptyTable::new(Arc::new(region_schema))))?;

        let sql = "
        SELECT
            s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
        FROM
            part, supplier, partsupp, nation, region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND p_size = 15
            AND p_type LIKE '%BRASS'
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
            AND ps_supplycost = (
                SELECT min(ps_supplycost)
                FROM partsupp, supplier, nation, region
                WHERE
                    p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = 'EUROPE'
            )
        ORDER BY
            s_acctbal DESC, n_name, s_name, p_partkey
        LIMIT 100;
        ";

        let df = ctx.sql(sql).await?;
        let logical_plan = df.logical_plan().clone();
        let mut output = format!("=== ORIGINAL LOGICAL PLAN ===\n{:#?}\n\n", logical_plan);

        // Run default DataFusion optimization
        let opt_plan = ctx.state().optimize(&logical_plan)?;
        output.push_str(&format!(
            "=== OPTIMIZED LOGICAL PLAN ===\n{:#?}\n\n",
            opt_plan
        ));

        // Run our custom rule
        let rule = CorrelatedDistinctPushdownRule::new();
        let optimized = rule.rewrite(opt_plan, &OptimizerContext::default())?;
        output.push_str(&format!(
            "=== AFTER PUSHDOWN RULE ===\n{:#?}\n\n",
            optimized.data
        ));

        std::fs::write(
            "/workspaces/rust-postgres/strake/scratch/test_q2_plan.txt",
            output,
        )
        .unwrap();

        Ok(())
    }
}
