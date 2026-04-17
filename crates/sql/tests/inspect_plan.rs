//! Tests for plan inspection and normalization.

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;

#[tokio::test]
async fn inspect_plan() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create two dummy tables
    ctx.sql("CREATE TABLE users (id INT, name TEXT)").await?;
    ctx.sql("CREATE TABLE orders (id INT, user_id INT, amount DOUBLE)")
        .await?;

    let plan = ctx.sql("SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name").await?.into_optimized_plan()?;

    println!("Original Plan Schema:");
    for (qualifier, field) in plan.schema().iter() {
        println!("Field name: '{}', qualifier: {:?}", field.name(), qualifier);
    }

    let normalized_plan = plan
        .transform_up(|node: LogicalPlan| {
            node.map_expressions(|expr: Expr| {
                expr.transform_down(|e: Expr| {
                    if let Expr::Column(mut col) = e {
                        if col
                            .relation
                            .as_ref()
                            .map(|r| r.to_string().contains('.'))
                            .unwrap_or(false)
                        {
                            col.relation = None;
                            Ok(Transformed::yes(Expr::Column(col)))
                        } else {
                            Ok(Transformed::no(Expr::Column(col)))
                        }
                    } else {
                        Ok(Transformed::no(e))
                    }
                })
            })
        })
        .data()?;

    println!("\nNormalized Plan Schema:");
    for (qualifier, field) in normalized_plan.schema().iter() {
        let name: &str = field.name();
        println!("Field name: '{}', qualifier: {:?}", name, qualifier);
    }

    Ok(())
}
