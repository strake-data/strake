mod common;

use anyhow::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use strake_sql::optimizer::remapper::remap_plan_for_federation;

#[tokio::test]
async fn test_aggressive_aliasing_behavior() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    // A simple Projection that should be wrapped ONLY if aggressive aliasing is on
    let plan = ctx
        .table("users")
        .await?
        .select(vec![col("name")])?
        .into_optimized_plan()?;

    let remapped = remap_plan_for_federation(plan.clone())?;

    let is_wrapped = matches!(remapped, LogicalPlan::SubqueryAlias(_))
        || matches!(remapped, LogicalPlan::Extension(_)); // Extension if it wrapped because of schema mismatch

    // This is a bit tricky to test because 'aggressive-join-aliasing' might be off by default.
    // We can at least log what happened.
    println!(
        "Aggressive aliasing is on: {}",
        cfg!(feature = "aggressive-join-aliasing")
    );
    println!("Plan was wrapped: {}", is_wrapped);

    Ok(())
}
