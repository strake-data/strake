use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::*;
use std::sync::Arc;
use strake_sql::optimizer::flatten_federated::FlattenJoinsRule;
use strake_sql::sql_gen::get_sql_for_plan;

#[tokio::test]
async fn test_flatten_with_limit() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, false),
    ]));

    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;

    // SELECT id, val FROM t1 LIMIT 10
    // Plan: Limit(10) -> Projection(id, val) -> TableScan(t1)
    let df = ctx
        .table("t1")
        .await?
        .select(vec![col("id"), col("val")])?
        .limit(0, Some(10))?;

    let plan = df.into_optimized_plan()?;

    // Apply FlattenJoinsRule
    let config = OptimizerContext::default();
    let rule = FlattenJoinsRule::new();
    let optimized = rule.rewrite(plan.clone(), &config)?.data;

    // Generate SQL for both
    let original_sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
    let optimized_sql = get_sql_for_plan(&optimized, "postgres")?.expect("sql generated");

    println!("ORIGINAL SQL: {}", original_sql);
    println!("OPTIMIZED SQL: {}", optimized_sql);

    // Verify: LIMIT should be preserved
    assert!(optimized_sql.contains("LIMIT 10"));
    assert!(!optimized_sql.contains("SELECT * FROM (SELECT"));

    Ok(())
}
