use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::*;
use std::sync::Arc;
use strake_sql::optimizer::flatten_federated::FlattenJoinsRule;
use strake_sql::sql_gen::get_sql_for_plan;

#[tokio::test]
async fn test_join_column_collision() -> Result<()> {
    let ctx = SessionContext::new();

    // Both tables have "id" column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, false),
    ]));

    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(
            schema.clone(),
        )),
    )?;
    ctx.register_table(
        "t2",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;

    // SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id = t2.id
    let df = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )?
        .select(vec![col("t1.id"), col("t2.id")])?;

    let plan = df.into_optimized_plan()?;

    // Original plan: Projection(t1.id, t2.id) -> Join -> ...

    // Apply FlattenJoinsRule
    let config = OptimizerContext::default();
    let rule = FlattenJoinsRule::new();
    let optimized = rule.rewrite(plan.clone(), &config)?.data;

    // Generate SQL for both
    let original_sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
    let optimized_sql = get_sql_for_plan(&optimized, "postgres")?.expect("sql generated");

    println!("ORIGINAL SQL: {}", original_sql);
    println!("OPTIMIZED SQL: {}", optimized_sql);

    // Verify: Both should correctly distinguish between the two "id" columns
    // Postgres uses "t0"."id" and "t1"."id"
    assert!(optimized_sql.contains("\"t0\".\"id\"") || optimized_sql.contains("t0.id"));
    assert!(optimized_sql.contains("\"t1\".\"id\"") || optimized_sql.contains("t1.id"));

    Ok(())
}
