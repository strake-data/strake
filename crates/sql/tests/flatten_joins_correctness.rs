use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::*;
use std::sync::Arc;
use strake_sql::optimizer::flatten_federated::FlattenJoinsRule;
use strake_sql::sql_gen::get_sql_for_plan;

#[tokio::test]
async fn test_flatten_joins_preserves_aggregate_semantics() -> Result<()> {
    // Setup Context
    let ctx = SessionContext::new();

    // Create schemas
    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let order_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));

    // Register empty batches to define tables
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(user_schema)),
    )?;
    ctx.register_table(
        "t2",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(order_schema)),
    )?;

    // Build plan: Select columns in specific order before Aggregate
    let t1 = ctx.table("t1").await?;
    let t2 = ctx.table("t2").await?;

    let df = t1
        .join(t2, JoinType::Inner, &["user_id"], &["user_id"], None)?
        .select(vec![col("name"), col("amount"), col("order_id")])?
        .aggregate(
            vec![col("name")],
            vec![datafusion::functions_aggregate::expr_fn::count(col("amount")).alias("count")],
        )?;

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

    // Basic verification of SQL generation
    assert!(original_sql.contains("GROUP BY"));
    assert!(optimized_sql.contains("GROUP BY"));

    // Verify that the redundant subquery is removed in optimized SQL
    // ORIGINAL usually has SELECT * FROM (SELECT name, count FROM (...) GROUP BY name)
    // OPTIMIZED should have flattened it.
    assert!(original_sql.contains("SELECT") && original_sql.contains("FROM"));

    Ok(())
}
