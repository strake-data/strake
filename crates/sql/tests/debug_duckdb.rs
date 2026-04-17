//! Tests for DuckDB SQL generation.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::JoinType;
use datafusion::prelude::*;
use std::sync::Arc;
use strake_sql::sql_gen::get_sql_for_plan;

#[tokio::test]
async fn debug_duckdb_sql_gen() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("user_id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));

    ctx.register_table(
        "users",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema1)),
    )?;
    ctx.register_table(
        "orders",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema2)),
    )?;

    let plan = ctx
        .table("users")
        .await?
        .alias("u")?
        .join(
            ctx.table("orders").await?.alias("o")?,
            JoinType::Inner,
            &["id"],
            &["user_id"],
            None,
        )?
        .aggregate(vec![col("u.name")], vec![sum(col("amount"))])?
        .into_optimized_plan()?;

    println!("Plan Display:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "duckdb")
        .expect("SQL generation failed")
        .expect("No SQL generated");

    println!("Generated SQL:\n{}", sql);

    Ok(())
}

#[tokio::test]
async fn test_federated_join_agg() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let schema_u = Arc::new(Schema::new(vec![
        Field::new("user_id_pk", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let schema_o = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("user_id_fk", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let schema_l = Arc::new(Schema::new(vec![
        Field::new("user_id_loc", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    ctx.register_table(
        "users",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema_u)),
    )?;
    ctx.register_table(
        "orders",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema_o)),
    )?;
    ctx.register_table(
        "locations",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema_l)),
    )?;

    // Build the plan using DataFrame API
    let plan = ctx
        .table("users")
        .await?
        .join(
            ctx.table("orders").await?,
            JoinType::Inner,
            &["user_id_pk"],
            &["user_id_fk"],
            None,
        )?
        .join(
            ctx.table("locations").await?,
            JoinType::Inner,
            &["user_id_pk"],
            &["user_id_loc"],
            None,
        )?
        .aggregate(vec![col("name"), col("city")], vec![sum(col("amount"))])?
        .into_optimized_plan()?;

    println!("Complex Plan:\n{}", plan.display_indent());
    let sql = get_sql_for_plan(&plan, "duckdb")
        .expect("SQL generation failed")
        .expect("No SQL generated");
    println!("Complex SQL:\n{}", sql);

    // Verify that the SUM argument is wrapped in a CAST for DuckDB
    assert!(sql.contains("SUM(CAST("));
    assert!(sql.contains("AS DOUBLE"));

    Ok(())
}

#[tokio::test]
async fn test_simple_agg() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    ctx.register_table(
        "orders",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;

    let plan = ctx
        .sql("SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id ORDER BY user_id")
        .await?
        .into_optimized_plan()?;

    println!("Simple Agg Plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "duckdb")
        .expect("SQL generation failed")
        .expect("No SQL generated");

    println!("Simple Agg SQL:\n{}", sql);

    Ok(())
}
#[tokio::test]
async fn test_sort_projection_agg() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    ctx.register_table(
        "users",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;

    let sql_plan = "SELECT name, city, SUM(amount) as total FROM users GROUP BY name, city ORDER BY total DESC LIMIT 5";
    let plan = ctx.sql(sql_plan).await?.into_optimized_plan()?;

    println!("Sort-Proj-Agg Plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "duckdb")
        .expect("SQL generation failed")
        .expect("No SQL generated");

    println!("Sort-Proj-Agg SQL:\n{}", sql);

    // Check for the alias mismatch
    // Expected: ... AS "rel_X" ORDER BY "rel_X"."total"
    // Failing symptom: ... AS "rel_X" ORDER BY "rel_Y"."total"

    // We can't easily predict X and Y, but we can check if the last AS alias matches the ORDER BY alias.
    Ok(())
}
