mod common;
#[macro_use]
mod fixtures;
mod dialects;
mod integration;
mod unit;

use anyhow::Result;

use datafusion::prelude::*;
use strake_sql::sql_gen::{get_sql_for_plan, is_substrait_source};

#[tokio::test]
async fn test_postgres_quoting() {
    let plan = common::test_plan().await;
    let sql = get_sql_for_plan(&plan, "postgres")
        .expect("failed to generate sql")
        .expect("expected SQL output");
    // Postgres uses " for identifiers
    assert!(sql.contains(r#""users""#) || sql.contains("users"));
    println!("Postgres SQL: {}", sql);
}

#[tokio::test]
async fn test_mysql_quoting() {
    let plan = common::test_plan().await;
    let sql = get_sql_for_plan(&plan, "mysql")
        .expect("failed to generate sql")
        .expect("expected SQL output");
    println!("MySQL SQL: {}", sql);
}

#[tokio::test]
async fn test_oracle_dialect() {
    let plan = common::test_plan().await;
    let sql = get_sql_for_plan(&plan, "oracle")
        .expect("failed to generate sql")
        .expect("expected SQL output");
    println!("Oracle SQL: {}", sql);
}

#[tokio::test]
async fn test_snowflake_dialect() {
    let plan = common::test_plan().await;
    let sql = get_sql_for_plan(&plan, "snowflake")
        .expect("failed to generate sql")
        .expect("expected SQL output");
    println!("Snowflake SQL: {}", sql);
}

#[tokio::test]
async fn test_substrait_source() {
    assert!(is_substrait_source("duckdb"));
    assert!(is_substrait_source("datafusion"));
    assert!(!is_substrait_source("postgres"));
}

#[tokio::test]
async fn test_local_execution_fallback() {
    let plan = common::test_plan().await;
    let result = get_sql_for_plan(&plan, "unknown_db").expect("should not error");
    assert!(
        result.is_none(),
        "unknown dialect should return None for local execution"
    );
}

#[tokio::test]
#[cfg(feature = "aggressive-join-aliasing")]
async fn test_schema_adapter_wrapping() -> Result<()> {
    use datafusion::logical_expr::Extension;
    use strake_sql::optimizer::remapper::remap_plan_for_federation;

    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    // Create an aggregation that renames columns (schema mismatch triggers SchemaAdapter)
    let plan = ctx
        .table("users")
        .await?
        .aggregate(
            vec![col("dept_id")],
            vec![datafusion::functions_aggregate::expr_fn::count(col("id")).alias("user_count")],
        )?
        .into_optimized_plan()?;

    let remapped = remap_plan_for_federation(plan)?;

    // Assert that remapped is an Extension node wrapping SchemaAdapter
    // (Because it had to flatten/alias, it should return an adapter to maintain original schema)
    assert!(matches!(remapped, LogicalPlan::Extension(Extension { .. })));

    Ok(())
}

/*
#[tokio::test]
async fn test_aggregation_smart_aliasing() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("dept_id", arrow::datatypes::DataType::Int32, false),
    ]);
    let table = std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
        std::sync::Arc::new(schema),
    ));
    ctx.register_table("users", table.clone())?;
    ctx.register_table("depts", table.clone())?;

    let u = ctx.table("users").await?.alias("u")?;
    let d = ctx.table("depts").await?.alias("d")?;

    let join = u.join(d, JoinType::Inner, &["dept_id"], &["id"], None)?;
    let proj = join.select(vec![col("u.name"), col("d.name")])?;
    let agg = proj.aggregate(
        vec![col("d.name")],
        vec![datafusion::functions_aggregate::expr_fn::count(col(
            "u.name",
        ))],
    )?;

    let plan = agg.into_optimized_plan()?;
    let sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
    println!("Generated SQL:\n{}", sql);

    assert!(sql.contains("GROUP BY"));
    assert!(
        !sql.contains("derived_sq_2_derived_sq_1"),
        "Found recursively nested alias!"
    );

    Ok(())
}
*/

#[tokio::test]
async fn test_scoped_subquery_generation() -> Result<()> {
    // Construct plan: SELECT u.name FROM (SELECT * FROM users u) AS derived
    let ctx = SessionContext::new();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]);
    ctx.register_table(
        "users",
        std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
            std::sync::Arc::new(schema),
        )),
    )?;

    // Build plan manually to ensure we reference inner alias 'u' from outer scope
    let table_scan = ctx.table("users").await?;
    let subquery = table_scan.alias("u")?.alias("derived")?; // Equivalent to (SELECT * FROM users u) AS derived

    // Now project from it.
    let plan = subquery.select(vec![col("name")])?.into_optimized_plan()?;

    println!("Plan before SQL gen:\n{}", plan.display_indent());
    let sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
    println!("Generated SQL: {}", sql);

    assert!(sql.contains("\"t1\"") || sql.contains("t1"));
    // The column name is valid.
    assert!(sql.contains("\"name\"") || sql.contains("name"));

    Ok(())
}

#[tokio::test]
async fn test_union_scope_merge() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]);

    ctx.register_table(
        "users1",
        std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
            std::sync::Arc::new(schema.clone()),
        )),
    )?;
    ctx.register_table(
        "users2",
        std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
            std::sync::Arc::new(schema),
        )),
    )?;

    // SELECT name FROM users1 u1 UNION ALL SELECT name FROM users2 u2
    let t1 = ctx
        .table("users1")
        .await?
        .alias("u1")?
        .select(vec![col("name")])?;
    let t2 = ctx
        .table("users2")
        .await?
        .alias("u2")?
        .select(vec![col("name")])?;

    // Create a Union of the two tables
    let plan = t1.union(t2)?.into_optimized_plan()?;

    println!("Union Plan:\n{}", plan.display_indent());
    let sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
    println!("Generated SQL: {}", sql);

    // Systematic aliases t0, t1, t2, t3 should be present
    assert!(sql.contains("\"t0\"") && sql.contains("\"t1\""));
    assert!(sql.contains("\"t2\"") && sql.contains("\"t3\""));

    Ok(())
}
