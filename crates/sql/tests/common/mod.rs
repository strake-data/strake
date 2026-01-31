use anyhow::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;

#[allow(dead_code)]
pub async fn test_plan() -> LogicalPlan {
    let ctx = SessionContext::new();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]);
    let _ = ctx.register_table(
        "users",
        std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
            std::sync::Arc::new(schema),
        )),
    );

    ctx.table("users")
        .await
        .unwrap()
        .filter(col("id").gt(lit(10)))
        .unwrap()
        .select(vec![col("name")])
        .unwrap()
        .into_optimized_plan()
        .unwrap()
}

#[allow(dead_code)]
pub fn make_test_schema() -> arrow::datatypes::Schema {
    arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("dept_id", arrow::datatypes::DataType::Int32, false),
    ])
}

#[allow(dead_code)]
pub async fn setup_ctx_with_tables(ctx: &SessionContext) -> Result<()> {
    let schema = make_test_schema();
    let table = std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
        std::sync::Arc::new(schema),
    ));
    ctx.register_table("users", table.clone())?;
    ctx.register_table("depts", table.clone())?;
    Ok(())
}
