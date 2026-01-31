mod common;

use anyhow::Result;
use datafusion::common::TableReference;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::*;
use strake_sql::optimizer::remapper::{remap_plan_for_federation, PlanScopeRemapper};
use strake_sql::relation_scope::DerivedNameParser;

#[tokio::test]
async fn test_column_resolution_simple() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    let plan = ctx
        .table("users")
        .await?
        .select(vec![col("name")])?
        .into_optimized_plan()?;

    // Test that remapping preserves the schema
    let remapped = remap_plan_for_federation(plan.clone())?;

    assert_eq!(remapped.schema(), plan.schema());

    Ok(())
}

#[test]
fn test_derived_name_parsing() {
    // Basic parse
    let parsed = DerivedNameParser::parse("derived_sq_1_u_name").unwrap();
    assert_eq!(parsed.0, Some(TableReference::bare("u")));
    assert_eq!(parsed.1, "name");

    // Idempotency check: "derived_sq_2_derived_sq_1_u_name"
    // The current implementation parses from the last "derived_sq_" which is good.
    let parsed2 = DerivedNameParser::parse("derived_sq_2_derived_sq_1_u_name").unwrap();
    assert_eq!(parsed2.0, Some(TableReference::bare("u")));
    assert_eq!(parsed2.1, "name");
}

#[tokio::test]
async fn test_filter_remapping() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    // Create a plan that will be aliased (e.g. a join input if aggressive is on, or just force it)
    // We can test the remapper directly on a Filter node with a parent scope
    let table_scan = ctx
        .table("users")
        .await?
        .alias("u")?
        .into_optimized_plan()?;

    let remapper = PlanScopeRemapper::new();
    let (remapped_table, table_scope) = remapper.remap_plan(&table_scan, &None)?;

    // Construct a filter that uses "u.id" but the table was potentially aliased to "derived_sq_1"
    let filter_expr = col("u.id").gt(lit(10));
    let filter_plan = LogicalPlanBuilder::from(remapped_table)
        .filter(filter_expr)?
        .build()?;

    let (remapped_filter, _) = remapper.remap_plan(&filter_plan, &Some(Box::new(table_scope)))?;

    // The filter expression should now reference the new column name if aliasing happened
    let sql = strake_sql::sql_gen::get_sql_for_plan(&remapped_filter, "postgres")?.unwrap();

    // If it was aliased, it might look like "derived_sq_1"."users_u_id" > 10
    // At the very least, it shouldn't be "u"."id" if "u" is gone from scope
    println!("Filtered SQL: {}", sql);

    Ok(())
}

#[tokio::test]
async fn test_sort_remapping() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    let plan = ctx
        .table("users")
        .await?
        .alias("u")?
        .sort(vec![col("u.name").sort(true, true)])?
        .into_optimized_plan()?;

    let remapped = remap_plan_for_federation(plan)?;
    let sql = strake_sql::sql_gen::get_sql_for_plan(&remapped, "postgres")?.unwrap();

    println!("Sorted SQL: {}", sql);
    assert!(sql.contains("ORDER BY"));

    Ok(())
}

#[tokio::test]
async fn test_correlated_subquery_scope() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    // Test the remapper's ability to resolve columns from a parent scope
    // 1. Create a parent scope that includes 'u' with its columns
    let root_scope = strake_sql::relation_scope::RelationScope::new();
    let parent_scope = root_scope.push_subquery(
        "u".to_string(),
        vec![datafusion::common::Column::new(
            Some(TableReference::bare("u")),
            "id",
        )],
    );

    // 2. Test remapping of a correlated expression
    let inner_expr = col("u.id");
    let inner_table = ctx.table("depts").await?.into_optimized_plan()?;
    let (_, depts_scope) = PlanScopeRemapper::new().remap_plan(&inner_table, &None)?;
    let combined_scope = depts_scope.with_parent(Some(Box::new(parent_scope)));

    // 3. Remap the correlated expression directly
    let remapper = PlanScopeRemapper::new();
    let remapped_expr = remapper.remap_expr(&inner_expr, &combined_scope)?;

    // 4. Verify the remapped expression
    if let Expr::Column(c) = remapped_expr {
        assert_eq!(c.relation, Some(TableReference::bare("u")));
        assert_eq!(c.name, "id");
    } else {
        panic!("Expected column expression, got {:?}", remapped_expr);
    }

    println!("Correlated Expression remapped successfully");
    Ok(())
}

/*
#[tokio::test]
async fn test_left_join_null_handling() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    let u = ctx.table("users").await?.alias("u")?;
    let d = ctx.table("depts").await?.alias("d")?;

    let plan = u
        .join(d, JoinType::Left, &["dept_id"], &["id"], None)?
        .select(vec![col("u.name"), col("d.name")])?
        .into_optimized_plan()?;

    let remapped = remap_plan_for_federation(plan)?;
    let sql = strake_sql::sql_gen::get_sql_for_plan(&remapped, "postgres")?.unwrap();

    println!("Left Join SQL: {}", sql);
    assert!(sql.contains("LEFT JOIN") || sql.contains("LEFT OUTER JOIN"));

    Ok(())
}
*/
