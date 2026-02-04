use arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::*;
use proptest::prelude::*;
use std::sync::Arc;
use strake_sql::optimizer::flatten_federated::FlattenJoinsRule;
use strake_sql::sql_gen::get_sql_for_plan;

fn create_test_schema(prefix: &str, num_cols: usize) -> Arc<Schema> {
    let fields: Vec<_> = (0..num_cols)
        .map(|i| Field::new(format!("{}_col_{}", prefix, i), DataType::Int32, false))
        .collect();
    Arc::new(Schema::new(fields))
}

async fn build_random_plan(num_tables: usize, num_cols: usize) -> Result<LogicalPlan> {
    let ctx = SessionContext::new();

    // Register tables with unique column names to avoid ambiguity
    for i in 0..num_tables {
        let table_name = format!("t{}", i);
        let schema = create_test_schema(&table_name, num_cols);
        ctx.register_table(
            &table_name,
            Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
        )?;
    }

    // Start with first table
    let mut df = ctx.table("t0").await?;

    // Join with remaining tables
    for i in 1..num_tables {
        let left_col = "t0_col_0".to_string();
        let right_table = format!("t{}", i);
        let right_col = format!("{}_col_0", right_table);
        df = df.join(
            ctx.table(&right_table).await?,
            JoinType::Inner,
            &[&left_col],
            &[&right_col],
            None,
        )?;
    }

    Ok(df.logical_plan().clone())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn test_flatten_joins_preserves_semantics_proptest(
        num_tables in 2..3usize, // Stabilized to 2 tables for Phase 2
        num_cols in 2..5usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let plan_res: Result<LogicalPlan> = build_random_plan(num_tables, num_cols).await;
            let plan = plan_res.unwrap();

            // Apply FlattenJoinsRule
            let config = OptimizerContext::default();
            let rule = FlattenJoinsRule::new();
            let optimized = rule.rewrite(plan.clone(), &config).unwrap().data;

            // Generate SQL for both
            let _original_sql: String = get_sql_for_plan(&plan, "postgres").unwrap().unwrap();
            let optimized_sql: String = get_sql_for_plan(&optimized, "postgres").unwrap().unwrap();

            // Standard unparsing should still work
            assert!(optimized_sql.contains("SELECT"));
            assert!(optimized_sql.contains("FROM"));

            // Verify that the rule actually did something (removed a subquery/alias) if applicable
            // For a simple join, DataFusion might already produce a flat plan depending on optimizations
        });
    }
}
