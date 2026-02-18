use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::*;
use std::sync::Arc;
use strake_sql::optimizer::flatten_federated::FlattenJoinsRule;
use strake_sql::sql_gen::get_sql_for_plan;

fn create_test_schema(prefix: &str, num_cols: usize) -> Arc<Schema> {
    let fields: Vec<_> = (0..num_cols)
        .map(|i| Field::new(format!("{}_col_{}", prefix, i), DataType::Int32, false))
        .collect();
    Arc::new(Schema::new(fields))
}

async fn build_join_tree(num_tables: usize) -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let num_cols = 2; // Keep schema simple

    // Register tables
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

fn benchmark_flatten_joins(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = OptimizerContext::default();
    let rule = FlattenJoinsRule::new();

    let mut group = c.benchmark_group("optimizer");

    // Benchmark 2-table join (supported by SqlGen)
    let plan_2 = rt.block_on(build_join_tree(2)).unwrap();

    group.bench_function("flatten_joins_2_tables", |b| {
        b.iter(|| {
            let _ = rule.rewrite(plan_2.clone(), &config).unwrap();
        })
    });

    // Benchmark 5-table join (just optimization, no SQL gen)
    let plan_5 = rt.block_on(build_join_tree(5)).unwrap();
    group.bench_function("flatten_joins_5_tables", |b| {
        b.iter(|| {
            let _ = rule.rewrite(plan_5.clone(), &config).unwrap();
        })
    });

    group.finish();

    // Benchmark SQL Gen: Original vs Optimized (Only for 2 tables due to limitation)
    let mut group_gen = c.benchmark_group("sql_generation");

    let optimized_2 = rule.rewrite(plan_2.clone(), &config).unwrap().data;

    group_gen.bench_function("generate_sql_original_2_tables", |b| {
        b.iter(|| {
            let _ = get_sql_for_plan(&plan_2, "postgres").unwrap();
        })
    });

    group_gen.bench_function("generate_sql_flattened_2_tables", |b| {
        b.iter(|| {
            let _ = get_sql_for_plan(&optimized_2, "postgres").unwrap();
        })
    });

    group_gen.finish();
}

criterion_group!(benches, benchmark_flatten_joins);
criterion_main!(benches);
