//! Comprehensive Property-Driven SQL Parser & Generator Test Suite
//!
//! Tests DataFusion LogicalPlan translation and unparsing across all supported
//! database dialects in `SourceType` using `proptest`.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, col, lit};
use datafusion::prelude::{ExprFunctionExt, SessionContext};
use datafusion::scalar::ScalarValue;
use proptest::prelude::*;
use sqlparser::dialect::{
    DuckDbDialect, GenericDialect, MySqlDialect, OracleDialect, PostgreSqlDialect, SQLiteDialect,
    SnowflakeDialect,
};
use sqlparser::parser::Parser;
use std::ops::Add;
use std::sync::Arc;
use strake_sql::sql_gen::{get_sql_for_plan, unparse_expr_to_sql};

fn parse_sql_with_dialect(sql: &str, dialect_name: &str) -> bool {
    let parser: Box<dyn sqlparser::dialect::Dialect> = match dialect_name {
        "postgres" => Box::new(PostgreSqlDialect {}),
        "mysql" => Box::new(MySqlDialect {}),
        "sqlite" => Box::new(SQLiteDialect {}),
        "oracle" => Box::new(OracleDialect {}),
        "snowflake" => Box::new(SnowflakeDialect {}),
        "duckdb" => Box::new(DuckDbDialect {}),
        _ => Box::new(GenericDialect {}),
    };
    match Parser::parse_sql(parser.as_ref(), sql) {
        Ok(_) => true,
        Err(e) => {
            eprintln!(
                "Failed to parse generated SQL for dialect '{}': {}\nSQL: {}",
                dialect_name, e, sql
            );
            false
        }
    }
}

fn create_mixed_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val_int", DataType::Int64, true),
        Field::new("val_float", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("is_active", DataType::Boolean, true),
        Field::new("created_date", DataType::Date32, true),
        Field::new("created_date64", DataType::Date64, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "tz_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        ),
    ]))
}

fn arb_dialect() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        Just("postgres"),
        Just("duckdb"),
        Just("mysql"),
        Just("sqlite"),
        Just("oracle"),
        Just("snowflake"),
    ]
}

// ----------------------------------------------------------------------------
// Dynamic Schema & Identifier Generator Strategy
// ----------------------------------------------------------------------------

fn arb_schema() -> impl Strategy<Value = Arc<Schema>> {
    prop::collection::vec(
        (
            prop_oneof![
                Just("id"),
                Just("val_int"),
                Just("val_float"),
                Just("user_name"),
                Just("is_active"),
                Just("created_at"),
            ],
            prop_oneof![
                Just(DataType::Int32),
                Just(DataType::Int64),
                Just(DataType::Float64),
                Just(DataType::Utf8),
                Just(DataType::Boolean),
                Just(DataType::Date32),
                Just(DataType::Timestamp(TimeUnit::Microsecond, None)),
            ],
        ),
        2..6,
    )
    .prop_map(|fields_spec| {
        let mut fields = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (i, (name, dt)) in fields_spec.into_iter().enumerate() {
            let unique_name = if seen.insert(name) {
                name.to_string()
            } else {
                format!("{}_{}", name, i)
            };
            fields.push(Field::new(unique_name, dt, true));
        }
        Arc::new(Schema::new(fields))
    })
}

// ----------------------------------------------------------------------------
// Recursive & Rich Expression Generators
// ----------------------------------------------------------------------------

fn arb_leaf_expr() -> impl Strategy<Value = Expr> {
    prop_oneof![
        Just(col("id")),
        Just(col("val_int")),
        Just(col("val_float")),
        Just(col("user_name")),
        Just(lit(42i64)),
        Just(lit("test_str")),
        Just(lit(true)),
    ]
}

fn arb_complex_expr(depth: u32) -> BoxedStrategy<Expr> {
    if depth == 0 {
        arb_leaf_expr().boxed()
    } else {
        prop_oneof![
            arb_leaf_expr(),
            // Binary expressions
            (arb_complex_expr(depth - 1), arb_complex_expr(depth - 1)).prop_map(|(l, r)| l.add(r)),
            (arb_complex_expr(depth - 1), arb_complex_expr(depth - 1)).prop_map(|(l, r)| l.eq(r)),
            (arb_complex_expr(depth - 1), arb_complex_expr(depth - 1)).prop_map(|(l, r)| l.gt(r)),
            (arb_complex_expr(depth - 1), arb_complex_expr(depth - 1)).prop_map(|(l, r)| l.and(r)),
            (arb_complex_expr(depth - 1), arb_complex_expr(depth - 1)).prop_map(|(l, r)| l.or(r)),
            // Functions
            arb_complex_expr(depth - 1)
                .prop_map(|e| datafusion::functions::string::lower().call(vec![e])),
            arb_complex_expr(depth - 1)
                .prop_map(|e| datafusion::functions::math::abs().call(vec![e])),
            // Nullability & LIKE
            arb_complex_expr(depth - 1).prop_map(|e| e.is_null()),
            arb_complex_expr(depth - 1).prop_map(|e| e.is_not_null()),
            arb_complex_expr(depth - 1).prop_map(|e| e.like(lit("%val%"))),
            // CASE WHEN
            (
                arb_complex_expr(depth - 1),
                arb_complex_expr(depth - 1),
                arb_complex_expr(depth - 1)
            )
                .prop_map(|(cond, then_expr, else_expr)| {
                    Expr::Case(datafusion::logical_expr::Case {
                        expr: None,
                        when_then_expr: vec![(Box::new(cond), Box::new(then_expr))],
                        else_expr: Some(Box::new(else_expr)),
                    })
                },),
        ]
        .boxed()
    }
}

// ----------------------------------------------------------------------------
// Property Tests: Expression Unparsing
// ----------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_unparse_arbitrary_expressions_all_dialects(
        dialect in arb_dialect(),
        expr in arb_complex_expr(2),
    ) {
        let sql_res = unparse_expr_to_sql(&expr, dialect);
        prop_assert!(sql_res.is_ok(), "Failed to unparse expr for dialect {}: {:?}", dialect, sql_res.err());
        let expr_sql = sql_res.unwrap();
        prop_assert!(!expr_sql.is_empty(), "Generated empty SQL string for dialect {}", dialect);

        // Wrap expression in SELECT <expr> FROM t1 skeleton to validate dialect AST parseability
        let full_sql = format!("SELECT {} FROM t1", expr_sql);
        let is_parsable = parse_sql_with_dialect(&full_sql, dialect);
        prop_assert!(is_parsable, "Unparsed expression wrapped in query is not syntactically valid for dialect {}: {}", dialect, full_sql);
    }
}

// ----------------------------------------------------------------------------
// Comprehensive LogicalPlan Builders for Proptest
// ----------------------------------------------------------------------------

async fn build_scan_plan_with_schema(schema: Arc<Schema>) -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;
    let df = ctx.table("t1").await?;
    Ok(df.logical_plan().clone())
}

async fn build_filter_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;
    let df = ctx
        .table("t1")
        .await?
        .filter(col("id").gt(lit(10)).and(col("name").is_not_null()))?;
    Ok(df.logical_plan().clone())
}

async fn build_aggregate_having_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;
    let df = ctx
        .table("t1")
        .await?
        .aggregate(
            vec![col("is_active"), col("name")],
            vec![
                count(col("id")).alias("cnt"),
                sum(col("val_int")).alias("sum_val"),
                min(col("created_date")).alias("min_date"),
                max(col("updated_at")).alias("max_ts"),
            ],
        )?
        .filter(col("cnt").gt(lit(1)))?;
    Ok(df.logical_plan().clone())
}

async fn build_window_frame_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;

    let row_num_expr = datafusion::functions_window::expr_fn::row_number()
        .partition_by(vec![col("is_active")])
        .order_by(vec![col("val_int").sort(true, true)])
        .build()?
        .alias("row_num");

    let rank_expr = datafusion::functions_window::expr_fn::rank()
        .partition_by(vec![col("is_active")])
        .order_by(vec![col("val_int").sort(false, false)])
        .build()?
        .alias("rank_val");

    let df =
        ctx.table("t1")
            .await?
            .select(vec![col("id"), col("name"), row_num_expr, rank_expr])?;
    Ok(df.logical_plan().clone())
}

async fn build_join_plan(join_type: JoinType) -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema1 = create_mixed_schema();
    let schema2 = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema1)),
    )?;
    ctx.register_table(
        "t2",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema2)),
    )?;

    let df1 = ctx.table("t1").await?;
    let df2 = ctx.table("t2").await?;

    let joined = df1.join(df2, join_type, &["id", "val_int"], &["id", "val_int"], None)?;
    Ok(joined.logical_plan().clone())
}

async fn build_sort_limit_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;
    let df = ctx
        .table("t1")
        .await?
        .sort(vec![
            col("val_int").sort(false, true),
            col("name").sort(true, false),
        ])?
        .limit(5, Some(20))?;
    Ok(df.logical_plan().clone())
}

async fn build_set_ops_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
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

    let df1 = ctx
        .table("t1")
        .await?
        .select(vec![col("id"), col("name")])?;
    let df2 = ctx
        .table("t2")
        .await?
        .select(vec![col("id"), col("name")])?;

    let set_df = df1.union(df2)?;
    Ok(set_df.logical_plan().clone())
}

async fn build_distinct_computed_proj_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;
    let df = ctx
        .table("t1")
        .await?
        .select(vec![
            col("id"),
            (col("val_int") + lit(100i64)).alias("computed_val"),
            datafusion::functions::string::lower()
                .call(vec![col("name")])
                .alias("lower_name"),
        ])?
        .distinct()?;
    Ok(df.logical_plan().clone())
}

async fn build_subquery_alias_plan() -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let schema = create_mixed_schema();
    ctx.register_table(
        "t1",
        Arc::new(datafusion::datasource::empty::EmptyTable::new(schema)),
    )?;
    let df = ctx
        .table("t1")
        .await?
        .filter(col("id").gt(lit(0)))?
        .alias("sub_t")?;
    Ok(df.logical_plan().clone())
}

async fn build_values_plan() -> Result<LogicalPlan> {
    let schema = create_mixed_schema();
    let df_schema = DFSchema::try_from_qualified_schema("t1", &schema)?;
    let values_plan = LogicalPlan::Values(datafusion::logical_expr::Values {
        schema: Arc::new(df_schema),
        values: vec![vec![
            lit(1i32),
            lit(10i64),
            lit(1.5f64),
            lit("a"),
            lit(true),
            lit(ScalarValue::Date32(Some(100))),
            lit(ScalarValue::TimestampMicrosecond(Some(1000), None)),
            lit(ScalarValue::TimestampMicrosecond(Some(1000), None)),
        ]],
    });
    Ok(values_plan)
}

// ----------------------------------------------------------------------------
// Property Tests: Full Query Generation across Dialects
// ----------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_plan_sql_generation_all_dialects(
        dialect in arb_dialect(),
        dyn_schema in arb_schema(),
        plan_type in 0..11usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let test_res: std::result::Result<(), TestCaseError> = rt.block_on(async {
            let plan = match plan_type {
                0 => build_scan_plan_with_schema(dyn_schema).await.expect("failed to build scan plan"),
                1 => build_filter_plan().await.expect("failed to build filter plan"),
                2 => build_aggregate_having_plan().await.expect("failed to build aggregate having plan"),
                3 => build_window_frame_plan().await.expect("failed to build window frame plan"),
                4 => build_join_plan(JoinType::Inner).await.expect("failed to build inner join plan"),
                5 => build_join_plan(JoinType::Left).await.expect("failed to build left join plan"),
                6 => build_join_plan(JoinType::Right).await.expect("failed to build right join plan"),
                7 => build_sort_limit_plan().await.expect("failed to build sort limit plan"),
                8 => build_set_ops_plan().await.expect("failed to build union plan"),
                9 => build_distinct_computed_proj_plan().await.expect("failed to build distinct computed proj plan"),
                10 => build_subquery_alias_plan().await.expect("failed to build subquery alias plan"),
                _ => build_values_plan().await.expect("failed to build values plan"),
            };

            let res = get_sql_for_plan(&plan, dialect);
            prop_assert!(res.is_ok(), "get_sql_for_plan failed for dialect {}: {:?}", dialect, res.err());

            let sql_opt = res.unwrap();
            prop_assert!(sql_opt.is_some(), "Expected generated SQL string for dialect {}", dialect);

            let sql = sql_opt.unwrap();
            prop_assert!(!sql.is_empty(), "Empty generated SQL string for dialect {}", dialect);

            // Verify strict dialect parseability
            let is_parsable = parse_sql_with_dialect(&sql, dialect);
            prop_assert!(is_parsable, "Generated SQL is not syntactically valid for dialect {}: {}", dialect, sql);
            Ok(())
        });
        test_res?;
    }
}
