use crate::fixtures::*;
use datafusion::common::Result;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_window::row_number::row_number;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart,
};
use std::sync::Arc;
use strake_sql::dialects::FunctionMapper;
use strake_sql::sql_generator::context::ColumnEntry;
use strake_sql::sql_generator::dialect::GeneratorDialect;
use strake_sql::sql_generator::SqlGenerator;

async fn setup_ctx() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("role", DataType::Utf8, false),
    ]));
    let batch1 = datafusion::arrow::record_batch::RecordBatch::new_empty(schema1);
    let batch2 = datafusion::arrow::record_batch::RecordBatch::new_empty(schema2);
    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;
    Ok(ctx)
}

#[tokio::test]
async fn test_join_column_collision() -> Result<()> {
    let ctx = setup_ctx().await?;

    // SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id = t2.id
    let plan = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            datafusion::logical_expr::JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )?
        .select(vec![col("t1.id"), col("t2.id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen.generate(&plan).unwrap();
        println!("JOIN COLLISION SQL: {}", sql);
        // Should use systematic aliases rel_0, rel_1 for source tables
        assert!(sql.contains("\"rel_0\".\"id\""));
        assert!(sql.contains("\"rel_1\".\"id\""));
        assert!(sql.contains("FROM \"t1\" AS \"rel_0\" INNER JOIN \"t2\" AS \"rel_1\""));
    });
    Ok(())
}

#[tokio::test]
async fn test_nested_projection_aliases() -> Result<()> {
    let ctx = setup_ctx().await?;

    // SELECT id FROM (SELECT id FROM (SELECT id FROM t1))
    let plan = ctx
        .table("t1")
        .await?
        .select(vec![col("id")])?
        .select(vec![col("id")])?
        .select(vec![col("id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen.generate(&plan).unwrap();
        println!("NESTED PROJECTION SQL: {}", sql);
        assert!(sql.contains("\"rel_0\".\"id\"") || sql.contains("\"rel_1\".\"id\""));
    });
    Ok(())
}

#[tokio::test]
async fn test_limit_validation() -> Result<()> {
    // This test originally checked that invalid LIMIT expressions (like column refs) were rejected.
    // However, with P1 fixes, we now allow dynamic limits (subqueries, arithmetic, references).
    // Whether the target DB accepts them is up to the DB, but the generator should not block them.

    let ctx = setup_ctx().await?;
    let table_plan = ctx.table("t1").await?.into_optimized_plan()?;

    let plan = LogicalPlan::Limit(datafusion::logical_expr::Limit {
        skip: None,
        fetch: Some(Box::new(col("id"))), // Column ref in LIMIT (dynamic)
        input: Arc::new(table_plan),
    });

    with_generator!(gen, {
        let res = gen.generate(&plan);
        assert!(
            res.is_ok(),
            "Generator should allow dynamic limit expressions"
        );
        let sql = res.unwrap();
        // It likely generates something like LIMIT "rel_0"."id"
        // We just verify it generates *some* limit clause
        assert!(sql.contains("LIMIT"));
    });
    Ok(())
}

#[tokio::test]
async fn test_recursion_limit() -> Result<()> {
    let ctx = setup_ctx().await?;
    let table_plan = Arc::new(ctx.table("t1").await?.into_optimized_plan()?);

    let mut plan = table_plan;
    for _ in 0..150 {
        plan = Arc::new(LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new(vec![col("id")], plan).unwrap(),
        ));
    }

    with_generator!(gen, {
        let res = gen.generate(&plan);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err
            .to_string()
            .contains("Maximum recursion depth (50) exceeded"));
    });
    Ok(())
}

#[tokio::test]
async fn test_scope_violation_context() -> Result<()> {
    with_generator!(gen, {
        let col = datafusion::common::Column::new(None::<String>, "non_existent");
        gen.context
            .enter_scope(
                "rel_0".to_string(),
                vec![
                    ColumnEntry {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        source_alias: "rel_0".into(),
                        provenance: vec!["rel_0".to_string()],
                        unique_id: 0,
                    },
                    ColumnEntry {
                        name: "name".into(),
                        data_type: DataType::Utf8,
                        source_alias: "rel_0".into(),
                        provenance: vec!["rel_0".to_string()],
                        unique_id: 1,
                    },
                ]
                .into(),
                vec!["t1".to_string()],
            )
            .commit();

        let res = gen.context.resolve_column(&col, "TestNode");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.to_string().contains("Column 'non_existent' not found"));
        assert!(err.to_string().contains("rel_0.id"));
        assert!(err.to_string().contains("rel_0.name"));
    });
    Ok(())
}

#[tokio::test]
async fn test_window_function_with_frame() -> Result<()> {
    let ctx = setup_ctx().await?;

    use datafusion::logical_expr::{
        window_frame::WindowFrame, window_frame::WindowFrameBound, window_frame::WindowFrameUnits,
    };
    use datafusion::scalar::ScalarValue;

    let window_expr = row_number()
        .partition_by(vec![col("name")])
        .order_by(vec![col("id").sort(true, false)])
        .window_frame(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
        ))
        .build()?
        .alias("row_num");

    let plan = ctx
        .table("t1")
        .await?
        .window(vec![window_expr.alias("row_num")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen.generate(&plan).unwrap();
        println!("WINDOW FRAME SQL: {}", sql);
        assert!(sql.contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"));
    });
    Ok(())
}

#[tokio::test]
async fn test_nested_subquery_alias_stack_depth() -> Result<()> {
    let ctx = setup_ctx().await?;

    let plan = ctx
        .table("t1")
        .await?
        .select(vec![col("id")])?
        .alias("level1")?
        .select(vec![col("id")])?
        .alias("level2")?
        .select(vec![col("id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let _ = gen.generate(&plan).unwrap();
        assert_eq!(gen.context.scope_stack_len(), 1);
    });
    Ok(())
}

#[tokio::test]
async fn test_join_isolation() -> Result<()> {
    let ctx = setup_ctx().await?;

    let plan = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            datafusion::logical_expr::JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )?
        .alias("joined")?
        .select(vec![col("id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen.generate(&plan).unwrap();
        println!("JOIN ISOLATION SQL: {}", sql);
        assert!(!sql.starts_with("SELECT \"rel_0\"") && !sql.starts_with("SELECT \"rel_1\""));
        assert!(sql.contains("\"t1\" AS \"rel_0\""));
        assert!(sql.contains("\"t2\" AS \"rel_1\""));
    });
    Ok(())
}

#[tokio::test]
async fn test_function_mapper_security() -> Result<()> {
    let mapper = FunctionMapper::new().transform("unsafe_func", |args| {
        SqlExpr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("NVL"))]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(args[0].clone())),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(
                        sqlparser::ast::Value::Number("0".to_string(), false).into(),
                    ))),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        })
    });

    let dialect = GeneratorDialect::new(
        &PostgreSqlDialect {},
        Some(&mapper),
        std::sync::Arc::new(strake_sql::sql_generator::dialect::DefaultDialectCapabilities),
        std::sync::Arc::new(strake_sql::sql_generator::dialect::DefaultTypeMapper),
        "postgres",
    );
    let mut gen = SqlGenerator::new(dialect);

    let evil_input = "foo\"bar";

    gen.context
        .enter_scope(
            "rel_0".to_string(),
            vec![ColumnEntry {
                name: evil_input.into(),
                data_type: DataType::Int32,
                source_alias: "rel_0".into(),
                provenance: vec!["rel_0".to_string()],
                unique_id: 0,
            }]
            .into(),
            vec![],
        )
        .commit();

    let mut translator =
        strake_sql::sql_generator::expr::ExprTranslator::new(&mut gen.context, &gen.dialect);
    let res = translator.translate_function("UNSAFE_FUNC", &[col(evil_input)], None);

    // The security fix should reject identifiers with quotes
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("forbidden characters"));

    Ok(())
}

#[tokio::test]
async fn test_context_underflow_guard() -> Result<()> {
    let mut gen = SqlGenerator::new(GeneratorDialect::new(
        &PostgreSqlDialect {},
        None,
        std::sync::Arc::new(strake_sql::sql_generator::dialect::DefaultDialectCapabilities),
        std::sync::Arc::new(strake_sql::sql_generator::dialect::DefaultTypeMapper),
        "postgres",
    ));

    gen.context.pop_scope();
    gen.context.pop_scope();

    assert_eq!(gen.context.scope_stack_len(), 0);
    Ok(())
}

#[tokio::test]
async fn test_kitchen_sink_query() -> Result<()> {
    let ctx = setup_ctx().await?;

    let plan = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            datafusion::logical_expr::JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )?
        .select(vec![col("t1.id"), col("name"), col("role")])?
        .filter(col("name").not_eq(lit("admin")))?
        .aggregate(vec![col("name")], vec![sum(col("id")).alias("total_id")])?
        .window(vec![row_number()
            .partition_by(vec![col("name")])
            .order_by(vec![col("total_id").sort(false, false)])
            .build()?
            .alias("rn")])?
        .alias("sub")?
        .filter(col("rn").eq(lit(1)))?
        .sort(vec![col("total_id").sort(false, false)])?
        .limit(0, Some(10))?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen.generate(&plan).unwrap();
        println!("KITCHEN SINK SQL:\n{}", sql);

        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM"));
        assert!(sql.contains("INNER JOIN"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("row_number() OVER") || sql.contains("ROW_NUMBER() OVER"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
    });
    Ok(())
}
#[tokio::test]
async fn test_identifier_injection_rejection() -> Result<()> {
    let payloads = vec![
        "users\" UNION SELECT",
        "x; DROP TABLE users",
        "name\0hidden",
        "\"\"\"", // Triple quotes
        "back`tick",
        "back\\slash",
    ];

    for payload in payloads {
        let res = strake_sql::sql_generator::sanitize::safe_ident(payload);
        assert!(
            res.is_err(),
            "Payload '{}' should have been rejected",
            payload
        );
    }
    Ok(())
}
