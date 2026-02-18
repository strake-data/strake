use crate::fixtures::*;
use datafusion::common::Result;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_window::row_number::row_number;
use datafusion::logical_expr::{placeholder, LogicalPlan, LogicalPlanBuilder};

#[tokio::test]
async fn test_table_scan_generation() -> Result<()> {
    let ctx = setup_context().await?;
    let plan = ctx.table("users").await?.into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(
            sql.contains("SELECT \"rel_0\".\"id\", \"rel_0\".\"name\" FROM \"users\" AS \"rel_0\"")
        );
    });
    Ok(())
}

#[tokio::test]
async fn test_projection_generation() -> Result<()> {
    let ctx = setup_context().await?;
    let plan = ctx
        .table("users")
        .await?
        .select(vec![col("id"), col("name")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(
            sql.contains("SELECT \"rel_0\".\"id\", \"rel_0\".\"name\" FROM \"users\" AS \"rel_0\"")
        );
    });
    Ok(())
}

#[tokio::test]
async fn test_filter_generation() -> Result<()> {
    let ctx = setup_context().await?;
    let plan = ctx
        .table("users")
        .await?
        .filter(col("id").eq(lit(1)))?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("SELECT \"rel_0\".\"id\", \"rel_0\".\"name\" FROM \"users\" AS \"rel_0\" WHERE \"rel_0\".\"id\" = 1"));
    });
    Ok(())
}

#[tokio::test]
async fn test_subquery_alias_scope_isolation() -> Result<()> {
    let ctx = setup_context().await?;

    let plan = ctx
        .table("users")
        .await?
        .select(vec![col("id")])?
        .alias("derived")?
        .select(vec![col("id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        // Now explicit columns instead of *
        assert!(sql.contains(
            "SELECT \"rel_1\".\"id\" FROM (SELECT \"rel_0\".\"id\" FROM \"users\" AS \"rel_0\") AS \"rel_1\""
        ));
    });
    Ok(())
}

#[tokio::test]
async fn test_join_generation() -> Result<()> {
    let ctx = setup_context().await?;

    let schema_orders = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("user_id", DataType::Int32, false),
    ]));
    let batch_orders = datafusion::arrow::record_batch::RecordBatch::new_empty(schema_orders);
    ctx.register_batch("orders", batch_orders)?;

    let plan = ctx
        .table("users")
        .await?
        .join(
            ctx.table("orders").await?,
            datafusion::logical_expr::JoinType::Inner,
            &["id"],
            &["user_id"],
            None,
        )?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains(
            "INNER JOIN \"orders\" AS \"rel_1\" ON \"rel_0\".\"id\" = \"rel_1\".\"user_id\""
        ));
    });
    Ok(())
}

#[tokio::test]
async fn test_determinism() -> Result<()> {
    let ctx = setup_context().await?;
    let plan = ctx
        .table("users")
        .await?
        .filter(col("id").eq(lit(1)))?
        .into_optimized_plan()?;

    with_generator!(gen1, {
        let sql1 = gen1.generate(&plan).unwrap();
        with_generator!(gen2, {
            let sql2 = gen2.generate(&plan).unwrap();
            assert_eq!(sql1, sql2);
            assert!(sql1.contains("rel_0"));
        });
    });
    Ok(())
}

#[tokio::test]
async fn test_aggregate_generation() -> Result<()> {
    let ctx = setup_context().await?;
    let plan = ctx
        .table("users")
        .await?
        .aggregate(vec![col("name")], vec![sum(col("id"))])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("SUM(") || sql.contains("sum("));
        assert!(sql.contains("GROUP BY"));
    });
    Ok(())
}

#[tokio::test]
async fn test_sort_limit_generation() -> Result<()> {
    let ctx = setup_context().await?;
    let plan = ctx
        .table("users")
        .await?
        .sort(vec![col("id").sort(true, true)])?
        .limit(0, Some(10))?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("NULLS FIRST"));
    });
    Ok(())
}

#[tokio::test]
async fn test_dynamic_limit_generation() -> Result<()> {
    let ctx = setup_context().await?;
    let table_plan = ctx.table("users").await?.into_optimized_plan()?;

    let plan = LogicalPlan::Limit(datafusion::logical_expr::Limit {
        skip: None,
        fetch: Some(Box::new(placeholder("$1"))),
        input: Arc::new(table_plan),
    });

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("LIMIT $1"));
    });
    Ok(())
}

#[tokio::test]
async fn test_window_function_generation() -> Result<()> {
    let ctx = setup_context().await?;

    let udf = match row_number() {
        Expr::WindowFunction(w) => w.fun.clone(),
        _ => panic!("Expected window function"),
    };

    let plan = ctx
        .table("users")
        .await?
        .window(vec![Expr::WindowFunction(Box::new(
            datafusion::logical_expr::expr::WindowFunction::new(udf, vec![]),
        ))
        .alias("cnt")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.to_uppercase().contains("ROW_NUMBER()"));
        assert!(sql.contains("OVER ("));
    });
    Ok(())
}

#[tokio::test]
async fn test_union_generation() -> Result<()> {
    let ctx = setup_context().await?;

    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch2 = datafusion::arrow::record_batch::RecordBatch::new_empty(schema2);
    ctx.register_batch("users2", batch2)?;

    let plan = ctx
        .table("users")
        .await?
        .union(ctx.table("users2").await?)?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("UNION ALL"));
    });
    Ok(())
}

#[tokio::test]
async fn test_distinct_generation() -> Result<()> {
    let ctx = setup_context().await?;

    let df = ctx.table("users").await?;
    let plan = datafusion::logical_expr::LogicalPlanBuilder::from(df.into_optimized_plan()?)
        .distinct()?
        .build()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("SELECT DISTINCT"));
    });
    Ok(())
}

#[tokio::test]
async fn test_empty_relation_generation() -> Result<()> {
    let _ctx = setup_context().await?;

    let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false).build()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("1 = 0"));
        assert!(sql.contains("SELECT NULL"));
    });
    Ok(())
}

#[tokio::test]
async fn test_recursive_query_generation() -> Result<()> {
    let _ctx = setup_context().await?;

    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));

    let static_term = LogicalPlanBuilder::values(vec![vec![lit(1)]])?
        .project(vec![col("column1").alias("i")])?
        .build()?;

    let table_source = Arc::new(datafusion::logical_expr::LogicalTableSource::new(
        schema.clone(),
    ));
    let recursive_term = LogicalPlanBuilder::scan("recc", table_source, None)?
        .filter(col("recc.i").lt(lit(10)))?
        .project(vec![(col("recc.i") + lit(1)).alias("i")])?
        .build()?;

    let plan = LogicalPlan::RecursiveQuery(datafusion::logical_expr::RecursiveQuery {
        name: "recc".to_string(),
        static_term: Arc::new(static_term),
        recursive_term: Arc::new(recursive_term),
        is_distinct: false,
    });

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        assert!(sql.contains("WITH RECURSIVE"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("recc"));
    });
    Ok(())
}
