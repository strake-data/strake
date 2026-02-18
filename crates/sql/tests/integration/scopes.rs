use crate::fixtures::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use std::sync::Arc;

async fn setup_join_context() -> Result<(SessionContext, Arc<Schema>, Arc<Schema>)> {
    let ctx = SessionContext::new();
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("role", DataType::Utf8, false),
    ]));
    let batch1 = datafusion::arrow::record_batch::RecordBatch::new_empty(schema1.clone());
    let batch2 = datafusion::arrow::record_batch::RecordBatch::new_empty(schema2.clone());
    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;
    Ok((ctx, schema1, schema2))
}

#[tokio::test]
async fn test_join_column_collision() -> Result<()> {
    let (ctx, _, _) = setup_join_context().await?;

    // SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id = t2.id
    let plan = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )?
        .select(vec![col("t1.id"), col("t2.id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        // Verify that the ON condition and SELECT use correct disambiguated aliases
        assert!(
            sql.contains("\"rel_0\".\"id\" = \"rel_1\".\"id\""),
            "Join condition should be disambiguated: {}",
            sql
        );
        assert!(
            sql.contains("\"rel_0\".\"id\"") && sql.contains("\"rel_1\".\"id\""),
            "Projection should be disambiguated: {}",
            sql
        );
    });
    Ok(())
}

#[tokio::test]
async fn test_projection_provenance_above_join() -> Result<()> {
    let (ctx, _, _) = setup_join_context().await?;

    // Ensure that a projection above a join correctly tracks which column came from which side
    let plan = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )?
        // This projection happens ABOVE the join
        .select(vec![col("t1.id"), col("t2.role")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        // Check that t0.id and t1.role are used in the same SELECT
        assert!(sql.contains("\"rel_0\".\"id\""));
        assert!(sql.contains("\"rel_1\".\"role\""));
    });
    Ok(())
}

#[tokio::test]
async fn test_nested_subquery_alias_resets_source_alias() -> Result<()> {
    let (ctx, _, _) = setup_join_context().await?;

    // (SELECT id FROM t1) AS sub1
    let plan = ctx
        .table("t1")
        .await?
        .select(vec![col("id")])?
        .alias("sub1")?
        .select(vec![col("id")])?
        .into_optimized_plan()?;

    with_generator!(gen, {
        let sql = gen
            .generate(&plan)
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
        // Inner: SELECT t0.id FROM t1 AS t0
        // Outer: SELECT t1.id FROM (...) AS t1
        assert!(sql.contains(
            "SELECT \"rel_1\".\"id\" FROM (SELECT \"rel_0\".\"id\" FROM \"t1\" AS \"rel_0\") AS \"rel_1\""
        ));
    });
    Ok(())
}
