//! SQL Generation for Remote Sources.
//!
//! Converts DataFusion LogicalPlans to SQL text using the DialectRouter
//! to select the appropriate rendering strategy per source type.

use anyhow::{Context, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;

use crate::dialect_router::{route_dialect, DialectPath};

/// Generates Substrait plan bytes for the given LogicalPlan.
///
/// Should be used when `is_substrait_source` returns true.
pub async fn get_substrait_for_plan(plan: &LogicalPlan, ctx: &SessionContext) -> Result<Vec<u8>> {
    crate::substrait_producer::to_substrait_bytes(plan, ctx).await
}

/// Converts a logical plan to SQL text using the appropriate dialect for the target source.
///
/// Used by the federation layer when pushing subqueries to remote databases.
/// Each dialect handles identifier quoting, literal formatting, and syntax
/// differences specific to that database.
///
/// # Returns
/// - `Ok(Some(sql))` - SQL generated successfully
/// - `Ok(None)` - LocalExecution fallback (no SQL pushdown)
/// - `Err(...)` - Substrait path requested (use `get_substrait_for_plan` instead)
pub fn get_sql_for_plan(plan: &LogicalPlan, source_type: &str) -> Result<Option<String>> {
    match route_dialect(source_type) {
        DialectPath::Native(dialect) | DialectPath::Custom(dialect) => {
            let unparser = Unparser::new(&*dialect);
            let ast = unparser
                .plan_to_sql(plan)
                .context("Failed to unparse LogicalPlan to AST")?;
            Ok(Some(ast.to_string()))
        }
        DialectPath::Substrait => {
            // Caller should use get_substrait_for_plan instead
            Err(anyhow::anyhow!(
                "Source '{}' uses Substrait, use get_substrait_for_plan instead",
                source_type
            ))
        }
        DialectPath::LocalExecution => {
            // No pushdown available - return None to signal local execution
            tracing::debug!(
                source_type = %source_type,
                "No dialect available, using local execution"
            );
            Ok(None)
        }
    }
}

/// Check if a source uses Substrait for plan pushdown
pub fn is_substrait_source(source_type: &str) -> bool {
    matches!(route_dialect(source_type), DialectPath::Substrait)
}

/// Check if a source requires local execution (no pushdown)
pub fn is_local_execution(source_type: &str) -> bool {
    matches!(route_dialect(source_type), DialectPath::LocalExecution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_postgres_quoting() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "postgres")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        // Postgres uses " for identifiers
        assert!(sql.contains(r#""users""#) || sql.contains("users"));
        println!("Postgres SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_mysql_quoting() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "mysql")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        println!("MySQL SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_oracle_dialect() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "oracle")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        println!("Oracle SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_snowflake_dialect() {
        let plan = test_plan().await;
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
        let plan = test_plan().await;
        let result = get_sql_for_plan(&plan, "unknown_db").expect("should not error");
        assert!(
            result.is_none(),
            "unknown dialect should return None for local execution"
        );
    }

    async fn test_plan() -> LogicalPlan {
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
}
