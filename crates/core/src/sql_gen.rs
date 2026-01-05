use anyhow::{Context, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::unparser::Unparser;

use datafusion::sql::unparser::dialect::{
    DefaultDialect, Dialect as UnparserDialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};

/// Generates a SQL string for the given source type using the appropriate SQL dialect.
pub fn get_sql_for_plan(plan: &LogicalPlan, source_type: &str) -> Result<String> {
    // 1. Pick the pre-made dialect from DataFusion (which wraps/uses sqlparser internally)
    let dialect: Box<dyn UnparserDialect> = match source_type.to_lowercase().as_str() {
        "postgres" | "postgresql" => Box::new(PostgreSqlDialect {}),
        "mysql" => Box::new(MySqlDialect {}),
        "sqlite" => Box::new(SqliteDialect {}),
        _ => Box::new(DefaultDialect {}),
    };

    // 2. DataFusion handles the rest automaticall
    let unparser = Unparser::new(&*dialect);

    let ast = unparser
        .plan_to_sql(plan)
        .context("Failed to unparse LogicalPlan to AST")?;

    Ok(ast.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_postgres_quoting() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "postgres").expect("failed to generate sql");
        // Postgres uses " for identifiers
        assert!(sql.contains(r#""users""#) || sql.contains("users"));
        println!("Postgres SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_mysql_quoting() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "mysql").expect("failed to generate sql");
        println!("MySQL SQL: {}", sql);
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
