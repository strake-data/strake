//! Diagnostic tests for Oracle pushdown correctness.
//!
//! Tests boolean precedence for IN-list expansion and same-source
//! CTE+join+aggregate subplan pushdown via the SQL generator.

use anyhow::Result;
use datafusion::prelude::*;
use strake_sql::sql_gen::get_sql_for_plan;

/// Helper: register a table with the given name and schema fields in the session context.
fn register_table(
    ctx: &SessionContext,
    name: &str,
    fields: Vec<(&str, arrow::datatypes::DataType)>,
) {
    let schema = arrow::datatypes::Schema::new(
        fields
            .into_iter()
            .map(|(n, t)| arrow::datatypes::Field::new(n, t, true))
            .collect::<Vec<_>>(),
    );
    ctx.register_table(
        name,
        std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
            std::sync::Arc::new(schema),
        )),
    )
    .unwrap();
}

// =============================================================================
// Phase 1: Boolean Precedence Tests
// =============================================================================

#[tokio::test]
async fn test_oracle_in_list_preserved_in_generated_sql() -> Result<()> {
    let ctx = SessionContext::new();
    register_table(
        &ctx,
        "users",
        vec![
            ("ID", arrow::datatypes::DataType::Int32),
            ("NAME", arrow::datatypes::DataType::Utf8),
            ("AGE", arrow::datatypes::DataType::Int32),
        ],
    );

    // Build: SELECT "ID" FROM users WHERE "ID" IN (1, 2) AND "NAME" = 'Alice'
    let plan = ctx
        .sql(r#"SELECT "ID" FROM users WHERE "ID" IN (1, 2) AND "NAME" = 'Alice'"#)
        .await?
        .into_optimized_plan()?;

    println!("Optimized plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "oracle")?.expect("expected SQL output");
    println!("Generated Oracle SQL: {}", sql);

    // The SQL should either preserve IN (1, 2) or parenthesise the OR chain
    let sql_lower = sql.to_lowercase();
    let has_in = sql_lower.contains("in (1, 2)") || sql_lower.contains("in (1.0, 2.0)");
    let has_parenthesized_or = sql_lower.contains("(") && sql_lower.contains(" or ");

    assert!(
        has_in || has_parenthesized_or,
        "Generated SQL must preserve IN syntax or parenthesise OR chains. Got: {}",
        sql
    );

    Ok(())
}

#[tokio::test]
async fn test_oracle_in_list_with_and_precedence() -> Result<()> {
    let ctx = SessionContext::new();
    register_table(
        &ctx,
        "users",
        vec![
            ("ID", arrow::datatypes::DataType::Int32),
            ("NAME", arrow::datatypes::DataType::Utf8),
            ("AGE", arrow::datatypes::DataType::Int32),
        ],
    );

    // The exact query from the Oracle test:
    // WHERE "ID" IN (1, 2) AND "NAME" = 'Alice' AND "AGE" IN (25, 30)
    let plan = ctx
        .sql(
            r#"SELECT "ID" FROM users
               WHERE "ID" IN (1, 2)
                 AND "NAME" = 'Alice'
                 AND "AGE" IN (25, 30)"#,
        )
        .await?
        .into_optimized_plan()?;

    println!("Optimized plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "oracle")?.expect("expected SQL output");
    println!("Generated Oracle SQL: {}", sql);

    // Verify: no unparenthesised OR chain under AND
    // An unparenthesised expansion would look like: ... AND "ID" = 1 OR "ID" = 2 AND ...
    // which would be semantically wrong.
    let sql_upper = sql.to_uppercase();

    // If OR appears, it must be inside parentheses or inside an IN
    if sql_upper.contains(" OR ") {
        // Check that every OR is wrapped in parens
        // Simple check: find " OR " and verify there's a "(" before it at a reasonable distance
        let positions: Vec<usize> = sql_upper.match_indices(" OR ").map(|(i, _)| i).collect();
        for pos in positions {
            let before = &sql_upper[..pos];
            let after = &sql_upper[pos..];
            // There should be a matching "(" before this OR
            let open_count = before.chars().filter(|c| *c == '(').count();
            let close_count = before.chars().filter(|c| *c == ')').count();
            assert!(
                open_count > close_count,
                "OR at position {} appears to be unparenthesised in: {}",
                pos,
                sql
            );
            let _ = after; // suppress unused warning
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_make_binary_op_or_under_and() {
    use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Ident};
    use strake_sql::sql_generator::expr::{expr_precedence, make_binary_op};

    // Build: (a = 1 OR a = 2) AND b = 'x'
    let a_eq_1 = SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::Identifier(Ident::new("a"))),
        op: BinaryOperator::Eq,
        right: Box::new(SqlExpr::Value(
            sqlparser::ast::Value::Number("1".to_string(), false).into(),
        )),
    };
    let a_eq_2 = SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::Identifier(Ident::new("a"))),
        op: BinaryOperator::Eq,
        right: Box::new(SqlExpr::Value(
            sqlparser::ast::Value::Number("2".to_string(), false).into(),
        )),
    };
    let or_chain = make_binary_op(a_eq_1, BinaryOperator::Or, a_eq_2);
    let b_eq_x = SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::Identifier(Ident::new("b"))),
        op: BinaryOperator::Eq,
        right: Box::new(SqlExpr::Value(
            sqlparser::ast::Value::SingleQuotedString("x".to_string()).into(),
        )),
    };

    // Verify OR precedence is lower than AND
    assert_eq!(
        expr_precedence(&or_chain),
        10,
        "OR should have precedence 10"
    );

    // Now AND them together — make_binary_op should parenthesise the OR
    let and_result = make_binary_op(or_chain, BinaryOperator::And, b_eq_x);
    let result_str = and_result.to_string();
    println!("AND result: {}", result_str);

    // The OR chain must be wrapped in parentheses
    assert!(
        result_str.contains("(a = 1 OR a = 2)"),
        "OR under AND must be parenthesised. Got: {}",
        result_str
    );
}

// =============================================================================
// Phase 2: Same-Source CTE+Join+Aggregate Pushdown Tests
// =============================================================================

#[tokio::test]
async fn test_oracle_join_pushdown_sql_gen() -> Result<()> {
    let ctx = SessionContext::new();
    register_table(
        &ctx,
        "users",
        vec![
            ("id", arrow::datatypes::DataType::Int32),
            ("name", arrow::datatypes::DataType::Utf8),
            ("age", arrow::datatypes::DataType::Int32),
        ],
    );
    register_table(
        &ctx,
        "orders",
        vec![
            ("order_id", arrow::datatypes::DataType::Int32),
            ("user_id", arrow::datatypes::DataType::Int32),
            ("amount", arrow::datatypes::DataType::Float64),
        ],
    );

    // Simple join without CTE/aggregate first
    let plan = ctx
        .sql(
            r#"SELECT u.name, o.amount
               FROM users u
               JOIN orders o ON u.id = o.user_id"#,
        )
        .await?
        .into_optimized_plan()?;

    println!("Join plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "oracle")?;
    println!("Oracle join SQL: {:?}", sql);

    assert!(
        sql.is_some(),
        "SQL generation should succeed for simple join"
    );
    let sql_str = sql.unwrap();
    assert!(
        sql_str.to_uppercase().contains("JOIN"),
        "Generated SQL should contain JOIN. Got: {}",
        sql_str
    );

    Ok(())
}

#[tokio::test]
async fn test_oracle_cte_join_aggregate_pushdown_sql_gen() -> Result<()> {
    let ctx = SessionContext::new();
    register_table(
        &ctx,
        "users",
        vec![
            ("id", arrow::datatypes::DataType::Int32),
            ("name", arrow::datatypes::DataType::Utf8),
            ("age", arrow::datatypes::DataType::Int32),
        ],
    );
    register_table(
        &ctx,
        "orders",
        vec![
            ("order_id", arrow::datatypes::DataType::Int32),
            ("user_id", arrow::datatypes::DataType::Int32),
            ("amount", arrow::datatypes::DataType::Float64),
        ],
    );

    // The exact query pattern from test_oracle_same_source_cte_join_aggregate_pushdown
    let plan = ctx
        .sql(
            r#"WITH active_users AS (
                SELECT id, name FROM users WHERE age >= 25
            )
            SELECT u.name, SUM(o.amount) as total
            FROM active_users u
            JOIN orders o ON u.id = o.user_id
            GROUP BY u.name"#,
        )
        .await?
        .into_optimized_plan()?;

    println!("CTE+Join+Aggregate plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "oracle");
    match &sql {
        Ok(Some(s)) => println!("✓ Oracle CTE+Join+Aggregate SQL: {}", s),
        Ok(None) => println!("✗ SQL generation returned None (local execution path)"),
        Err(e) => println!("✗ SQL generation failed: {}", e),
    }

    // This is the key assertion — if this fails, we know what needs fixing
    let sql_str = sql?.expect("SQL generation should succeed for CTE+join+aggregate plan");
    println!("Generated Oracle SQL: {}", sql_str);

    // Verify the SQL has the expected structure
    let sql_upper = sql_str.to_uppercase();
    assert!(sql_upper.contains("JOIN"), "SQL should contain JOIN");
    assert!(
        sql_upper.contains("SUM") || sql_upper.contains("GROUP BY"),
        "SQL should contain aggregate"
    );

    Ok(())
}

#[tokio::test]
async fn test_oracle_self_join_pushdown_sql_gen() -> Result<()> {
    let ctx = SessionContext::new();
    register_table(
        &ctx,
        "users",
        vec![
            ("id", arrow::datatypes::DataType::Int32),
            ("name", arrow::datatypes::DataType::Utf8),
            ("age", arrow::datatypes::DataType::Int32),
        ],
    );

    // Self-join (similar to test_same_source_join_pushdown_federation in test_federation.py)
    let plan = ctx
        .sql(
            r#"SELECT u.id, u.name, u.age
               FROM users u
               JOIN users u2 ON u.id = u2.id
               WHERE u.age > 25"#,
        )
        .await?
        .into_optimized_plan()?;

    println!("Self-join plan:\n{}", plan.display_indent());

    let sql = get_sql_for_plan(&plan, "oracle")?;
    println!("Oracle self-join SQL: {:?}", sql);

    assert!(sql.is_some(), "SQL generation should succeed for self-join");
    let sql_str = sql.unwrap();
    assert!(
        sql_str.to_uppercase().contains("JOIN"),
        "Generated SQL should contain JOIN. Got: {}",
        sql_str
    );

    Ok(())
}
