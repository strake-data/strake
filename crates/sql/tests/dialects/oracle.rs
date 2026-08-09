//! Oracle Dialect Tests
//!
//! Comprehensive test cases for Oracle SQL dialect translation.

use sqlparser::ast::{Expr as SqlExpr, Ident};
use strake_sql::dialects::OracleDialect;

fn id(name: &str) -> SqlExpr {
    SqlExpr::Identifier(Ident::new(name))
}

fn lit(s: &str) -> SqlExpr {
    SqlExpr::Value(sqlparser::ast::Value::SingleQuotedString(s.to_string()).into())
}

// ----------------------------------------------------------------------------
// NULL handling
// ----------------------------------------------------------------------------

#[test]
fn coalesce_to_nvl() {
    let result = OracleDialect::new()
        .mapper()
        .translate("coalesce", &[id("a"), id("b")]);
    assert_eq!(result.map(|r| r.to_string()), Some("NVL(a, b)".to_string()));
}

#[test]
fn ifnull_to_nvl() {
    let result = OracleDialect::new()
        .mapper()
        .translate("ifnull", &[id("col"), id("0")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("NVL(col, 0)".to_string())
    );
}

// ----------------------------------------------------------------------------
// String functions
// ----------------------------------------------------------------------------

#[test]
fn concat_uses_pipe_operator() {
    let result = OracleDialect::new()
        .mapper()
        .translate("concat", &[id("a"), id("b"), id("c")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("a || b || c".to_string())
    );
}

#[test]
fn strpos_to_instr() {
    let result = OracleDialect::new()
        .mapper()
        .translate("strpos", &[id("haystack"), id("needle")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("INSTR(haystack, needle)".to_string())
    );
}

#[test]
fn position_to_instr_with_swapped_args() {
    let result = OracleDialect::new()
        .mapper()
        .translate("position", &[id("needle"), id("haystack")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("INSTR(haystack, needle)".to_string())
    );
}

#[test]
fn string_agg_to_listagg_with_within_group() {
    let result = OracleDialect::new()
        .mapper()
        .translate("string_agg", &[id("name"), lit(",")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("LISTAGG(name, ',') WITHIN GROUP (ORDER BY name)".to_string())
    );
}

// ----------------------------------------------------------------------------
// Date/Time functions
// ----------------------------------------------------------------------------

#[test]
fn current_timestamp_to_systimestamp() {
    let result = OracleDialect::new()
        .mapper()
        .translate("current_timestamp", &[]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("SYSTIMESTAMP".to_string())
    );
}

#[test]
fn now_to_systimestamp() {
    let result = OracleDialect::new().mapper().translate("now", &[]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("SYSTIMESTAMP".to_string())
    );
}

#[test]
fn current_date_to_sysdate() {
    let result = OracleDialect::new().mapper().translate("current_date", &[]);
    assert_eq!(result.map(|r| r.to_string()), Some("SYSDATE".to_string()));
}

#[test]
fn to_timestamp_single_arg() {
    let result = OracleDialect::new()
        .mapper()
        .translate("to_timestamp", &[lit("2024-01-15")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("TO_TIMESTAMP('2024-01-15')".to_string())
    );
}

#[test]
fn to_timestamp_with_format() {
    let result = OracleDialect::new().mapper().translate(
        "to_timestamp",
        &[lit("2024-01-15 10:30:00"), lit("YYYY-MM-DD HH24:MI:SS")],
    );
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')".to_string())
    );
}

#[test]
fn date_trunc_to_trunc_with_format() {
    let result = OracleDialect::new()
        .mapper()
        .translate("date_trunc", &[lit("month"), id("START_DATE")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("TRUNC(START_DATE, 'MM')".to_string())
    );
}

// ----------------------------------------------------------------------------
// Expression unparsing & Temporal pushdown tests
// ----------------------------------------------------------------------------

#[test]
fn unparse_date32_literal_uses_explicit_to_date() {
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;
    use strake_sql::sql_gen::unparse_expr_to_sql;

    // Date32(20660) -> 2026-07-26
    let expr = col("START_DATE").gt_eq(lit(ScalarValue::Date32(Some(20660))));
    let sql = unparse_expr_to_sql(&expr, "oracle").unwrap();
    assert_eq!(sql, "\"start_date\" >= TO_DATE('2026-07-26', 'YYYY-MM-DD')");
}

#[test]
fn unparse_timestamp_microsecond_uses_explicit_to_timestamp() {
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;
    use strake_sql::sql_gen::unparse_expr_to_sql;

    // 1785649509392963 microseconds -> 2026-08-02 05:45:09.392963
    let expr = col("START_DATE").gt(lit(ScalarValue::TimestampMicrosecond(
        Some(1785649509392963),
        None,
    )));
    let sql = unparse_expr_to_sql(&expr, "oracle").unwrap();
    assert_eq!(
        sql,
        "\"start_date\" > TO_TIMESTAMP('2026-08-02 05:45:09.392963', 'YYYY-MM-DD HH24:MI:SS.FF')"
    );
}

#[test]
fn unparse_timestamp_tz_uses_explicit_to_timestamp_tz() {
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;
    use strake_sql::sql_gen::unparse_expr_to_sql;

    let expr = col("START_DATE").gt(lit(ScalarValue::TimestampMicrosecond(
        Some(1785649509392963),
        Some(Arc::from("+00:00")),
    )));
    let sql = unparse_expr_to_sql(&expr, "oracle").unwrap();
    assert_eq!(
        sql,
        "\"start_date\" > TO_TIMESTAMP_TZ('2026-08-02 05:45:09.392963 +0000', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"
    );
}

#[test]
fn unparse_ilike_rewrites_to_lower_like() {
    use datafusion::logical_expr::{col, lit};
    use strake_sql::sql_gen::unparse_expr_to_sql;

    let expr = col("NAME").ilike(lit("alice%"));
    let sql = unparse_expr_to_sql(&expr, "oracle").unwrap();
    assert_eq!(sql, "LOWER(\"name\") LIKE LOWER('alice%')");
}
