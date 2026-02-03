//! Snowflake Dialect Tests
//!
//! Comprehensive test cases for Snowflake SQL dialect translation.

use sqlparser::ast::{Expr as SqlExpr, Ident};
use strake_sql::dialects::SnowflakeDialect;

fn id(name: &str) -> SqlExpr {
    SqlExpr::Identifier(Ident::new(name))
}

fn lit(s: &str) -> SqlExpr {
    SqlExpr::Value(sqlparser::ast::Value::SingleQuotedString(s.to_string()).into())
}

#[test]
fn coalesce_renamed() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("coalesce", &[id("a"), id("b")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("COALESCE(a, b)".to_string())
    );
}

#[test]
fn string_agg_to_listagg() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("string_agg", &[id("name"), lit(",")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("LISTAGG(name, ',')".to_string())
    );
}

#[test]
fn array_agg_supported() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("array_agg", &[id("id")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("ARRAY_AGG(id)".to_string())
    );
}

#[test]
fn current_timestamp_to_function() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("current_timestamp", &[]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("CURRENT_TIMESTAMP".to_string())
    );
}

#[test]
fn from_unixtime_to_timestamp() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("from_unixtime", &[id("ts")]);
    assert_eq!(
        result.map(|r| r.to_string()),
        Some("TO_TIMESTAMP(ts)".to_string())
    );
}
