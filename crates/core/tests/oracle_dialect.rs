//! Oracle Dialect Tests
//!
//! Comprehensive test cases for Oracle SQL dialect translation.

use strake_core::dialects::OracleDialect;

// ----------------------------------------------------------------------------
// NULL handling
// ----------------------------------------------------------------------------

#[test]
fn coalesce_to_nvl() {
    let result = OracleDialect::new()
        .mapper()
        .translate("coalesce", &["a".into(), "b".into()]);
    assert_eq!(result, Some("NVL(a, b)".to_string()));
}

#[test]
fn ifnull_to_nvl() {
    let result = OracleDialect::new()
        .mapper()
        .translate("ifnull", &["col".into(), "0".into()]);
    assert_eq!(result, Some("NVL(col, 0)".to_string()));
}

// ----------------------------------------------------------------------------
// String functions
// ----------------------------------------------------------------------------

#[test]
fn concat_uses_pipe_operator() {
    let result = OracleDialect::new()
        .mapper()
        .translate("concat", &["a".into(), "b".into(), "c".into()]);
    assert_eq!(result, Some("a || b || c".to_string()));
}

#[test]
fn concat_ws_uses_separator() {
    let result = OracleDialect::new().mapper().translate(
        "concat_ws",
        &["'-'".into(), "a".into(), "b".into(), "c".into()],
    );
    assert_eq!(result, Some("a || '-' || b || '-' || c".to_string()));
}

#[test]
fn strpos_to_instr() {
    let result = OracleDialect::new()
        .mapper()
        .translate("strpos", &["haystack".into(), "needle".into()]);
    assert_eq!(result, Some("INSTR(haystack, needle)".to_string()));
}

#[test]
fn position_to_instr_with_swapped_args() {
    // PostgreSQL: position(needle IN haystack)
    // Oracle: INSTR(haystack, needle)
    let result = OracleDialect::new()
        .mapper()
        .translate("position", &["needle".into(), "haystack".into()]);
    assert_eq!(result, Some("INSTR(haystack, needle)".to_string()));
}

#[test]
fn string_agg_to_listagg_with_within_group() {
    let result = OracleDialect::new()
        .mapper()
        .translate("string_agg", &["name".into(), "','".into()]);
    assert_eq!(
        result,
        Some("LISTAGG(name, ',') WITHIN GROUP (ORDER BY name)".to_string())
    );
}

#[test]
fn group_concat_to_listagg() {
    let result = OracleDialect::new()
        .mapper()
        .translate("group_concat", &["name".into()]);
    assert_eq!(
        result,
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
    assert_eq!(result, Some("SYSTIMESTAMP".to_string()));
}

#[test]
fn now_to_systimestamp() {
    let result = OracleDialect::new().mapper().translate("now", &[]);
    assert_eq!(result, Some("SYSTIMESTAMP".to_string()));
}

#[test]
fn current_date_to_sysdate() {
    let result = OracleDialect::new().mapper().translate("current_date", &[]);
    assert_eq!(result, Some("SYSDATE".to_string()));
}

#[test]
fn date_trunc_to_trunc_with_swapped_args() {
    // PostgreSQL: date_trunc('month', created_at)
    // Oracle: TRUNC(created_at, 'MONTH')
    let result = OracleDialect::new()
        .mapper()
        .translate("date_trunc", &["'month'".into(), "created_at".into()]);
    assert_eq!(result, Some("TRUNC(created_at, 'month')".to_string()));
}

#[test]
fn date_part_to_extract() {
    let result = OracleDialect::new()
        .mapper()
        .translate("date_part", &["'year'".into(), "hire_date".into()]);
    assert_eq!(result, Some("EXTRACT(YEAR FROM hire_date)".to_string()));
}

#[test]
fn to_timestamp_single_arg() {
    let result = OracleDialect::new()
        .mapper()
        .translate("to_timestamp", &["'2024-01-15'".into()]);
    assert_eq!(result, Some("TO_TIMESTAMP('2024-01-15')".to_string()));
}

#[test]
fn to_timestamp_with_format() {
    let result = OracleDialect::new().mapper().translate(
        "to_timestamp",
        &[
            "'2024-01-15 10:30:00'".into(),
            "'YYYY-MM-DD HH24:MI:SS'".into(),
        ],
    );
    assert_eq!(
        result,
        Some("TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')".to_string())
    );
}

#[test]
fn from_unixtime_epoch_conversion() {
    let result = OracleDialect::new()
        .mapper()
        .translate("from_unixtime", &["1618088028".into()]);
    assert_eq!(
        result,
        Some("TO_DATE('1970-01-01', 'YYYY-MM-DD') + (1618088028 / 86400)".to_string())
    );
}

#[test]
fn dateadd_to_interval() {
    let result = OracleDialect::new()
        .mapper()
        .translate("dateadd", &["day".into(), "7".into(), "hire_date".into()]);
    assert_eq!(result, Some("hire_date + INTERVAL '7' DAY".to_string()));
}

#[test]
fn datediff_to_subtraction() {
    let result = OracleDialect::new().mapper().translate(
        "datediff",
        &["day".into(), "start_date".into(), "end_date".into()],
    );
    assert_eq!(result, Some("(end_date - start_date)".to_string()));
}

// ----------------------------------------------------------------------------
// Numeric functions
// ----------------------------------------------------------------------------

#[test]
fn random_to_dbms_random() {
    let result = OracleDialect::new().mapper().translate("random", &[]);
    assert_eq!(result, Some("DBMS_RANDOM.VALUE".to_string()));
}

#[test]
fn rand_to_dbms_random() {
    let result = OracleDialect::new().mapper().translate("rand", &[]);
    assert_eq!(result, Some("DBMS_RANDOM.VALUE".to_string()));
}

// ----------------------------------------------------------------------------
// Boolean emulation (Oracle has no native SQL BOOLEAN)
// ----------------------------------------------------------------------------

#[test]
fn bool_and_to_min_case() {
    let result = OracleDialect::new()
        .mapper()
        .translate("bool_and", &["is_active".into()]);
    assert_eq!(
        result,
        Some("MIN(CASE WHEN is_active THEN 1 ELSE 0 END) = 1".to_string())
    );
}

#[test]
fn bool_or_to_max_case() {
    let result = OracleDialect::new()
        .mapper()
        .translate("bool_or", &["has_error".into()]);
    assert_eq!(
        result,
        Some("MAX(CASE WHEN has_error THEN 1 ELSE 0 END) = 1".to_string())
    );
}

#[test]
fn iif_to_case_when() {
    let result = OracleDialect::new().mapper().translate(
        "iif",
        &["amount > 0".into(), "'credit'".into(), "'debit'".into()],
    );
    assert_eq!(
        result,
        Some("CASE WHEN amount > 0 THEN 'credit' ELSE 'debit' END".to_string())
    );
}

// ----------------------------------------------------------------------------
// Regex
// ----------------------------------------------------------------------------

#[test]
fn regexp_replace_preserves_args() {
    let result = OracleDialect::new().mapper().translate(
        "regexp_replace",
        &["text".into(), "'[0-9]+'".into(), "'X'".into()],
    );
    assert_eq!(
        result,
        Some("REGEXP_REPLACE(text, '[0-9]+', 'X')".to_string())
    );
}

#[test]
fn regexp_like_preserves_args() {
    let result = OracleDialect::new()
        .mapper()
        .translate("regexp_like", &["email".into(), "'^[a-z]+@'".into()]);
    assert_eq!(result, Some("REGEXP_LIKE(email, '^[a-z]+@')".to_string()));
}
