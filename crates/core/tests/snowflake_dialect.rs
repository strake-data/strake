//! Snowflake Dialect Tests
//!
//! Test cases for Snowflake SQL dialect translation.

use strake_core::dialects::SnowflakeDialect;

#[test]
fn string_agg_to_listagg_without_within_group() {
    // Snowflake LISTAGG doesn't require WITHIN GROUP
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("string_agg", &["name".into(), "','".into()]);
    assert_eq!(result, Some("LISTAGG(name, ',')".to_string()));
}

#[test]
fn json_extract_to_get_path() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("json_extract", &["data".into(), "'$.name'".into()]);
    assert_eq!(result, Some("GET_PATH(data, '$.name')".to_string()));
}

#[test]
fn current_timestamp_with_parens() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("current_timestamp", &[]);
    assert_eq!(result, Some("CURRENT_TIMESTAMP()".to_string()));
}

#[test]
fn now_to_current_timestamp() {
    let result = SnowflakeDialect::new().mapper().translate("now", &[]);
    assert_eq!(result, Some("CURRENT_TIMESTAMP()".to_string()));
}

#[test]
fn date_transforms_renamed() {
    let mapper = SnowflakeDialect::new();
    assert_eq!(
        mapper
            .mapper()
            .translate("date_part", &["part".into(), "date".into()]),
        Some("DATE_PART(part, date)".to_string())
    );
    assert_eq!(
        mapper.mapper().translate(
            "date_add",
            &["part".into(), "interval".into(), "date".into()]
        ),
        Some("DATEADD(part, interval, date)".to_string())
    );
    assert_eq!(
        mapper
            .mapper()
            .translate("date_diff", &["part".into(), "start".into(), "end".into()]),
        Some("DATEDIFF(part, start, end)".to_string())
    );
}

#[test]
fn date_trunc_preserves_order() {
    // Snowflake keeps same arg order as PostgreSQL
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("date_trunc", &["'month'".into(), "created_at".into()]);
    assert_eq!(result, Some("DATE_TRUNC('month', created_at)".to_string()));
}

#[test]
fn array_agg_syntax() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("array_agg", &["col".into()]);
    assert_eq!(result, Some("ARRAY_AGG(col)".to_string()));
}

#[test]
fn random_to_uniform() {
    let result = SnowflakeDialect::new().mapper().translate("random", &[]);
    assert_eq!(
        result,
        Some("UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())".to_string())
    );
}

#[test]
fn rand_to_uniform() {
    let result = SnowflakeDialect::new().mapper().translate("rand", &[]);
    assert_eq!(
        result,
        Some("UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())".to_string())
    );
}

#[test]
fn strpos_to_position() {
    // PostgreSQL: strpos(haystack, needle)
    // Snowflake: POSITION(needle, haystack)
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("strpos", &["haystack".into(), "needle".into()]);
    assert_eq!(result, Some("POSITION(needle, haystack)".to_string()));
}

#[test]
fn from_unixtime_to_to_timestamp() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("from_unixtime", &["1618088028".into()]);
    assert_eq!(result, Some("TO_TIMESTAMP(1618088028)".to_string()));
}

#[test]
fn nvl_renamed() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("nvl", &["a".into(), "b".into()]);
    assert_eq!(result, Some("NVL(a, b)".to_string()));
}

#[test]
fn iff_renamed() {
    let result = SnowflakeDialect::new()
        .mapper()
        .translate("iff", &["cond".into(), "a".into(), "b".into()]);
    assert_eq!(result, Some("IFF(cond, a, b)".to_string()));
}

#[test]
fn startswith_endswith() {
    let mapper = SnowflakeDialect::new();
    assert_eq!(
        mapper
            .mapper()
            .translate("startswith", &["col".into(), "'prefix'".into()]),
        Some("STARTSWITH(col, 'prefix')".to_string())
    );
    assert_eq!(
        mapper
            .mapper()
            .translate("endswith", &["col".into(), "'suffix'".into()]),
        Some("ENDSWITH(col, 'suffix')".to_string())
    );
}

#[test]
fn hash_functions() {
    let mapper = SnowflakeDialect::new();
    assert_eq!(
        mapper.mapper().translate("sha256", &["'data'".into()]),
        Some("SHA2('data')".to_string())
    );
    assert_eq!(
        mapper.mapper().translate("md5", &["'data'".into()]),
        Some("MD5('data')".to_string())
    );
}

#[test]
fn json_utils() {
    let mapper = SnowflakeDialect::new();
    assert_eq!(
        mapper.mapper().translate("parse_json", &["str".into()]),
        Some("PARSE_JSON(str)".to_string())
    );
    assert_eq!(
        mapper
            .mapper()
            .translate("object_construct", &["k".into(), "v".into()]),
        Some("OBJECT_CONSTRUCT(k, v)".to_string())
    );
}

#[test]
fn array_ops() {
    let mapper = SnowflakeDialect::new();
    assert_eq!(
        mapper
            .mapper()
            .translate("array_contains", &["arr".into(), "val".into()]),
        Some("ARRAY_CONTAINS(arr, val)".to_string())
    );
    assert_eq!(
        mapper
            .mapper()
            .translate("array_cat", &["a1".into(), "a2".into()]),
        Some("ARRAY_CAT(a1, a2)".to_string())
    );
}

#[test]
fn string_position_functions() {
    let mapper = SnowflakeDialect::new();
    assert_eq!(
        mapper
            .mapper()
            .translate("split_part", &["str".into(), "' '".into(), "1".into()]),
        Some("SPLIT_PART(str, ' ', 1)".to_string())
    );
    assert_eq!(
        mapper.mapper().translate("last_day", &["date".into()]),
        Some("LAST_DAY(date)".to_string())
    );
}
