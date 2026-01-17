//! Snowflake Dialect
//!
//! Custom UnparserDialect for Snowflake with common function translations.

use super::FunctionMapper;
use datafusion::sql::unparser::dialect::Dialect;

/// Snowflake-specific SQL dialect for the Unparser
#[derive(Debug, Clone)]
pub struct SnowflakeDialect {
    mapper: FunctionMapper,
}

impl Default for SnowflakeDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SnowflakeDialect {
    pub fn new() -> Self {
        Self {
            mapper: snowflake_function_rules(),
        }
    }

    /// Access the function mapper for custom translations
    pub fn mapper(&self) -> &FunctionMapper {
        &self.mapper
    }
}

impl Dialect for SnowflakeDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn use_timestamp_for_date64(&self) -> bool {
        true
    }
}

/// Snowflake-specific function translation rules
fn snowflake_function_rules() -> FunctionMapper {
    FunctionMapper::new()
        // Most common functions are ANSI-compatible in Snowflake
        .rename("length", "LENGTH")
        .rename("substr", "SUBSTR")
        .rename("upper", "UPPER")
        .rename("lower", "LOWER")
        .rename("trim", "TRIM")
        .rename("coalesce", "COALESCE")
        .rename("abs", "ABS")
        .rename("ceil", "CEIL")
        .rename("floor", "FLOOR")
        .rename("round", "ROUND")
        .rename("regexp_replace", "REGEXP_REPLACE")
        .rename("regexp_like", "REGEXP_LIKE")
        .rename("concat", "CONCAT")
        .rename("concat_ws", "CONCAT_WS")
        .rename("left", "LEFT")
        .rename("right", "RIGHT")
        .rename("ascii", "ASCII")
        .rename("chr", "CHAR")
        .rename("log", "LOG")
        // Date/Time
        .rename("date_part", "DATE_PART")
        .rename("date_add", "DATEADD")
        .rename("date_diff", "DATEDIFF")
        .rename("dayname", "DAYNAME")
        .rename("monthname", "MONTHNAME")
        .rename("to_timestamp", "TO_TIMESTAMP")
        .rename("to_date", "TO_DATE")
        .rename("nvl", "NVL")
        .rename("iff", "IFF")
        .rename("div0", "DIV0")
        .rename("startswith", "STARTSWITH")
        .rename("endswith", "ENDSWITH")
        .rename("contains", "CONTAINS")
        .rename("sha256", "SHA2")
        .rename("md5", "MD5")
        .rename("parse_json", "PARSE_JSON")
        .rename("object_construct", "OBJECT_CONSTRUCT")
        .rename("array_contains", "ARRAY_CONTAINS")
        .rename("array_cat", "ARRAY_CAT")
        .rename("split_part", "SPLIT_PART")
        .rename("last_day", "LAST_DAY")
        // Transforms
        .transform("string_agg", |args| {
            // PostgreSQL string_agg → Snowflake LISTAGG
            let expr = args.first().map(|s| s.as_str()).unwrap_or("NULL");
            let sep = args.get(1).map(|s| s.as_str()).unwrap_or("','");
            format!("LISTAGG({}, {})", expr, sep)
        })
        .transform("array_agg", |args| {
            // Snowflake ARRAY_AGG syntax
            let expr = args.first().map(|s| s.as_str()).unwrap_or("NULL");
            format!("ARRAY_AGG({})", expr)
        })
        .transform("json_extract", |args| {
            // PostgreSQL json -> Snowflake GET_PATH
            let expr = args.first().map(|s| s.as_str()).unwrap_or("NULL");
            let path = args.get(1).map(|s| s.as_str()).unwrap_or("''");
            format!("GET_PATH({}, {})", expr, path)
        })
        .transform("current_timestamp", |_| "CURRENT_TIMESTAMP()".to_string())
        .transform("now", |_| "CURRENT_TIMESTAMP()".to_string())
        .transform("date_trunc", |args| {
            // Snowflake DATE_TRUNC uses single quotes around unit
            let unit = args.first().map(|s| s.as_str()).unwrap_or("'day'");
            let expr = args
                .get(1)
                .map(|s| s.as_str())
                .unwrap_or("CURRENT_TIMESTAMP()");
            format!("DATE_TRUNC({}, {})", unit, expr)
        })
        .transform("strpos", |args| {
            // PostgreSQL strpos(haystack, needle) → Snowflake POSITION(needle, haystack)
            let haystack = args.first().cloned().unwrap_or_default();
            let needle = args.get(1).cloned().unwrap_or_default();
            format!("POSITION({}, {})", needle, haystack)
        })
        .transform("random", |_| {
            "UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())".to_string()
        })
        .transform("rand", |_| {
            "UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())".to_string()
        })
        .transform("from_unixtime", |args| {
            let ts = args.first().cloned().unwrap_or_default();
            format!("TO_TIMESTAMP({})", ts)
        })
}
