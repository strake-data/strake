//! Oracle Dialect
//!
//! Custom UnparserDialect for Oracle Database with comprehensive function translations.

use super::FunctionMapper;
use datafusion::sql::unparser::dialect::Dialect;

/// Oracle-specific SQL dialect for the Unparser

#[derive(Debug, Clone)]
pub struct OracleDialect {
    mapper: FunctionMapper,
}

impl Default for OracleDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleDialect {
    pub fn new() -> Self {
        Self {
            mapper: oracle_function_rules(),
        }
    }

    /// Access the function mapper for custom translations
    pub fn mapper(&self) -> &FunctionMapper {
        &self.mapper
    }
}

impl Dialect for OracleDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn use_timestamp_for_date64(&self) -> bool {
        false
    }
}

/// Comprehensive Oracle function translation rules
fn oracle_function_rules() -> FunctionMapper {
    FunctionMapper::new()
        // ========================================
        // NULL handling
        // ========================================
        .rename("coalesce", "NVL") // NVL for 2 args; COALESCE also works
        .rename("nullif", "NULLIF")
        .transform("ifnull", |args| {
            // MySQL IFNULL → Oracle NVL
            let a = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            let b = args.get(1).cloned().unwrap_or_else(|| "NULL".to_string());
            format!("NVL({}, {})", a, b)
        })
        // ========================================
        // String functions
        // ========================================
        .rename("length", "LENGTH")
        .rename("char_length", "LENGTH")
        .rename("character_length", "LENGTH")
        .rename("octet_length", "LENGTHB") // byte length
        .rename("substr", "SUBSTR")
        .rename("substring", "SUBSTR")
        .rename("upper", "UPPER")
        .rename("lower", "LOWER")
        .rename("trim", "TRIM")
        .rename("ltrim", "LTRIM")
        .rename("rtrim", "RTRIM")
        .rename("lpad", "LPAD")
        .rename("rpad", "RPAD")
        .rename("replace", "REPLACE")
        .rename("translate", "TRANSLATE")
        .rename("reverse", "REVERSE")
        .rename("initcap", "INITCAP")
        .rename("ascii", "ASCII")
        .rename("chr", "CHR")
        // Concatenation: PostgreSQL concat() → Oracle ||
        .transform("concat", |args| args.join(" || "))
        .transform("concat_ws", |args| {
            // concat_ws(sep, a, b, c) → a || sep || b || sep || c
            if args.is_empty() {
                return "NULL".to_string();
            }
            let sep = args.first().unwrap();
            args[1..].join(&format!(" || {} || ", sep))
        })
        .transform("strpos", |args| {
            // strpos(string, substring) → INSTR(string, substring)
            let a = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            let b = args.get(1).cloned().unwrap_or_else(|| "NULL".to_string());
            format!("INSTR({}, {})", a, b)
        })
        .transform("position", |args| {
            // position(substring IN string) parsed as position(substring, string)
            let a = args.get(1).cloned().unwrap_or_else(|| "NULL".to_string());
            let b = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            format!("INSTR({}, {})", a, b)
        })
        .transform("regexp_replace", |args| {
            let a = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            let b = args.get(1).cloned().unwrap_or_else(|| "NULL".to_string());
            let c = args.get(2).cloned().unwrap_or_else(|| "NULL".to_string());
            format!("REGEXP_REPLACE({}, {}, {})", a, b, c)
        })
        .transform("regexp_like", |args| {
            let a = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            let b = args.get(1).cloned().unwrap_or_else(|| "NULL".to_string());
            format!("REGEXP_LIKE({}, {})", a, b)
        })
        // ========================================
        // Numeric functions
        // ========================================
        .rename("abs", "ABS")
        .rename("ceil", "CEIL")
        .rename("ceiling", "CEIL")
        .rename("floor", "FLOOR")
        .rename("round", "ROUND")
        .rename("trunc", "TRUNC")
        .rename("truncate", "TRUNC")
        .rename("mod", "MOD")
        .rename("power", "POWER")
        .rename("pow", "POWER")
        .rename("sqrt", "SQRT")
        .rename("exp", "EXP")
        .rename("ln", "LN")
        .rename("log", "LOG")
        .rename("log10", "LOG") // LOG(10, x) in Oracle
        .rename("sign", "SIGN")
        .rename("greatest", "GREATEST")
        .rename("least", "LEAST")
        // Random: random() → DBMS_RANDOM.VALUE
        .transform("random", |_| "DBMS_RANDOM.VALUE".to_string())
        .transform("rand", |_| "DBMS_RANDOM.VALUE".to_string())
        // ========================================
        // Date/Time functions
        // ========================================
        .transform("current_timestamp", |_| "SYSTIMESTAMP".to_string())
        .transform("current_date", |_| "SYSDATE".to_string())
        .transform("now", |_| "SYSTIMESTAMP".to_string())
        .transform("getdate", |_| "SYSDATE".to_string()) // MSSQL
        .transform("date_part", |args| {
            // date_part('year', date) → EXTRACT(YEAR FROM date)
            let part = args.first().map(|s| s.trim_matches('\'')).unwrap_or("YEAR");
            let date = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            format!("EXTRACT({} FROM {})", part.to_uppercase(), date)
        })
        .transform("extract", |args| {
            // Already correct syntax for Oracle
            let a = args.first().cloned().unwrap_or_else(|| "YEAR".to_string());
            let b = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            format!("EXTRACT({} FROM {})", a, b)
        })
        .transform("date_trunc", |args| {
            let unit = args.first().cloned().unwrap_or_else(|| "'DAY'".to_string());
            let expr = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            format!("TRUNC({}, {})", expr, unit)
        })
        .transform("to_char", |args| {
            let a = args
                .first()
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            let b = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "'YYYY-MM-DD'".to_string());
            format!("TO_CHAR({}, {})", a, b)
        })
        .transform("to_date", |args| {
            let a = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            let b = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "'YYYY-MM-DD'".to_string());
            format!("TO_DATE({}, {})", a, b)
        })
        .transform("to_timestamp", |args| {
            if args.len() == 1 {
                format!("TO_TIMESTAMP({})", args[0])
            } else {
                let a = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
                let b = args
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| "'YYYY-MM-DD HH24:MI:SS'".to_string());
                format!("TO_TIMESTAMP({}, {})", a, b)
            }
        })
        .transform("dateadd", |args| {
            // dateadd(day, 1, date) → date + INTERVAL '1' DAY
            let unit = args.first().map(|s| s.trim_matches('\'')).unwrap_or("DAY");
            let amount = args.get(1).cloned().unwrap_or_else(|| "0".to_string());
            let date = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            format!("{} + INTERVAL '{}' {}", date, amount, unit.to_uppercase())
        })
        .transform("datediff", |args| {
            // datediff(day, date1, date2) → date2 - date1 (returns days)
            let date1 = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            let date2 = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "SYSDATE".to_string());
            format!("({} - {})", date2, date1)
        })
        .transform("from_unixtime", |args| {
            // from_unixtime(epoch) → TO_DATE('1970-01-01', 'YYYY-MM-DD') + (epoch / 86400)
            let ts = args.first().cloned().unwrap_or_else(|| "0".to_string());
            format!("TO_DATE('1970-01-01', 'YYYY-MM-DD') + ({} / 86400)", ts)
        })
        // ========================================
        // Aggregate functions
        // ========================================
        .rename("count", "COUNT")
        .rename("sum", "SUM")
        .rename("avg", "AVG")
        .rename("min", "MIN")
        .rename("max", "MAX")
        .rename("stddev", "STDDEV")
        .rename("variance", "VARIANCE")
        .transform("string_agg", |args| {
            let expr = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            let sep = args.get(1).cloned().unwrap_or_else(|| "','".to_string());
            format!(
                "LISTAGG({}, {}) WITHIN GROUP (ORDER BY {})",
                expr, sep, expr
            )
        })
        .transform("group_concat", |args| {
            // MySQL GROUP_CONCAT → Oracle LISTAGG
            let expr = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            format!("LISTAGG({}, ',') WITHIN GROUP (ORDER BY {})", expr, expr)
        })
        .rename("array_agg", "COLLECT") // Oracle 10g+
        // ========================================
        // Type casting
        // ========================================
        // CAST handled by Unparser, but type names differ:
        // VARCHAR → VARCHAR2, TEXT → CLOB, INT → NUMBER
        // ========================================
        // Boolean / Logical (Oracle has no native BOOLEAN in SQL)
        // ========================================
        .transform("bool_and", |args| {
            // bool_and(x) → MIN(CASE WHEN x THEN 1 ELSE 0 END) = 1
            let expr = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            format!("MIN(CASE WHEN {} THEN 1 ELSE 0 END) = 1", expr)
        })
        .transform("bool_or", |args| {
            let expr = args.first().cloned().unwrap_or_else(|| "NULL".to_string());
            format!("MAX(CASE WHEN {} THEN 1 ELSE 0 END) = 1", expr)
        })
        // ========================================
        // Miscellaneous
        // ========================================
        .rename("decode", "DECODE") // Oracle native
        .rename("nvl2", "NVL2") // Oracle native: NVL2(expr, if_not_null, if_null)
        .transform("iif", |args| {
            // IIF(cond, then, else) → CASE WHEN cond THEN then ELSE else END
            let cond = args.first().cloned().unwrap_or_else(|| "1=1".to_string());
            let then_val = args.get(1).cloned().unwrap_or_else(|| "NULL".to_string());
            let else_val = args.get(2).cloned().unwrap_or_else(|| "NULL".to_string());
            format!("CASE WHEN {} THEN {} ELSE {} END", cond, then_val, else_val)
        })
}
