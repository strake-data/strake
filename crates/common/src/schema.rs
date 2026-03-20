use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectedTable {
    pub source: String,
    pub schema: String,
    pub name: String,
    /// Only populated when `full = true`.
    pub columns: Vec<IntrospectedColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectedColumn {
    pub name: String,
    /// Full precision type string: "NUMERIC(18,4)", "VARCHAR(64)", etc.
    pub type_str: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
    pub constraints: Vec<ColumnConstraint>,
    /// Populated from DB-native column comment if present; None otherwise.
    pub db_comment: Option<String>,
    /// Populated by AI enrichment pass; None until --ai-descriptions runs.
    pub ai_description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey {
        references: String,
    },
    /// Lifted from DB CHECK constraint where parseable.
    /// Value is a canonicalised expression, not raw SQL.
    Check {
        expression: String,
    },
    /// Directly mappable contract rule — lifted only when the
    /// CHECK expression is unambiguous (single-column comparison).
    ContractRule(ContractRuleKind),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ContractRuleKind {
    Gt {
        value: serde_json::Value,
    },
    Gte {
        value: serde_json::Value,
    },
    Lt {
        value: serde_json::Value,
    },
    Lte {
        value: serde_json::Value,
    },
    Between {
        min: serde_json::Value,
        max: serde_json::Value,
    },
    In {
        values: Vec<serde_json::Value>,
    },
    Regex {
        pattern: String,
    },
    NotNull,
}

/// Normalizes the type string based on common dialect conventions.
pub fn normalize_type_str(raw: &str) -> String {
    let raw = raw.to_lowercase();
    let raw = raw.trim();

    match raw {
        "character varying" | "varchar" => "VARCHAR".to_string(),
        s if s.starts_with("character varying(") || s.starts_with("varchar(") => s.to_uppercase(),
        "numeric" | "decimal" => "NUMERIC".to_string(),
        s if s.starts_with("numeric(") || s.starts_with("decimal(") => s.to_uppercase(),
        "integer" | "int" | "int4" => "INTEGER".to_string(),
        "bigint" | "int8" => "BIGINT".to_string(),
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "timestamp without time zone" | "timestamp" => "TIMESTAMP".to_string(),
        "timestamp with time zone" | "timestamptz" => "TIMESTAMPTZ".to_string(),
        "text" => "TEXT".to_string(),
        "float" | "float8" | "double precision" => "DOUBLE".to_string(),
        "real" | "float4" => "FLOAT".to_string(),
        other => other.to_uppercase(),
    }
}

/// Best-effort lifting of a CHECK expression to a ContractRuleKind.
pub fn lift_check_expression(col_name: &str, expression: &str) -> Option<ContractRuleKind> {
    let expr = expression.to_lowercase();
    let col_name_lower = col_name.to_lowercase();

    // Simple patterns
    // col > N
    if let Some(val) = try_parse_op(&expr, &col_name_lower, ">") {
        return Some(ContractRuleKind::Gt { value: val });
    }
    // col >= N
    if let Some(val) = try_parse_op(&expr, &col_name_lower, ">=") {
        return Some(ContractRuleKind::Gte { value: val });
    }
    // col < N
    if let Some(val) = try_parse_op(&expr, &col_name_lower, "<") {
        return Some(ContractRuleKind::Lt { value: val });
    }
    // col <= N
    if let Some(val) = try_parse_op(&expr, &col_name_lower, "<=") {
        return Some(ContractRuleKind::Lte { value: val });
    }
    // col IS NOT NULL
    if expr.contains(&format!("{} is not null", col_name_lower)) {
        return Some(ContractRuleKind::NotNull);
    }

    None
}

fn try_parse_op(expr: &str, col_name: &str, op: &str) -> Option<serde_json::Value> {
    let pattern = format!("{} {} ", col_name, op);
    if let Some(pos) = expr.find(&pattern) {
        let val_str = &expr[pos + pattern.len()..].trim();
        // Try to parse as number or string
        if let Ok(n) = val_str.parse::<i64>() {
            return Some(serde_json::Value::Number(n.into()));
        }
        if let Ok(f) = val_str.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Some(serde_json::Value::Number(n));
            }
        }
        // Fallback or more complex parsing needed?
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_type_str() {
        assert_eq!(
            normalize_type_str("character varying(64)"),
            "CHARACTER VARYING(64)"
        );
        assert_eq!(normalize_type_str("varchar(255)"), "VARCHAR(255)");
        assert_eq!(normalize_type_str("numeric(18,4)"), "NUMERIC(18,4)");
        assert_eq!(normalize_type_str("integer"), "INTEGER");
        assert_eq!(
            normalize_type_str("timestamp without time zone"),
            "TIMESTAMP"
        );
    }

    #[test]
    fn test_lift_check_expression() {
        let col = "amount";
        assert_eq!(
            lift_check_expression(col, "amount > 0"),
            Some(ContractRuleKind::Gt { value: 0.into() })
        );
        assert_eq!(
            lift_check_expression(col, "amount IS NOT NULL"),
            Some(ContractRuleKind::NotNull)
        );
    }
}
