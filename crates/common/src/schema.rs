//! # Schema and Data Quality
//!
//! Types for representing table schemas, column constraints, and data quality rules.
//!
//! ## Overview
//!
//! This module provides the infrastructure for representing the structure of
//! external data sources via [`IntrospectedTable`] and [`IntrospectedColumn`].
//! It also defines the data contract system using [`ColumnConstraint`] and
//! [`ContractRuleKind`] to ensure data integrity and quality.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::schema::{IntrospectedTable, IntrospectedColumn};
//! let mut table = IntrospectedTable::default();
//! table.name = "users".to_string();
//! table.source = "postgres".to_string();
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Introspection**: Schema metadata is typically fetched once during source
//!   registration or periodically via background workers.
//! - **Validation**: Contract rules are evaluated during query execution with minimal
//!   overhead by leveraging DataFusion's physical plan.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! Logical validation errors (contract violations) are reported as `DataFusionError`.
//!

use serde::{Deserialize, Serialize};

/// Represents an introspected table structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntrospectedTable {
    /// Name of the data source containing this table.
    pub source: String,
    /// Schema name (e.g., "public", "information_schema").
    pub schema: String,
    /// Table name.
    pub name: String,
    /// List of columns in the table (populated when `full = true`).
    pub columns: Vec<IntrospectedColumn>,
    /// Database-native table comment, if any.
    pub db_comment: Option<String>,
    /// AI-generated description of the table's purpose.
    pub ai_description: Option<String>,
}

/// Represents an introspected column structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntrospectedColumn {
    /// Column name.
    pub name: String,
    /// Native database type string (e.g., "VARCHAR(255)", "BIGINT").
    pub type_str: String,
    /// Whether the column allows NULL values.
    pub nullable: bool,
    /// Whether the column is part of the primary key.
    pub is_primary_key: bool,
    /// Whether the column is a foreign key.
    pub is_foreign_key: bool,
    /// List of constraints applied to the column.
    pub constraints: Vec<ColumnConstraint>,
    /// Database-native column comment, if any.
    pub db_comment: Option<String>,
    /// AI-generated description of the column's purpose.
    pub ai_description: Option<String>,
}

/// Column-level constraint or metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ColumnConstraint {
    /// Column is NOT NULL.
    NotNull,
    /// Column is UNIQUE.
    Unique,
    /// Column is part of the PRIMARY KEY.
    PrimaryKey,
    /// Column is a FOREIGN KEY.
    ForeignKey {
        /// Fully qualified name of the referenced table and column.
        references: String,
    },
    /// Lifted from DB CHECK constraint where parseable.
    /// Value is a canonicalised expression, not raw SQL.
    Check {
        /// Canonicalized SQL expression.
        expression: String,
    },
    /// Directly mappable contract rule — lifted only when the
    /// CHECK expression is unambiguous (single-column comparison).
    ContractRule(ContractRuleKind),
}

/// Type of contract rule applied to data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ContractRuleKind {
    /// Greater than value.
    Gt {
        /// Value to compare against.
        value: serde_json::Value,
    },
    /// Greater than or equal to value.
    Gte {
        /// Value to compare against.
        value: serde_json::Value,
    },
    /// Less than value.
    Lt {
        /// Value to compare against.
        value: serde_json::Value,
    },
    /// Less than or equal to value.
    Lte {
        /// Value to compare against.
        value: serde_json::Value,
    },
    /// Value between min and max (inclusive).
    Between {
        /// Minimum value.
        min: serde_json::Value,
        /// Maximum value.
        max: serde_json::Value,
    },
    /// Value is in a list of allowed values.
    In {
        /// List of allowed values.
        values: Vec<serde_json::Value>,
    },
    /// Value matches a regular expression.
    Regex {
        /// Regex patterns.
        pattern: String,
    },
    /// Value is NOT NULL.
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
        if let Ok(f) = val_str.parse::<f64>()
            && let Some(n) = serde_json::Number::from_f64(f)
        {
            return Some(serde_json::Value::Number(n));
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
