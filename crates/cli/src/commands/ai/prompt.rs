//! # Prompt Construction and Response Parsing
//!
//! Isolated from HTTP concerns so it can be unit-tested without a live API.

use anyhow::{Context, Result};
use strake_common::schema::IntrospectedTable;

/// The system instructions shared by all providers.
pub(super) const SYSTEM_PROMPT: &str = "Return exactly a JSON object with 'table_description' (string) and \
     'columns' (object mapping column names to strings) keys. No markdown, no explanation.";

/// Builds the column description prompt sent to any provider as the user message.
pub(super) fn build_user_prompt(table: &IntrospectedTable) -> String {
    let cols = table
        .columns
        .iter()
        .map(|c| {
            format!(
                "- {}: {} ({})",
                c.name,
                c.type_str,
                if c.nullable { "nullable" } else { "not null" }
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "Generate a concise one-sentence description for the table '{}.{}' \
         and each of its columns.\nColumns:\n{}",
        table.schema, table.name, cols
    )
}

/// Applies a provider's JSON text response to the table's columns in-place.
///
/// # Errors
/// Returns an error if `json_text` is not valid JSON or does not contain
/// the expected keys.
pub(super) fn apply_descriptions(table: &mut IntrospectedTable, json_text: &str) -> Result<()> {
    // Strip markdown formatting if any is present, e.g. ```json ... ```
    let json_text = json_text.trim();
    let json_text = json_text
        .strip_prefix("```json")
        .or_else(|| json_text.strip_prefix("```"))
        .and_then(|s| s.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(json_text);

    let parsed: serde_json::Value =
        serde_json::from_str(json_text).context("Failed to parse AI JSON response")?;

    if let Some(table_desc) = parsed["table_description"].as_str() {
        table.ai_description = Some(table_desc.to_string());
    }

    let cols = parsed["columns"]
        .as_object()
        .context("Expected 'columns' object in AI response")?;

    for col in &mut table.columns {
        if let Some(desc) = cols.get(&col.name).and_then(|v| v.as_str()) {
            col.ai_description = Some(desc.to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use strake_common::schema::{IntrospectedColumn, IntrospectedTable};

    fn make_table() -> IntrospectedTable {
        IntrospectedTable {
            source: "postgres".to_string(),
            schema: "public".to_string(),
            name: "orders".to_string(),
            columns: vec![
                IntrospectedColumn {
                    name: "id".to_string(),
                    type_str: "int4".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    is_foreign_key: false,
                    constraints: vec![],
                    db_comment: None,
                    ai_description: None,
                },
                IntrospectedColumn {
                    name: "total".to_string(),
                    type_str: "numeric(10,2)".to_string(),
                    nullable: true,
                    is_primary_key: false,
                    is_foreign_key: false,
                    constraints: vec![],
                    db_comment: None,
                    ai_description: None,
                },
            ],
            db_comment: None,
            ai_description: None,
        }
    }

    #[test]
    fn build_user_prompt_contains_table_name() {
        let table = make_table();
        let prompt = build_user_prompt(&table);
        assert!(prompt.contains("public.orders"));
    }

    #[test]
    fn build_user_prompt_contains_all_columns() {
        let table = make_table();
        let prompt = build_user_prompt(&table);
        assert!(prompt.contains("- id: int4 (not null)"));
        assert!(prompt.contains("- total: numeric(10,2) (nullable)"));
    }

    #[test]
    fn apply_descriptions_sets_ai_description() {
        let mut table = make_table();
        let json = r#"{"table_description": "A table of orders", "columns": {"id": "Primary key", "total": "Order total in USD"}}"#;
        apply_descriptions(&mut table, json).unwrap();
        assert_eq!(table.ai_description.as_deref(), Some("A table of orders"));
        assert_eq!(
            table.columns[0].ai_description.as_deref(),
            Some("Primary key")
        );
        assert_eq!(
            table.columns[1].ai_description.as_deref(),
            Some("Order total in USD")
        );
    }

    #[test]
    fn apply_descriptions_strips_json_fence() {
        let mut table = make_table();
        let json = "```json\n{\"table_description\": \"desc\", \"columns\": {\"id\": \"PK\"}}\n```";
        apply_descriptions(&mut table, json).unwrap();
        assert_eq!(table.ai_description.as_deref(), Some("desc"));
        assert_eq!(table.columns[0].ai_description.as_deref(), Some("PK"));
    }

    #[test]
    fn apply_descriptions_strips_bare_fence() {
        let mut table = make_table();
        let json = "```\n{\"table_description\": \"desc\", \"columns\": {\"id\": \"PK\"}}\n```";
        apply_descriptions(&mut table, json).unwrap();
        assert_eq!(table.ai_description.as_deref(), Some("desc"));
        assert_eq!(table.columns[0].ai_description.as_deref(), Some("PK"));
    }

    #[test]
    fn apply_descriptions_ignores_unknown_columns() {
        let mut table = make_table();
        let json = r#"{"table_description": "desc", "columns": {"nonexistent": "desc"}}"#;
        apply_descriptions(&mut table, json).unwrap();
        assert_eq!(table.ai_description.as_deref(), Some("desc"));
        assert!(table.columns[0].ai_description.is_none());
    }

    #[test]
    fn apply_descriptions_errors_on_invalid_json() {
        let mut table = make_table();
        assert!(apply_descriptions(&mut table, "not json").is_err());
    }

    #[test]
    fn apply_descriptions_errors_on_missing_columns_key() {
        let mut table = make_table();
        assert!(apply_descriptions(&mut table, r#"{"table_description": "desc"}"#).is_err());
    }
}
