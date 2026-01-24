use crate::{ErrorCode, ErrorContext, StrakeError};
use datafusion::error::DataFusionError;

impl From<DataFusionError> for StrakeError {
    fn from(err: DataFusionError) -> Self {
        match &err {
            DataFusionError::SchemaError(schema_err, _) => match schema_err.as_ref() {
                datafusion::common::SchemaError::FieldNotFound {
                    field,
                    valid_fields,
                } => {
                    let available: Vec<String> =
                        valid_fields.iter().map(|f| f.name.clone()).collect();

                    let hint = find_closest_match(&field.name, &available);

                    let mut error = StrakeError::new(
                        ErrorCode::FieldNotFound,
                        format!("Field '{}' not found", field.name),
                    )
                    .with_context(ErrorContext::FieldNotFound {
                        field: field.name.clone(),
                        table: None,
                        available_fields: available,
                    });

                    if let Some(closest) = hint {
                        error = error.with_hint(format!("Did you mean '{}'?", closest));
                    }
                    error
                }
                _ => StrakeError::new(ErrorCode::DataFusionInternal, schema_err.to_string()),
            },
            DataFusionError::Plan(msg) => StrakeError::new(ErrorCode::SyntaxError, msg.clone()),
            DataFusionError::SQL(parse_err, _) => {
                StrakeError::new(ErrorCode::SyntaxError, parse_err.to_string())
            }
            _ => StrakeError::new(ErrorCode::DataFusionInternal, err.to_string()),
        }
    }
}

impl From<std::io::Error> for StrakeError {
    fn from(err: std::io::Error) -> Self {
        StrakeError::new(ErrorCode::DataFusionInternal, err.to_string())
    }
}

impl From<serde_json::Error> for StrakeError {
    fn from(err: serde_json::Error) -> Self {
        StrakeError::new(ErrorCode::SerializationFailed, err.to_string())
    }
}

impl From<serde_yaml::Error> for StrakeError {
    fn from(err: serde_yaml::Error) -> Self {
        StrakeError::new(ErrorCode::InvalidYaml, err.to_string())
    }
}

// Levenshtein-based suggestion (moved from strake-common)
fn find_closest_match(target: &str, options: &[String]) -> Option<String> {
    let mut best_match: Option<&str> = None;
    let mut min_distance = usize::MAX;

    for option in options {
        let distance = levenshtein(target, option);
        if distance < min_distance && distance <= 3 {
            min_distance = distance;
            best_match = Some(option.as_str());
        }
    }

    best_match.map(|s| s.to_string())
}

fn levenshtein(a: &str, b: &str) -> usize {
    let len_a = a.len();
    let len_b = b.len();
    let mut dp = vec![vec![0; len_b + 1]; len_a + 1];

    for (i, row) in dp.iter_mut().enumerate().take(len_a + 1) {
        row[0] = i;
    }
    for (j, val) in dp[0].iter_mut().enumerate().take(len_b + 1) {
        *val = j;
    }

    for i in 1..=len_a {
        for j in 1..=len_b {
            let cost = if a.chars().nth(i - 1) == b.chars().nth(j - 1) {
                0
            } else {
                1
            };
            dp[i][j] = std::cmp::min(
                std::cmp::min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                dp[i - 1][j - 1] + cost,
            );
        }
    }

    dp[len_a][len_b]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("book", "back"), 2);
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("same", "same"), 0);
    }

    #[test]
    fn test_find_closest_match() {
        let options = vec![
            "revenue".to_string(),
            "cost".to_string(),
            "profit".to_string(),
        ];

        // Exact matches
        assert_eq!(
            find_closest_match("revenue", &options),
            Some("revenue".to_string())
        );

        // Close matches
        assert_eq!(
            find_closest_match("revenu", &options),
            Some("revenue".to_string())
        );
        assert_eq!(
            find_closest_match("cst", &options),
            Some("cost".to_string())
        );

        // No match (distance > 3)
        assert_eq!(find_closest_match("completely_different", &options), None);
    }

    #[test]
    fn test_datafusion_error_mappings() {
        use datafusion::common::Column;

        // Test Plan mapping
        let plan_err = DataFusionError::Plan("Table not found".to_string());
        let strake_err: StrakeError = plan_err.into();
        assert_eq!(strake_err.code, ErrorCode::SyntaxError);
        assert_eq!(strake_err.message, "Table not found");

        // Test SchemaError mapping with hint
        let field = Column::from_name("revenu"); // Typo
        let valid_fields = vec![Column::from_name("revenue"), Column::from_name("cost")];

        let schema_err = DataFusionError::SchemaError(
            Box::new(datafusion::common::SchemaError::FieldNotFound {
                field: Box::new(field),
                valid_fields: valid_fields.clone(),
            }),
            Box::new(None),
        );

        let strake_err: StrakeError = schema_err.into();
        assert_eq!(strake_err.code, ErrorCode::FieldNotFound);
        assert_eq!(strake_err.message, "Field 'revenu' not found");
        assert_eq!(strake_err.hint, Some("Did you mean 'revenue'?".to_string()));

        // Check context type
        match strake_err.context {
            Some(ErrorContext::FieldNotFound {
                field,
                available_fields,
                ..
            }) => {
                assert_eq!(field, "revenu");
                assert_eq!(available_fields[0], "revenue");
            }
            _ => panic!("Expected FieldNotFound context"),
        }
    }

    #[test]
    fn test_io_error_mapping() {
        let io_err = std::io::Error::other("File error");
        let strake_err: StrakeError = io_err.into();
        assert_eq!(strake_err.code, ErrorCode::DataFusionInternal);
        assert!(strake_err.message.contains("File error"));
    }
}
