use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct StrakeError {
    pub message: String,
    pub code: String,
    pub details: Option<serde_json::Value>,
    pub hint: Option<String>,
}

impl StrakeError {
    pub fn new(message: String, code: String) -> Self {
        Self {
            message,
            code,
            details: None,
            hint: None,
        }
    }

    pub fn with_hint(mut self, hint: String) -> Self {
        self.hint = Some(hint);
        self
    }
}

impl fmt::Display for StrakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for StrakeError {}

impl From<DataFusionError> for StrakeError {
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::SchemaError(schema_err, _) => match *schema_err {
                datafusion::common::SchemaError::FieldNotFound {
                    field,
                    valid_fields,
                } => {
                    let hint = find_closest_match(
                        &field.name,
                        &valid_fields
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect::<Vec<_>>(),
                    );
                    let mut error = StrakeError::new(
                        format!("Field '{}' not found", field.name),
                        "SEMANTIC_ERROR".to_string(),
                    );
                    if let Some(closest) = hint {
                        error = error.with_hint(format!("Did you mean '{}'?", closest));
                    }
                    error
                }
                _ => StrakeError::new(schema_err.to_string(), "SCHEMA_ERROR".to_string()),
            },
            _ => StrakeError::new(err.to_string(), "INTERNAL_ERROR".to_string()),
        }
    }
}

// Simple Levenshtein distance based matcher
fn find_closest_match<'a>(target: &str, options: &[&'a str]) -> Option<&'a str> {
    let mut best_match: Option<&str> = None;
    let mut min_distance = usize::MAX;

    for option in options {
        let distance = levenshtein(target, option);
        if distance < min_distance && distance <= 3 {
            // Threshold of 3
            min_distance = distance;
            best_match = Some(option);
        }
    }

    best_match
}

fn levenshtein(a: &str, b: &str) -> usize {
    let len_a = a.len();
    let len_b = b.len();

    let mut dp = vec![vec![0; len_b + 1]; len_a + 1];

    for i in 0..=len_a {
        dp[i][0] = i;
    }

    for j in 0..=len_b {
        dp[0][j] = j;
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
    fn test_find_closest_match() {
        let options = vec!["revenue", "profit", "cost"];
        assert_eq!(find_closest_match("revenu", &options), Some("revenue"));
        assert_eq!(find_closest_match("revnue", &options), Some("revenue"));
        assert_eq!(find_closest_match("costs", &options), Some("cost"));
        assert_eq!(find_closest_match("xyz", &options), None);
    }
}
