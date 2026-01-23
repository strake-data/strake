use serde_json::Value;
use strake_error::{ErrorCode, ErrorContext, StrakeError};

#[test]
fn test_json_serialization() {
    let error = StrakeError::new(ErrorCode::FieldNotFound, "Field 'revenu' not found")
        .with_context(ErrorContext::FieldNotFound {
            field: "revenu".to_string(),
            table: Some("sales".to_string()),
            available_fields: vec!["revenue".to_string(), "cost".to_string()],
        })
        .with_hint("Did you mean 'revenue'?");

    let json = error.to_json();
    println!("JSON: {}", json);

    let v: Value = serde_json::from_str(&json).expect("valid json");

    assert_eq!(v["code"], "STRAKE-2002");
    assert_eq!(v["message"], "Field 'revenu' not found");
    assert_eq!(v["hint"], "Did you mean 'revenue'?");
    assert_eq!(v["context"]["type"], "field_not_found");
    assert_eq!(v["context"]["field"], "revenu");
}

#[test]
fn test_error_code_parsing() {
    let code: ErrorCode = "STRAKE-1004".to_string().try_into().unwrap();
    assert_eq!(code, ErrorCode::PoolExhausted);
}
