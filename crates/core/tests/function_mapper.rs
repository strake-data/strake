//! FunctionMapper Unit Tests
//!
//! Tests for the core FunctionMapper registry logic.

use strake_core::dialects::FunctionMapper;

#[test]
fn rename_creates_function_call() {
    let mapper = FunctionMapper::new().rename("old", "NEW");
    let result = mapper.translate("old", &["a".into(), "b".into()]);
    assert_eq!(result, Some("NEW(a, b)".to_string()));
}

#[test]
fn translate_is_case_insensitive() {
    let mapper = FunctionMapper::new().rename("coalesce", "NVL");
    assert!(mapper.translate("COALESCE", &["a".into()]).is_some());
    assert!(mapper.translate("Coalesce", &["a".into()]).is_some());
    assert!(mapper.translate("coalesce", &["a".into()]).is_some());
}

#[test]
fn unknown_function_returns_none() {
    let mapper = FunctionMapper::new();
    assert_eq!(mapper.translate("unknown_func", &["x".into()]), None);
}

#[test]
fn transform_can_reorder_args() {
    let mapper = FunctionMapper::new().transform("swap", |args| {
        format!(
            "SWAPPED({}, {})",
            args.get(1).cloned().unwrap_or_default(),
            args.first().cloned().unwrap_or_default()
        )
    });
    let result = mapper.translate("swap", &["first".into(), "second".into()]);
    assert_eq!(result, Some("SWAPPED(second, first)".to_string()));
}

#[test]
fn transform_can_change_to_operator() {
    let mapper = FunctionMapper::new().transform("concat", |args| args.join(" || "));
    let result = mapper.translate("concat", &["a".into(), "b".into()]);
    assert_eq!(result, Some("a || b".to_string()));
}

#[test]
fn empty_args_handled() {
    let mapper = FunctionMapper::new().rename("now", "SYSDATE");
    let result = mapper.translate("now", &[]);
    assert_eq!(result, Some("SYSDATE()".to_string()));
}
