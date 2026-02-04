//! FunctionMapper Unit Tests
//!
//! Tests for the core FunctionMapper registry logic.

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart,
};
use strake_sql::dialects::FunctionMapper;

fn id(name: &str) -> SqlExpr {
    SqlExpr::Identifier(Ident::new(name))
}

#[test]
fn rename_creates_function_call() {
    let mapper = FunctionMapper::new().rename("old", "NEW");
    let result = mapper
        .translate("old", &[id("a"), id("b")])
        .expect("translated");
    assert_eq!(result.to_string(), "NEW(a, b)");
}

#[test]
fn translate_is_case_insensitive() {
    let mapper = FunctionMapper::new().rename("coalesce", "NVL");
    assert!(mapper.translate("COALESCE", &[id("a")]).is_some());
    assert!(mapper.translate("Coalesce", &[id("a")]).is_some());
    assert!(mapper.translate("coalesce", &[id("a")]).is_some());
}

#[test]
fn unknown_function_returns_none() {
    let mapper = FunctionMapper::new();
    assert_eq!(mapper.translate("unknown_func", &[id("x")]), None);
}

#[test]
fn transform_can_reorder_args() {
    let mapper = FunctionMapper::new().transform("swap", |args| {
        SqlExpr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("SWAPPED"))]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        args.get(1).cloned().unwrap_or_else(|| id("")),
                    )),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        args.first().cloned().unwrap_or_else(|| id("")),
                    )),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        })
    });
    let result = mapper
        .translate("swap", &[id("first"), id("second")])
        .expect("translated");
    assert_eq!(result.to_string(), "SWAPPED(second, first)");
}

#[test]
fn transform_can_change_to_operator() {
    let mapper = FunctionMapper::new().transform("concat", |args| SqlExpr::BinaryOp {
        left: Box::new(args[0].clone()),
        op: BinaryOperator::StringConcat,
        right: Box::new(args[1].clone()),
    });
    let result = mapper
        .translate("concat", &[id("a"), id("b")])
        .expect("translated");
    assert_eq!(result.to_string(), "a || b");
}

#[test]
fn empty_args_handled() {
    let mapper = FunctionMapper::new().rename("now", "SYSDATE");
    let result = mapper.translate("now", &[]).expect("translated");
    assert_eq!(result.to_string(), "SYSDATE()");
}
