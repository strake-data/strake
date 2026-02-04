//! Function Mapper
//!
//! Declarative registry for translating DataFusion function names to dialect-specific syntax.
//! Inspired by sqlglot's AST transformation pattern.

use sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Type alias for transform function closures.
///
/// **Security Notice**: Closures must return pre-sanitized SQL snippets.
/// These strings are parsed as expressions and integrated into the final SQL.
/// Type alias for transform function closures.
pub type TransformFn = Arc<dyn Fn(&[SqlExpr]) -> SqlExpr + Send + Sync>;

/// A translation rule for converting a function to target dialect
pub enum Translation {
    /// Simple rename: "coalesce" → "NVL"
    Rename(&'static str),
    /// Custom transform with access to arguments
    Transform(TransformFn),
}

impl Clone for Translation {
    fn clone(&self) -> Self {
        match self {
            Translation::Rename(s) => Translation::Rename(s),
            Translation::Transform(f) => Translation::Transform(Arc::clone(f)),
        }
    }
}

/// Registry of function translations from DataFusion to target dialect
pub struct FunctionMapper {
    rules: HashMap<&'static str, Translation>,
}

impl std::fmt::Debug for FunctionMapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionMapper")
            .field("rules_count", &self.rules.len())
            .finish()
    }
}

impl Clone for FunctionMapper {
    fn clone(&self) -> Self {
        Self {
            rules: self.rules.iter().map(|(k, v)| (*k, v.clone())).collect(),
        }
    }
}

impl Default for FunctionMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionMapper {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }

    /// Add a simple rename rule
    pub fn rename(mut self, from: &'static str, to: &'static str) -> Self {
        self.rules.insert(from, Translation::Rename(to));
        self
    }

    /// Add a custom transform rule
    pub fn transform<F>(mut self, from: &'static str, f: F) -> Self
    where
        F: Fn(&[SqlExpr]) -> SqlExpr + Send + Sync + 'static,
    {
        self.rules.insert(from, Translation::Transform(Arc::new(f)));
        self
    }

    /// Translate a function call to target dialect syntax
    /// Returns None if no translation rule exists (use default rendering).
    pub fn translate(&self, func: &str, args: &[SqlExpr]) -> Option<SqlExpr> {
        let func_lower = func.to_lowercase();
        match self.rules.get(func_lower.as_str()) {
            Some(Translation::Rename(new_name)) => {
                let sql_args = args
                    .iter()
                    .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg.clone())))
                    .collect();

                let func_args = FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: sql_args,
                    clauses: vec![],
                });

                Some(SqlExpr::Function(Function {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(*new_name))]),
                    args: func_args,
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                    parameters: FunctionArguments::None,
                    uses_odbc_syntax: false,
                }))
            }
            Some(Translation::Transform(f)) => Some(f(args)),
            None => None, // No translation — use default
        }
    }

    /// Check if a function has a translation rule
    pub fn has_rule(&self, func: &str) -> bool {
        self.rules.contains_key(func.to_lowercase().as_str())
    }

    /// Helper to build a function AST node
    pub fn build_func(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
        let sql_args = args
            .into_iter()
            .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
            .collect();

        let func_args = FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: sql_args,
            clauses: vec![],
        });

        SqlExpr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            args: func_args,
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        })
    }
}
