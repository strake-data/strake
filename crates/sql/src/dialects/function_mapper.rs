//! Function Mapper
//!
//! Declarative registry for translating DataFusion function names to dialect-specific syntax.
//! Inspired by sqlglot's AST transformation pattern.

use std::collections::HashMap;
use std::sync::Arc;

/// Type alias for transform function closures
pub type TransformFn = Arc<dyn Fn(&[String]) -> String + Send + Sync>;

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
        F: Fn(&[String]) -> String + Send + Sync + 'static,
    {
        self.rules.insert(from, Translation::Transform(Arc::new(f)));
        self
    }

    /// Translate a function call to target dialect syntax
    /// Returns None if no translation rule exists (use default rendering)
    pub fn translate(&self, func: &str, args: &[String]) -> Option<String> {
        let func_lower = func.to_lowercase();
        match self.rules.get(func_lower.as_str()) {
            Some(Translation::Rename(new_name)) => {
                Some(format!("{}({})", new_name, args.join(", ")))
            }
            Some(Translation::Transform(f)) => Some(f(args)),
            None => None, // No translation — use default
        }
    }

    /// Check if a function has a translation rule
    pub fn has_rule(&self, func: &str) -> bool {
        self.rules.contains_key(func.to_lowercase().as_str())
    }
}
