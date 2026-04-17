/// Translation context for tracking scopes and aliases.
pub mod context;
/// Dialect-specific capabilities and type mappings.
pub mod dialect;
/// Errors encountered during SQL generation.
pub mod error;
/// Expression translation logic.
pub mod expr;
/// Identifier validation and sanitization.
pub mod sanitize;
/// Logical plan to SQL AST translation.
pub mod translator;

pub use self::translator::SqlGenerator;
pub use context::GeneratorContext;
