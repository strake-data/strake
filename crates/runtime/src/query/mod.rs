//! Query execution utilities and infrastructure.
//!
//! This module contains components that support the query execution lifecycle,
//! implementing a "deep module" architecture to isolate complex execution concerns.
//!
//! - **Cache**: Transparent query result caching with TTL and size limits.
//! - **Orchestrator**: Policy-driven coordination of caching, budgeting, and execution.
//! - **Pipeline**: Stateless physical execution of DataFusion plans.
//! - **Session**: Management of `SessionContext` with Strake extensions.
//! - **Cost Validator**: Enforcing resource limits and query complexity.
//! - **Plan Tree**: Visualization of federated execution plans.

/// Transparent query result caching.
pub mod cache;
/// Budget-based query validation.
pub mod cost_validator;
/// Policy-driven query execution orchestration.
pub mod orchestrator;
/// Custom physical query optimization rules.
pub mod physical_rules;
/// Stateless query execution pipeline.
pub mod pipeline;
/// Federated plan visualization utilities.
pub mod plan_tree;
/// Federated physical query planning.
pub mod planner;
/// DataFusion session management and extension injection.
pub mod session;
/// Query execution tracing and reporting.
pub mod trace;
