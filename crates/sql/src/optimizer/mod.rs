//! Custom query optimization rules.
//!
//! Strake injects custom rules into the DataFusion optimizer pipeline to support
//! federation and safe resource limits.
//!
//! # Rules
//!
//! - `DefensiveLimitRule`: Injects a `LIMIT` clause into queries that lack one, preventing runaway data fetches.
//! - `CostBasedValidator`: Analyzing the physical plan *after* optimization to reject expensive queries based on estimated rows/bytes.
//! - `FederationOptimizerRule`: (External crate) Responsible for pushing down subqueries to remote sources.

/// Rule for injecting defensive limits to prevent runaway queries.
pub mod defensive_trace;
/// Rule for distinct key pushdown.
pub mod distinct_decorrelation;
/// Rule for flattening join trees into N-way joins.
pub mod flatten_federated;
/// Custom logical node for representing N-way joins.
pub mod join_flattener;
