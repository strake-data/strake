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

pub mod defensive_trace;
pub mod flatten_federated;
pub mod join_flattener;
