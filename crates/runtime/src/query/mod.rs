//! Query execution utilities and infrastructure.
//!
//! This module contains components that support the query execution lifecycle:
//!
//! - **Cache**: Query result caching with TTL and size limits.
//! - **Cost Validator**: Estimating query cost and enforcing limits.
//! - **Planner**: Specialized query planning logic (mostly standard DataFusion, but extensible).
//! - **Trace**: `EXPLAIN ANALYZE` like tracing for debugging performance.
//! - **Plan Tree**: ASCII tree visualization of execution plans with federation details.

pub mod cache;
pub mod cost_validator;
pub mod plan_tree;
pub mod planner;
pub mod trace;
