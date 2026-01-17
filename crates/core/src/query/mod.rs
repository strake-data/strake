//! Query execution utilities and infrastructure.
//!
//! this module contains components that support the query execution lifecycle:
//!
//! - **Cache**: Query result caching with TTL and size limits.
//! - **Circuit Breaker**: Preventing system overload by fast-failing queries.
//! - **Cost Validator**: Estimating query cost and enforcing limits.
//! - **Planner**: specialized query planning logic (mostly standard DataFusion, but extensible).
//! - **Trace**: `EXPLAIN ANALYZE` like tracing for debugging performance.

pub mod cache;
pub mod circuit_breaker;
pub mod cost_validator;
pub mod planner;
pub mod trace;
