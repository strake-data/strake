#![deny(missing_docs)]
#![warn(rustdoc::all)]

//! Strake Core: High-performance federated SQL engine.
//!
//! This crate provides the core DataFusion-based query engine that powers
//! Strake's federation capabilities across disparate data sources.
//!
//! # Architecture
//!
//! The engine is designed as a set of modules:
//! - **FederationEngine**: Coordination and entry point.
//! - **ExecutionOrchestrator**: Policy-driven query lifecycle management.
//! - **QueryPipeline**: Stateless physical execution.
//! - **SessionManager**: Context reconstruction and extension management.
//!
//! # Example
//!
//! ```rust
//! // Example of creating a federation engine (simplified)
//! // let engine = FederationEngine::new(context, cache, configs, limits);
//! // let (schema, stream, warnings) = engine.execute_query_stream("SELECT * FROM pg.users", None).await?;
//! ```

/// DataFusion state extensions.
pub mod extensions;
/// Core federation engine.
pub mod federation;
/// Custom optimizer rules.
pub mod optimizer;
/// Query execution utilities.
pub mod query;
/// Local management sidecar.
pub mod sidecar;
