//! Strake Core: High-performance federated SQL engine.
//!
//! This crate provides the core DataFusion-based query engine that powers
//! Strake's federation capabilities across disparate data sources.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐
//! │ Federation  │
//! │   Engine    │
//! └──────┬──────┘
//!        │
//!   ┌────┴─────┐
//!   │ Sources  │ (Postgres, S3, HTTP, Snowflake)
//!   └──────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! // use strake_core::federation::FederationEngine; // Example usage
//! ```

pub mod auth;
pub mod config;
pub mod dialect_router;
pub mod dialects;
pub mod error;
pub mod federation;
pub mod models;
pub mod optimizer;
pub mod query;
pub mod sidecar;
pub mod sources;
pub mod sql_gen;
pub mod substrait_producer;
pub mod telemetry;
