//! # Strake Connectors
//!
//! This crate provides various data source connectors for Strake,
//! integrating with DataFusion for federated query execution.
//!
//! ## Overview
//!
//! Connectors are responsible for:
//! 1. **Discovery**: Identifying tables and schemas in external sources.
//! 2. **Execution**: Providing `TableProvider` implementations that can execute
//!    subqueries on remote engines (e.g., PostgreSQL, SQLite, Iceberg).
//! 3. **Optimization**: Pushing down filters and limits to minimize data transfer.
#![deny(missing_docs)]

/// Schema introspection utilities for discovering tables in external sources.
pub mod introspect;
/// Data source providers for various backends.
pub mod sources;
