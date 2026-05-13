#![deny(missing_docs)]
//! Core SQL logic for Strake.
//!
//! This crate handles the translation of DataFusion logical plans into:
//! - **SQL Dialects**: Postgres, MySQL, SQLite, Oracle, Snowflake (via `sql_gen`).
//! - **Substrait**: Binary query plans for DuckDB and other compliant engines (via `substrait_producer`).
//!
//! It also contains the optimizer rules used to validate query costs and ensure safe execution `optimizer`.
/// Smart router for choosing the correct SQL dialect or pushdown strategy.
pub mod dialect_router;
/// Custom SQL dialect implementations.
pub mod dialects;
/// Custom query optimization rules for SQL generation.
pub mod optimizer;
/// High-level entry points for SQL generation.
pub mod sql_gen;
/// Core SQL AST translation logic.
pub mod sql_generator;
/// Substrait plan producer for DuckDB.
pub mod substrait_producer;

pub use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
pub use datafusion_federation::FederatedPlanner;
