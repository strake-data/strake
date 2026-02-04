//! Core SQL logic for Strake.
//!
//! This crate handles the translation of DataFusion logical plans into:
//! - **SQL Dialects**: Postgres, MySQL, SQLite, Oracle, Snowflake (via `sql_gen`).
//! - **Substrait**: Binary query plans for DuckDB and other compliant engines (via `substrait_producer`).
//!
//! It also contains the optimizer rules used to validate query costs and ensure safe execution `optimizer`.
pub mod dialect_router;
pub mod dialects;
pub mod optimizer;
pub mod schema_adapter;
pub mod sql_gen;
pub mod sql_generator;
pub mod substrait_producer;

pub use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
pub use datafusion_federation::FederatedPlanner;
