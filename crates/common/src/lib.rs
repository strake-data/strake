//! Common utilities, types, and configurations shared across Strake crates.
//!
//! This crate contains the base building blocks for the Strake system, including:
//! - **Configuration**: Strongly typed application configuration (`config`).
//! - **Authentication**: User context and RLS rules (`auth`).
//! - **Error Handling**: Unified error types (`error`).
//! - **Telemetry**: Observability setup (`telemetry`).
//! - **Resilience**: Circuit breakers for fault tolerance (`circuit_breaker`).
//! - **Logging**: Contextual warning collection (`warnings`).
pub mod auth;
pub mod circuit_breaker;
pub mod config;
pub mod error;
pub mod models;
pub mod retry;
pub mod scrubber;
pub mod telemetry;
pub mod warnings;
