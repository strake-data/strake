#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
//! Common utilities, types, and configurations shared across Strake crates.
//!
//! This crate contains the base building blocks for the Strake system, including:
//! - **Configuration**: Strongly typed application configuration ([`config`]).
//!
//! ```rust
//! # use strake_common::config::AppConfig;
//! # use std::fs;
//! # let temp = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
//! # fs::write(&temp, "server:\n  listen_addr: 127.0.0.1:50051").unwrap();
//! let config = AppConfig::from_file(temp.path().to_str().unwrap()).unwrap();
//! assert_eq!(config.server.listen_addr, "127.0.0.1:50051");
//! ```
//! - **Authentication**: User context and RLS rules ([`auth`]).
//! - **Error Handling**: Unified error types (`error`).
//! - **Telemetry**: Observability setup (`telemetry`).
//! - **Resilience**: Circuit breakers for fault tolerance (`circuit_breaker`).
//! - **Logging**: Contextual warning collection (`warnings`).
//!
//! ## Safety
//!
//! This crate uses `#! [deny(unsafe_code)]` and aims for 100% safe Rust.
//! Use of `Atomic` types and `RwLock` ensures thread-safety without manual memory management.
//!
//! ## Errors
//!
//! Most operations in this crate return a `strake_common::error::Result`, which is a
//! type alias for `anyhow::Result` mapped to `StrakeError` context.
//!
//! ## Performance Characteristics
//!
//! - **Locking**: Uses `tokio::sync::RwLock` and `DashMap` for high-concurrency access to cache structures.
//! - **Allocation**: Minimizes hot-path allocations by using `Arc<str>` for repeated strings (e.g., file paths, permissions).
//! - **Metrics**: Lightweight telemetry hooks use atomic counters to minimize overhead during query execution.
/// Authentication and authorization types.
pub mod auth;
/// Resilience patterns like circuit breakers.
pub mod circuit_breaker;
/// Centralized configuration management.
pub mod config;
/// Unified error handling.
pub mod error;
/// Data models for configurations and results.
pub mod models;
/// Predicate caching for improved query performance on large data sources.
pub mod predicate_cache;
/// Retry logic for fallible operations.
pub mod retry;
/// Database and file schema introspection.
pub mod schema;
/// Sensitive data scrubbing.
pub mod scrubber;
/// Observability and telemetry.
pub mod telemetry;
/// Contextual warning collection.
pub mod warnings;
