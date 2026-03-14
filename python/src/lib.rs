#![deny(missing_docs)]

//! Python bindings for Strake.
//!
//! This crate implements the `_strake` extension module, which provides the
//! high-performance Rust core logic to the Strake Python SDK. It leverages
//! DataFusion and Apache Arrow for zero-copy data transfer between Rust
//! and Python.
//!
//! # Safety and Concurrency
//!
//! The extension module uses a global Tokio runtime for asynchronous I/O
//! and high-concurrency workloads. Python's Global Interpreter Lock (GIL)
//! is released during blocking operations.
//!
//! # Components
//!
//! - `StrakeConnection`: The main entry point, analogous to the `StrakeContext` in Rust.
//! - `backend`: Internal backend abstractions for embedded and remote execution.
//!
//! # Usage (Python side)
//!
//! ```python
//! import _strake
//! conn = _strake.StrakeConnection("strake.yaml")
//! table = conn.sql("SELECT * FROM data")
//! ```

use pyo3::prelude::*;

pub mod backend;
pub mod connection;
pub mod errors;

use connection::StrakeConnection;

#[pymodule]
fn _strake(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    m.add_class::<StrakeConnection>()?;
    errors::register_exceptions(_py, m)?;
    Ok(())
}
