//! Python bindings for Strake.
//!
//! This crate implements the `_strake` extension module, which provides the
//! high-performance Rust core logic to the Strake Python SDK.
//!
//! # Components
//!
//! - `StrakeConnection`: The main entry point, analogous to the `StrakeContext` in Rust.
//! - `backend`: Internal backend abstractions.
//!
//! # Usage (Python side)
//!
//! ```python
//! import _strake
//! conn = _strake.StrakeConnection("postgres://...")
//! df = conn.sql("SELECT * FROM table")
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
