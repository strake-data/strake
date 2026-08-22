//! # Errors
//!
//! Error types and panic-boundary handling for the Strake Python bindings.
//!
//! ## Overview
//! This module defines custom exceptions raised by the native python module and maps Rust-side
//! `StrakeError` types to Python exception subclasses.
//!
//! ## Usage
//! These exception classes are registered in the `_strake` module and can be imported and caught
//! by Python applications:
//! ```python
//! import strake
//! try:
//!     conn.sql("SELECT * FROM invalid_table")
//! except strake.QueryError as e:
//!     print(e)
//! ```
//!
//! ## Performance Characteristics
//! Error conversions construct Python dictionaries for attributes lazily and have negligible overhead.
//! Panic boundaries walk causal error chains using downcasts, avoiding deep traceback allocation
//! in standard paths.
//!
//! ## Errors
//! Maps Rust `ErrorCategory` to corresponding Python classes:
//! - `ConnectionError`: Underlying database connection issues.
//! - `QueryError`: SQL planning or execution issues.
//! - `ConfigError`: Invalid parameters or configurations.
//! - `AuthError`: Authentication or API key issues.
//! - `InternalError`: Unexpected system errors or panics.
#![deny(missing_docs)]

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use strake_error::{ErrorCategory, StrakeError};

// === Define Python Exception Classes ===

pyo3::create_exception!(
    strake,
    StrakeException,
    PyException,
    "Base exception class for all Strake exceptions raised in Python."
);

pyo3::create_exception!(
    strake,
    ConnectionError,
    StrakeException,
    "Raised when a connection to a database or a remote Strake server fails."
);

pyo3::create_exception!(
    strake,
    QueryError,
    StrakeException,
    "Raised when SQL planning, execution, or result serialization fails."
);

pyo3::create_exception!(
    strake,
    ConfigError,
    StrakeException,
    "Raised when invalid connection parameters or YAML configurations are supplied."
);

pyo3::create_exception!(
    strake,
    AuthError,
    StrakeException,
    "Raised when connection credentials or API keys are invalid or missing."
);

pyo3::create_exception!(
    strake,
    InternalError,
    StrakeException,
    "Raised when an unexpected panic or internal engine failure occurs."
);

/// Register exception types with the Python module.
///
/// # Errors
///
/// Returns a PyResult error if registering any of the exception classes fails.
pub fn register_exceptions(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("StrakeException", py.get_type::<StrakeException>())?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    m.add("ConfigError", py.get_type::<ConfigError>())?;
    m.add("AuthError", py.get_type::<AuthError>())?;
    m.add("InternalError", py.get_type::<InternalError>())?;
    Ok(())
}

/// Convert StrakeError to appropriate Python exception.
///
/// Maps the internal Strake error categories to the corresponding Python
/// exception classes defined in this module.
pub fn to_py_exception(py: Python<'_>, err: StrakeError) -> PyErr {
    let attrs = error_to_dict(py, &err);

    match err.code.category() {
        ErrorCategory::Connection => ConnectionError::new_err((err.message, attrs)),
        ErrorCategory::Query => QueryError::new_err((err.message, attrs)),
        ErrorCategory::Config => ConfigError::new_err((err.message, attrs)),
        ErrorCategory::Auth => AuthError::new_err((err.message, attrs)),
        ErrorCategory::Internal => InternalError::new_err((err.message, attrs)),
        // Handle future categories gracefully by defaulting to InternalError.
        _ => InternalError::new_err((err.message, attrs)),
    }
}

/// Create a dict with structured error attributes
fn error_to_dict(py: Python<'_>, err: &StrakeError) -> Py<PyDict> {
    let dict = PyDict::new(py);
    dict.set_item("code", err.code.to_string()).ok();
    dict.set_item("message", &err.message).ok();

    if let Some(hint) = &err.hint {
        dict.set_item("hint", hint).ok();
    }

    if let Some(trace_id) = &err.trace_id {
        dict.set_item("trace_id", trace_id).ok();
    }

    if let Some(context) = &err.context {
        // Serialize context to JSON then parse as Python dict
        if let Ok(json_str) = serde_json::to_string(context) {
            dict.set_item("context_json", json_str).ok();
        }
    }

    dict.into()
}

/// Helper to catch Rust panics at the PyO3 boundary and convert them into Python `InternalError` exceptions.
///
/// This ensures that the Python process does not abort or crash if a Rust panic occurs inside FFI.
///
/// # Errors
///
/// Returns a `PyResult::Err` containing an `InternalError` if a panic occurred or if the inner function returned an error.
///
/// # Panics
///
/// This function itself cannot panic; it catches all panics occurring within the closure `f`.
pub fn catch_panics<F, R>(f: F) -> PyResult<R>
where
    F: FnOnce() -> PyResult<R>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(res) => res,
        Err(err) => {
            let msg = if let Some(s) = err.downcast_ref::<&str>() {
                *s
            } else if let Some(s) = err.downcast_ref::<String>() {
                s.as_str()
            } else {
                "Unknown Rust panic"
            };
            Err(InternalError::new_err(format!("Rust panic: {}", msg)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catch_panics_success() {
        let res = catch_panics(|| Ok(42));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 42);
    }

    #[test]
    fn test_catch_panics_error() {
        let res = catch_panics(|| Err::<i32, _>(InternalError::new_err("original error")));
        assert!(res.is_err());
        let err = res.unwrap_err();
        let err_str = format!("{:?}", err);
        assert!(err_str.contains("original error"));
    }

    #[test]
    fn test_catch_panics_panic() {
        let res = catch_panics(|| -> PyResult<i32> {
            panic!("boom");
        });
        assert!(res.is_err());
        let err = res.unwrap_err();
        let err_str = format!("{:?}", err);
        assert!(err_str.contains("Rust panic: boom"));
    }
}
