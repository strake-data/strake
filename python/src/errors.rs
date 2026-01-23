use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use strake_error::{ErrorCategory, StrakeError};

// === Define Python Exception Classes ===

pyo3::create_exception!(strake, StrakeException, PyException);
pyo3::create_exception!(strake, ConnectionError, StrakeException);
pyo3::create_exception!(strake, QueryError, StrakeException);
pyo3::create_exception!(strake, ConfigError, StrakeException);
pyo3::create_exception!(strake, AuthError, StrakeException);
pyo3::create_exception!(strake, InternalError, StrakeException);

/// Register exception types with the Python module
pub fn register_exceptions(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("StrakeException", py.get_type::<StrakeException>())?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    m.add("ConfigError", py.get_type::<ConfigError>())?;
    m.add("AuthError", py.get_type::<AuthError>())?;
    m.add("InternalError", py.get_type::<InternalError>())?;
    Ok(())
}

/// Convert StrakeError to appropriate Python exception
pub fn to_py_exception(py: Python<'_>, err: StrakeError) -> PyErr {
    let attrs = error_to_dict(py, &err);

    match err.code.category() {
        ErrorCategory::Connection => ConnectionError::new_err((err.message, attrs)),
        ErrorCategory::Query => QueryError::new_err((err.message, attrs)),
        ErrorCategory::Config => ConfigError::new_err((err.message, attrs)),
        ErrorCategory::Auth => AuthError::new_err((err.message, attrs)),
        ErrorCategory::Internal => InternalError::new_err((err.message, attrs)),
        // Handle future categories gracefully
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
