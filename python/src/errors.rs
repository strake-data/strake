use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyErr;

/// Helper to convert errors to Python Runtime errors
pub fn to_py_err(msg: impl Into<String>, e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("{} : {}", msg.into(), e))
}

/// Helper to convert errors to Python Value errors
pub fn to_py_value_err(msg: impl Into<String>, e: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(format!("{} : {}", msg.into(), e))
}
