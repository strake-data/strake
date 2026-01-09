use pyo3::prelude::*;

pub mod backend;
pub mod connection;
pub mod errors;

use connection::StrakeConnection;

#[pymodule]
fn _strake(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<StrakeConnection>()?;
    Ok(())
}
