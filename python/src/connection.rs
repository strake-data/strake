use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use std::collections::HashMap;

use crate::backend::{Backend, EmbeddedBackend, RemoteBackend};
use crate::errors::{to_py_err, to_py_value_err};

/// A connection to the Strake federation engine.
#[pyclass]
pub struct StrakeConnection {
    backend: Backend,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl StrakeConnection {
    #[new]
    #[pyo3(signature = (dsn_or_config, sources_config = None, api_key = None))]
    fn new(
        dsn_or_config: String,
        sources_config: Option<String>,
        api_key: Option<String>,
    ) -> PyResult<Self> {
        let runtime =
            tokio::runtime::Runtime::new().map_err(|e| to_py_err("Failed to create runtime", e))?;

        let backend =
            if dsn_or_config.starts_with("grpc://") || dsn_or_config.starts_with("grpcs://") {
                // Remote mode
                let client = runtime
                    .block_on(async { RemoteBackend::new(dsn_or_config, api_key).await })
                    .map_err(|e| to_py_err("Remote connection failed", e))?;

                Backend::Remote(Box::new(client))
            } else {
                // Embedded mode
                let engine = runtime
                    .block_on(async { EmbeddedBackend::new(&dsn_or_config, sources_config).await })
                    .map_err(|e| to_py_value_err("Engine initialization failed", e))?;

                Backend::Embedded(engine)
            };

        Ok(Self { backend, runtime })
    }

    /// Execute a SQL query and return results as a PyArrow Table.
    #[pyo3(signature = (query, params = None))]
    fn sql(
        &mut self,
        query: String,
        params: Option<HashMap<String, Py<PyAny>>>,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
        // TODO: validation of params or interpolation logic
        if let Some(_p) = params {
            return Err(to_py_value_err(
                "Parameter binding",
                "Not yet implemented in this version",
            ));
        }

        let (schema, batches) = self
            .runtime
            .block_on(async { self.backend.execute(&query).await })
            .map_err(|e| to_py_err("Query execution failed", e))?;

        // Convert to PyArrow
        let pyarrow = py.import("pyarrow")?;
        let py_schema = schema
            .to_pyarrow(py)
            .map_err(|e| to_py_err("Arrow schema conversion failed", e))?;

        let is_empty = batches.is_empty();

        let mut py_batches = Vec::with_capacity(batches.len());
        for batch in batches {
            let py_batch = batch
                .to_pyarrow(py)
                .map_err(|e| to_py_err("Arrow conversion failed", e))?;
            py_batches.push(py_batch);
        }

        let table = if is_empty {
            pyarrow
                .getattr("Table")?
                .call_method1("from_batches", (py_batches, py_schema))?
        } else {
            // Use schema from batches to avoid nullability mismatch issues between Plan schema and Batch schema
            pyarrow
                .getattr("Table")?
                .call_method1("from_batches", (py_batches,))?
        };

        Ok(table.unbind())
    }

    /// Returns the logical plan of the query without executing it.
    fn trace(&mut self, query: String, _py: Python) -> PyResult<String> {
        self.runtime
            .block_on(async { self.backend.trace(&query).await })
            .map_err(|e| to_py_err("Trace failed", e))
    }

    /// Returns a list of available tables and sources.
    #[pyo3(signature = (table_name = None))]
    fn describe(&mut self, table_name: Option<String>, _py: Python) -> PyResult<String> {
        self.runtime
            .block_on(async { self.backend.describe(table_name).await })
            .map_err(|e| to_py_err("Describe failed", e))
    }

    /// Returns a detailed ASCII tree visualization of the execution plan.
    ///
    /// Shows federation pushdown indicators, join conditions, filter/projection
    /// details, and timing metrics when available.
    fn explain_tree(&mut self, query: String, _py: Python) -> PyResult<String> {
        self.runtime
            .block_on(async { self.backend.explain_tree(&query).await })
            .map_err(|e| to_py_err("Explain tree failed", e))
    }

    // Context Manager Protocol
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) {
        // No cleanup needed for now, but good practice for future
    }
}
