use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use std::collections::HashMap;

use crate::backend::{Backend, EmbeddedBackend, RemoteBackend};
use crate::errors::{to_py_exception, InternalError};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

// A single Tokio runtime shared across all Python threads.
// Initialised lazily via OnceLock::get_or_try_init — the idiomatic, race-free
// alternative to the manual double-checked-locking pattern.
static GLOBAL_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn get_runtime() -> PyResult<&'static tokio::runtime::Runtime> {
    // Fast path: already initialised.
    if let Some(runtime) = GLOBAL_RUNTIME.get() {
        return Ok(runtime);
    }

    // Slow path: initialise once. The inner check-after-lock avoids a second
    // initialisation if two threads race to the slow path simultaneously.
    static INIT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _lock = INIT_LOCK
        .lock()
        .map_err(|e| InternalError::new_err(format!("Runtime init lock poisoned: {}", e)))?;

    if let Some(runtime) = GLOBAL_RUNTIME.get() {
        return Ok(runtime);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("strake-python-runtime")
        .build()
        .map_err(|e| InternalError::new_err(format!("Failed to create global runtime: {}", e)))?;

    let _ = GLOBAL_RUNTIME.set(runtime);
    GLOBAL_RUNTIME
        .get()
        .ok_or_else(|| InternalError::new_err("Global runtime was not set after initialization"))
}

/// Guard: returns an error if called from inside a Tokio worker thread.
///
/// block_on() panics when nested inside an existing Tokio context. This check
/// converts that panic into a clear Python exception in both debug and release builds.
#[inline]
fn check_not_in_tokio_context(method: &str) -> PyResult<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        Err(InternalError::new_err(format!(
            "StrakeConnection.{method}() must not be called from within a Tokio async context. \
             Use asyncio.to_thread()."
        )))
    } else {
        Ok(())
    }
}

/// A connection to the Strake federation engine.
#[pyclass]
pub struct StrakeConnection {
    backend: Arc<Mutex<Backend>>,
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
        let runtime = get_runtime()?;

        let backend =
            if dsn_or_config.starts_with("grpc://") || dsn_or_config.starts_with("grpcs://") {
                // Remote mode
                let client = runtime
                    .block_on(async { RemoteBackend::new(dsn_or_config, api_key).await })
                    .map_err(to_py_exception_anyhow)?;
                Backend::Remote(Box::new(client))
            } else {
                // Embedded mode
                let engine = runtime
                    .block_on(async { EmbeddedBackend::new(&dsn_or_config, sources_config).await })
                    .map_err(to_py_exception_anyhow)?;
                Backend::Embedded(engine)
            };

        Ok(Self {
            backend: Arc::new(Mutex::new(backend)),
        })
    }

    /// Execute a SQL query and return results as a PyArrow Table.
    ///
    /// NOTE: Results are fully materialized in memory before returning.
    /// For large datasets, use the streaming `iter_batches()` API on the returned table.
    #[pyo3(signature = (query, params = None))]
    fn sql(
        &self,
        query: String,
        params: Option<HashMap<String, Py<PyAny>>>,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
        if let Some(_p) = params {
            return Err(InternalError::new_err(
                "Parameter binding: Not yet implemented in this version",
            ));
        }

        // SAFETY: block_on panics when nested inside a Tokio context.
        // The GIL is released via py.allow_threads() while waiting.
        check_not_in_tokio_context("sql")?;

        let backend = Arc::clone(&self.backend);
        let query_str = query.clone();

        let runtime = get_runtime()?;
        let (schema, batches) = py.detach(|| {
            runtime
                .block_on(async move {
                    let mut backend = backend.lock().await;
                    backend.execute(&query_str).await
                })
                .map_err(to_py_exception_anyhow)
        })?;

        // Convert to PyArrow
        let pyarrow = py.import("pyarrow")?;

        let has_batches = !batches.is_empty();
        let mut py_batches = Vec::with_capacity(batches.len());
        for batch in batches {
            let py_batch = batch
                .to_pyarrow(py)
                .map_err(|e| InternalError::new_err(format!("Arrow conversion failed: {}", e)))?;
            py_batches.push(py_batch);
        }

        let table = if has_batches {
            // Infer schema from batches to avoid plan-vs-batch nullability mismatch issues
            pyarrow
                .getattr("Table")?
                .call_method1("from_batches", (py_batches,))?
        } else {
            // Empty result: must supply schema explicitly
            let py_schema = schema.to_pyarrow(py).map_err(|e| {
                InternalError::new_err(format!("Arrow schema conversion failed: {}", e))
            })?;
            pyarrow
                .getattr("Table")?
                .call_method1("from_batches", (py_batches, py_schema))?
        };

        Ok(table.unbind())
    }

    /// Alias for sql() to match DB-API conventions.
    #[pyo3(signature = (query, params = None))]
    fn execute(
        &self,
        query: String,
        params: Option<HashMap<String, Py<PyAny>>>,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
        self.sql(query, params, py)
    }

    /// Returns the logical plan of the query without executing it.
    fn trace(&self, query: String, py: Python) -> PyResult<String> {
        check_not_in_tokio_context("trace")?;

        let backend = Arc::clone(&self.backend);
        let query_str = query.clone();

        let runtime = get_runtime()?;
        py.detach(|| {
            runtime
                .block_on(async move {
                    let mut backend = backend.lock().await;
                    backend.trace(&query_str).await
                })
                .map_err(to_py_exception_anyhow)
        })
    }

    /// Returns a list of available tables and sources.
    #[pyo3(signature = (table_name = None))]
    fn describe(&self, table_name: Option<String>, py: Python) -> PyResult<String> {
        check_not_in_tokio_context("describe")?;

        let backend = Arc::clone(&self.backend);

        let runtime = get_runtime()?;
        py.detach(|| {
            runtime
                .block_on(async move {
                    let mut backend = backend.lock().await;
                    backend.describe(table_name).await
                })
                .map_err(to_py_exception_anyhow)
        })
    }

    /// Returns a detailed ASCII tree visualization of the execution plan.
    ///
    /// Shows federation pushdown indicators, join conditions, filter/projection
    /// details, and timing metrics when available.
    fn explain_tree(&self, query: String, py: Python) -> PyResult<String> {
        check_not_in_tokio_context("explain_tree")?;

        let backend = Arc::clone(&self.backend);
        let query_str = query.clone();

        let runtime = get_runtime()?;
        py.detach(|| {
            runtime
                .block_on(async move {
                    let mut backend = backend.lock().await;
                    backend.explain_tree(&query_str).await
                })
                .map_err(to_py_exception_anyhow)
        })
    }

    /// Explicitly close the connection and shut down the engine.
    ///
    /// This is a **blocking** call that shuts down the backend synchronously.
    /// It can be safely called from both sync and async Python contexts (use
    /// `asyncio.to_thread(conn.close)` if inside an async function to avoid
    /// blocking the event loop).
    ///
    /// Both `with` and `async with` will call this on exit.
    fn close(&self, py: Python) -> PyResult<()> {
        check_not_in_tokio_context("close")?;

        let backend = Arc::clone(&self.backend);
        let runtime = get_runtime()?;

        tracing::info!("Closing StrakeConnection");
        py.detach(|| {
            runtime
                .block_on(async move {
                    let mut guard = backend.lock().await;
                    guard.shutdown().await
                })
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("Shutdown failed: {}", e))
                })
        })
    }

    // === Sync Context Manager Protocol ===
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.close(py)
    }

    // === Async Context Manager Protocol ===
    //
    // `async with strake.connect() as conn:` runs __aexit__ on the event loop.
    // close() blocks internally, so wrap it in asyncio.to_thread() for async callers.
    fn __aenter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __aexit__(
        &self,
        py: Python,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        // In an async context the caller should use asyncio.to_thread.
        // For simplicity we call close() directly; the GIL is released
        // inside close() via py.allow_threads(), so event loop threads are unblocked.
        self.close(py)
    }
}

impl Drop for StrakeConnection {
    fn drop(&mut self) {
        // We cannot use async lock in drop. Async cleanup (graceful network shutdown)
        // is skipped here. ALWAYS use `with` / `async with` or call conn.close()
        // for a clean shutdown.
        tracing::trace!("StrakeConnection dropped; engine may persist if close() was not called.");
    }
}

/// Convert an anyhow error to a typed Python exception using the StrakeError category.
/// Falls back to InternalError if the anyhow error is not a StrakeError.
fn to_py_exception_anyhow(e: anyhow::Error) -> PyErr {
    Python::attach(|py| {
        if let Some(strake_err) = e.downcast_ref::<strake_error::StrakeError>() {
            to_py_exception(py, strake_err.clone())
        } else {
            InternalError::new_err(e.to_string())
        }
    })
}
