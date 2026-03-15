//! # Backends
//!
//! Internal abstraction layer for executing federated queries against various data sources.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;

pub mod embedded;
pub mod remote;

pub use embedded::EmbeddedBackend;
pub use remote::RemoteBackend;

/// Query execution trait implemented by all backend types (Embedded, Remote).
#[async_trait]
pub trait StrakeQueryExecutor: Send + Sync {
    /// Execute a query and return the resulting schema and record batches.
    async fn execute(&mut self, query: &str) -> anyhow::Result<(Arc<Schema>, Vec<RecordBatch>)>;
    /// Returns the logical plan of the query without executing it
    async fn trace(&mut self, query: &str) -> anyhow::Result<String>;
    /// Returns a detailed ASCII tree visualization of the execution plan
    async fn explain_tree(&mut self, query: &str) -> anyhow::Result<String>;
    /// Returns a list of available tables and sources
    async fn describe(&mut self, table_name: Option<String>) -> anyhow::Result<String>;
    /// Returns a JSON string of registered sources and their configurations
    async fn list_sources(&mut self) -> anyhow::Result<String>;
    /// Perform graceful shutdown of the executor and its resources
    async fn shutdown(&mut self) -> anyhow::Result<()>;
}

/// The backend implementations available
pub(crate) enum Backend {
    Embedded(Box<EmbeddedBackend>),
    Remote(Box<RemoteBackend>),
}

#[async_trait]
impl StrakeQueryExecutor for Backend {
    async fn execute(&mut self, query: &str) -> anyhow::Result<(Arc<Schema>, Vec<RecordBatch>)> {
        match self {
            Backend::Embedded(b) => b.execute(query).await,
            Backend::Remote(b) => b.execute(query).await,
        }
    }

    async fn trace(&mut self, query: &str) -> anyhow::Result<String> {
        match self {
            Backend::Embedded(b) => b.trace(query).await,
            Backend::Remote(b) => b.trace(query).await,
        }
    }

    async fn describe(&mut self, table_name: Option<String>) -> anyhow::Result<String> {
        match self {
            Backend::Embedded(e) => e.describe(table_name).await,
            Backend::Remote(r) => r.describe(table_name).await,
        }
    }

    async fn explain_tree(&mut self, query: &str) -> anyhow::Result<String> {
        match self {
            Backend::Embedded(e) => e.explain_tree(query).await,
            Backend::Remote(r) => r.explain_tree(query).await,
        }
    }

    async fn list_sources(&mut self) -> anyhow::Result<String> {
        match self {
            Backend::Embedded(e) => e.list_sources().await,
            Backend::Remote(r) => r.list_sources().await,
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        match self {
            Backend::Embedded(e) => e.shutdown().await,
            Backend::Remote(r) => r.shutdown().await,
        }
    }
}
