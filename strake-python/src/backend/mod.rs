use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;

pub mod embedded;
pub mod remote;

pub use embedded::EmbeddedBackend;
pub use remote::RemoteBackend;

/// Common trait for backends (optional, but good for structure)
#[async_trait]
pub trait StrakeQueryExecutor: Send + Sync {
    async fn execute(&mut self, query: &str) -> anyhow::Result<(Arc<Schema>, Vec<RecordBatch>)>;
    /// Returns the logical plan of the query without executing it
    async fn trace(&mut self, query: &str) -> anyhow::Result<String>;
    /// Returns a list of available tables and sources
    async fn describe(&mut self, table_name: Option<String>) -> anyhow::Result<String>;
}

/// The backend implementations available
pub enum Backend {
    Embedded(EmbeddedBackend),
    Remote(Box<RemoteBackend>),
}

impl Backend {
    pub async fn execute(
        &mut self,
        query: &str,
    ) -> anyhow::Result<(Arc<Schema>, Vec<RecordBatch>)> {
        match self {
            Backend::Embedded(b) => b.execute(query).await,
            Backend::Remote(b) => b.execute(query).await,
        }
    }

    pub async fn trace(&mut self, query: &str) -> anyhow::Result<String> {
        match self {
            Backend::Embedded(b) => b.trace(query).await,
            Backend::Remote(b) => b.trace(query).await,
        }
    }

    pub async fn describe(&mut self, table_name: Option<String>) -> anyhow::Result<String> {
        match self {
            Backend::Embedded(e) => e.describe(table_name).await,
            Backend::Remote(r) => r.describe(table_name).await,
        }
    }
}
