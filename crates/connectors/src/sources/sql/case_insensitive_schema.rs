use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use std::sync::Arc;

/// A `SchemaProvider` that wraps a `MemorySchemaProvider` and provides
/// case-insensitive table lookups. This is essential for compatibility with
/// DataFusion v53's identifier normalization when dealing with case-sensitive
/// databases like Oracle where metadata might be uppercase.
#[derive(Debug)]
pub struct CaseInsensitiveSchemaProvider {
    inner: MemorySchemaProvider,
}

impl Default for CaseInsensitiveSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CaseInsensitiveSchemaProvider {
    /// Creates a new `CaseInsensitiveSchemaProvider`.
    pub fn new() -> Self {
        Self {
            inner: MemorySchemaProvider::new(),
        }
    }
}

#[async_trait]
impl SchemaProvider for CaseInsensitiveSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // 1. Try exact match first (preserves case if quoted in SQL)
        if let Some(t) = self.inner.table(name).await? {
            return Ok(Some(t));
        }

        // 2. Try uppercase match (typical for Oracle unquoted metadata)
        let upper = name.to_uppercase();
        if let Some(t) = self.inner.table(&upper).await? {
            tracing::debug!(
                requested_name = name,
                resolved_name = upper,
                "CaseInsensitiveSchemaProvider: resolved table via uppercase fallback"
            );
            return Ok(Some(t));
        }

        // 3. Try lowercase match (just in case)
        let lower = name.to_lowercase();
        if let Some(t) = self.inner.table(&lower).await? {
            tracing::debug!(
                requested_name = name,
                resolved_name = lower,
                "CaseInsensitiveSchemaProvider: resolved table via lowercase fallback"
            );
            return Ok(Some(t));
        }

        Ok(None)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        tracing::debug!(table_name = %name, "CaseInsensitiveSchemaProvider: registering table");
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
            || self.inner.table_exist(&name.to_uppercase())
            || self.inner.table_exist(&name.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;

    #[tokio::test]
    async fn test_case_insensitive_lookup() {
        let provider = CaseInsensitiveSchemaProvider::new();

        let schema = Arc::new(Schema::empty());
        let table = Arc::new(EmptyTable::new(schema));

        provider
            .register_table("SYSTEM.STRAKE_TEST_USERS".to_string(), table.clone())
            .unwrap();

        // Exact match
        assert!(
            provider
                .table("SYSTEM.STRAKE_TEST_USERS")
                .await
                .unwrap()
                .is_some()
        );

        // Lowercase match (simulate DataFusion normalization)
        assert!(
            provider
                .table("system.strake_test_users")
                .await
                .unwrap()
                .is_some()
        );

        // Non-existent
        assert!(provider.table("nonexistent").await.unwrap().is_none());
    }
}
