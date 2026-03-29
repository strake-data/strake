//! # Query Warnings
//!
//! Task-local warning collection for DataFusion execution.
//!
//! This module provides a mechanism to collect non-fatal warnings (e.g., schema drift)
//! during query execution and propagate them back to the user context.

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use std::any::Any;
use std::sync::{Arc, Mutex};

tokio::task_local! {
    /// Task-local storage for non-fatal warnings collected during query execution.
    pub static QUERY_WARNINGS: Arc<Mutex<Vec<String>>>;
}

/// Thread-safe collector for non-fatal warnings, intended to be stored in
/// `TaskContext` extensions to survive `tokio::spawn` boundaries.
#[derive(Debug, Clone, Default)]
pub struct WarningCollector(Arc<Mutex<Vec<String>>>);

impl WarningCollector {
    /// Creates a new, empty `WarningCollector`.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    /// Access the inner storage for use with task-local storage.
    pub fn inner(&self) -> Arc<Mutex<Vec<String>>> {
        self.0.clone()
    }

    /// Adds a warning string to the collector.
    pub fn add(&self, warning: String) {
        if let Ok(mut lock) = self.0.lock() {
            lock.push(warning);
        }
    }

    /// Removes and returns all collected warnings.
    /// Returns an empty vector if the lock is poisoned.
    pub fn take_all(&self) -> Vec<String> {
        if let Ok(mut lock) = self.0.lock() {
            std::mem::take(&mut *lock)
        } else {
            vec![]
        }
    }
}

impl ExtensionOptions for WarningCollector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> datafusion::common::Result<()> {
        Err(datafusion::common::DataFusionError::Configuration(format!(
            "WarningCollector does not support configuration key '{}'",
            key
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl ConfigExtension for WarningCollector {
    const PREFIX: &'static str = "strake";
}

/// Helper to add a warning to the current task's warning list, if active.
pub fn add_warning(warning: String) {
    if let Ok(warnings) = QUERY_WARNINGS.try_with(|w: &Arc<Mutex<Vec<String>>>| w.clone())
        && let Ok(mut lock) = warnings.lock()
    {
        lock.push(warning);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_warning_collector() {
        let collector = WarningCollector::new();
        collector.add("test warning".to_string());
        let warnings = collector.take_all();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], "test warning");

        // Should be empty after take_all
        assert!(collector.take_all().is_empty());
    }

    #[tokio::test]
    async fn test_add_warning_task_local() {
        let collector = Arc::new(Mutex::new(Vec::new()));
        let c_clone = Arc::clone(&collector);

        QUERY_WARNINGS
            .scope(c_clone, async {
                add_warning("async warning".to_string());
            })
            .await;

        let lock = collector.lock().unwrap();
        assert_eq!(lock.len(), 1);
        assert_eq!(lock[0], "async warning");
    }
}
