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
    pub static QUERY_WARNINGS: Arc<Mutex<Vec<String>>>;
}

/// Thread-safe collector for non-fatal warnings, intended to be stored in
/// `TaskContext` extensions to survive `tokio::spawn` boundaries.
#[derive(Debug, Clone, Default)]
pub struct WarningCollector(Arc<Mutex<Vec<String>>>);

impl WarningCollector {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    pub fn add(&self, warning: String) {
        if let Ok(mut lock) = self.0.lock() {
            lock.push(warning);
        }
    }

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

    fn set(&mut self, _key: &str, _value: &str) -> datafusion::common::Result<()> {
        Ok(())
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
    if let Ok(warnings) = QUERY_WARNINGS.try_with(|w: &Arc<Mutex<Vec<String>>>| w.clone()) {
        if let Ok(mut lock) = warnings.lock() {
            lock.push(warning);
        }
    }
}
