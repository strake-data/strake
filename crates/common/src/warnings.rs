//! # Query Warnings
//!
//! Task-local warning collection for DataFusion execution.
//!
//! This module provides a mechanism to collect non-fatal warnings (e.g., schema drift)
//! during query execution and propagate them back to the user context.

use std::sync::{Arc, Mutex};

tokio::task_local! {
    pub static QUERY_WARNINGS: Arc<Mutex<Vec<String>>>;
}

/// Helper to add a warning to the current task's warning list, if active.
pub fn add_warning(warning: String) {
    if let Ok(warnings) = QUERY_WARNINGS.try_with(|w: &Arc<Mutex<Vec<String>>>| w.clone()) {
        if let Ok(mut lock) = warnings.lock() {
            lock.push(warning);
        }
    }
}
