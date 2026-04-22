//! # Warning DataFusion Extension
//!
//! Provides the DataFusion extension wrapper for the `WarningCollector`.

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use std::any::Any;
use strake_common::warnings::WarningCollector;

/// DataFusion session extension wrapper for `WarningCollector`.
#[derive(Debug, Clone)]
pub struct WarningExtension {
    /// The warning collector instance.
    pub collector: WarningCollector,
}

impl ExtensionOptions for WarningExtension {
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
            "WarningExtension does not support configuration key '{}'",
            key
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl ConfigExtension for WarningExtension {
    const PREFIX: &'static str = "strake_warnings";
}
