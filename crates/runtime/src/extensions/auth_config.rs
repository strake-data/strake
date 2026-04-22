//! # Authentication DataFusion Extension
//!
//! Provides the DataFusion extension wrappers for the `AuthenticatedUser` domain object.

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use std::any::Any;
use strake_common::auth::AuthenticatedUser;

/// DataFusion session extension wrapper for `AuthenticatedUser`.
#[derive(Debug, Clone)]
pub struct AuthExtension {
    /// The user identity.
    pub user: AuthenticatedUser,
}

impl ExtensionOptions for AuthExtension {
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
            "AuthExtension does not support configuration key '{}'",
            key
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl ConfigExtension for AuthExtension {
    const PREFIX: &'static str = "strake"; // maintain same prefix for backwards compatibility
}
