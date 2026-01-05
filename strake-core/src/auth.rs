use serde::{Serialize, Deserialize};
use datafusion::common::config::{ConfigExtension, ExtensionOptions, ConfigEntry};
use std::any::Any;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AuthenticatedUser {
    pub id: String,
    pub permissions: Vec<String>,
    // table_name -> (rls_filter, masking_rules)
    pub rules: std::collections::HashMap<String, (Option<String>, Option<serde_json::Value>)>,
}

impl ExtensionOptions for AuthenticatedUser {
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

impl ConfigExtension for AuthenticatedUser {
    const PREFIX: &'static str = "strake";
}
