use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AuthenticatedUser {
    pub id: String,
    pub permissions: Vec<String>,
    // table_name -> (rls_filter, masking_rules)
    pub rules: std::collections::HashMap<String, (Option<String>, Option<serde_json::Value>)>,
}

impl AuthenticatedUser {
    /// Returns true if the user has the specified permission.
    /// Supports wildcards, e.g., 'governance:*' matches 'governance:contracts:read'.
    ///
    /// # Security
    /// Admin permissions (`admin` or `system:admin`) bypass all checks.
    /// This bypass is logged for audit purposes.
    pub fn has_permission(&self, permission: &str) -> bool {
        if self
            .permissions
            .iter()
            .any(|p| p == "admin" || p == "system:admin")
        {
            tracing::info!(
                user_id = %self.id,
                permission = %permission,
                "Permission granted via admin bypass"
            );
            return true;
        }

        for p in &self.permissions {
            if p == permission {
                return true;
            }

            if p.ends_with(":*") {
                let prefix = &p[..p.len() - 1]; // "governance:"
                if permission.starts_with(prefix) {
                    return true;
                }
            }

            if p == "*" {
                return true;
            }
        }

        false
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_permission_basic() {
        let user = AuthenticatedUser {
            permissions: vec!["read".to_string(), "write".to_string()],
            ..Default::default()
        };
        assert!(user.has_permission("read"));
        assert!(user.has_permission("write"));
        assert!(!user.has_permission("delete"));
    }

    #[test]
    fn test_has_permission_wildcard() {
        let user = AuthenticatedUser {
            permissions: vec!["governance:*".to_string()],
            ..Default::default()
        };
        assert!(user.has_permission("governance:contracts:read"));
        assert!(user.has_permission("governance:any"));
        assert!(!user.has_permission("other:read"));
    }

    #[test]
    fn test_has_permission_global_wildcard() {
        let user = AuthenticatedUser {
            permissions: vec!["*".to_string()],
            ..Default::default()
        };
        assert!(user.has_permission("anything"));
    }

    #[test]
    fn test_has_permission_admin_bypass() {
        let user = AuthenticatedUser {
            id: "test-user".to_string(),
            permissions: vec!["admin".to_string()],
            ..Default::default()
        };
        assert!(user.has_permission("any:action"));

        let system_admin = AuthenticatedUser {
            id: "sys-admin".to_string(),
            permissions: vec!["system:admin".to_string()],
            ..Default::default()
        };
        assert!(system_admin.has_permission("any:action"));
    }
}
