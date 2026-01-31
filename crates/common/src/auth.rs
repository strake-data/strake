use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskingRule {
    Redact,
    Hash,
    KeepFirst(usize),
    Default(String),
    // Allow raw value for flexibility until strict schema is defined
    Custom(serde_json::Value),
}

#[derive(Clone, Debug, Default)]
pub struct PermissionSet {
    exact: HashSet<String>,
    prefixes: Vec<String>, // Prefixes without the trailing :*
    is_admin: bool,
    global_wildcard: bool,
    // Original raw permissions for serialization/display
    pub raw: Vec<String>,
}

impl Serialize for PermissionSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.raw.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PermissionSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw: Vec<String> = Vec::deserialize(deserializer)?;
        Ok(PermissionSet::from(raw))
    }
}

impl From<Vec<String>> for PermissionSet {
    fn from(perms: Vec<String>) -> Self {
        let mut exact = HashSet::new();
        let mut prefixes = Vec::new();
        let mut is_admin = false;
        let mut global_wildcard = false;

        for p in &perms {
            match p.as_str() {
                "admin" | "system:admin" => is_admin = true,
                "*" => global_wildcard = true,
                s if s.ends_with(":*") => {
                    let prefix = &s[..s.len() - 2];
                    prefixes.push(prefix.to_string());
                }
                _ => {
                    exact.insert(p.clone());
                }
            }
        }

        Self {
            exact,
            prefixes,
            is_admin,
            global_wildcard,
            raw: perms,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TableRules {
    pub rls_filter: Option<String>,
    pub masking: Option<HashMap<String, MaskingRule>>,
}

impl From<(Option<String>, Option<HashMap<String, MaskingRule>>)> for TableRules {
    fn from(val: (Option<String>, Option<HashMap<String, MaskingRule>>)) -> Self {
        Self {
            rls_filter: val.0,
            masking: val.1,
        }
    }
}

impl From<TableRules> for (Option<String>, Option<HashMap<String, MaskingRule>>) {
    fn from(rules: TableRules) -> Self {
        (rules.rls_filter, rules.masking)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AuthenticatedUser {
    pub id: String,
    pub permissions: PermissionSet,
    // table_name -> rules
    pub rules: HashMap<String, TableRules>,
}

impl AuthenticatedUser {
    /// Returns true if the user has the specified permission.
    /// Supports wildcards, e.g., 'governance:*' matches 'governance:contracts:read'.
    ///
    /// # Security
    /// Admin permissions (`admin` or `system:admin`) bypass all checks.
    /// This bypass is logged for audit purposes.
    pub fn has_permission(&self, permission: &str) -> bool {
        // Optimized check
        if self.permissions.is_admin {
            tracing::info!(
                target: "strake::audit",
                user_id = %self.id,
                permission = %permission,
                action = "admin_bypass",
                "Permission granted via admin bypass"
            );
            return true;
        }

        if self.permissions.global_wildcard {
            return true;
        }

        if self.permissions.exact.contains(permission) {
            return true;
        }

        // Check prefixes
        // Optimization: Prefixes derived from "foo:*" so we look for "foo" prefix
        // and ensure the next char is ':' if strictly matching hierarchy.
        // Original code: p.ends_with(":*") -> prefix = p[..len-1] (includes colon).
        // My PermissionSet parsing removed ":*", so `prefixes` are e.g "governance".
        // If permission is "governance:contracts:read", starts_with("governance") is true.
        // But we want to ensure boundary. "governance" should match "governance:..." but not "governance_plus".
        // So we check if permission starts with "prefix" + ":".

        for prefix in &self.permissions.prefixes {
            if permission.starts_with(prefix) {
                // Strict hierarchy: "foo:*" matches "foo:bar" but not "foo"
                if let Some(rest) = permission.get(prefix.len()..) {
                    if rest.starts_with(':') {
                        return true;
                    }
                }
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
            permissions: PermissionSet::from(vec!["read".to_string(), "write".to_string()]),
            ..Default::default()
        };
        assert!(user.has_permission("read"));
        assert!(user.has_permission("write"));
        assert!(!user.has_permission("delete"));
    }

    #[test]
    fn test_has_permission_wildcard() {
        let user = AuthenticatedUser {
            permissions: PermissionSet::from(vec!["governance:*".to_string()]),
            ..Default::default()
        };
        assert!(user.has_permission("governance:contracts:read"));
        assert!(user.has_permission("governance:any"));

        // Edge case: "governance" does not match "governance:*" in typical prefix logic unless explicitly allowed.
        // My logic: prefix="governance", permission="governance".
        // Logic: starts_with("governance") -> true. rest="". rest.starts_with(':') -> false.
        // Original logic: "governance:" (len 11). permission "governance" (len 10). starts_with -> false.
        // So behavior is preserved.
        assert!(!user.has_permission("governance"));

        assert!(!user.has_permission("other:read"));
    }

    #[test]
    fn test_has_permission_global_wildcard() {
        let user = AuthenticatedUser {
            permissions: PermissionSet::from(vec!["*".to_string()]),
            ..Default::default()
        };
        assert!(user.has_permission("anything"));
    }

    #[test]
    fn test_has_permission_admin_bypass() {
        let user = AuthenticatedUser {
            id: "test-user".to_string(),
            permissions: PermissionSet::from(vec!["admin".to_string()]),
            ..Default::default()
        };
        assert!(user.has_permission("any:action"));

        let system_admin = AuthenticatedUser {
            id: "sys-admin".to_string(),
            permissions: PermissionSet::from(vec!["system:admin".to_string()]),
            ..Default::default()
        };
        assert!(system_admin.has_permission("any:action"));
    }
}
