//! # Authentication and Authorization
//!
//! This module provides the central framework for managing user identity,
//! hierarchical permissions, and dynamic data masking rules throughout the
//! Strake query engine and CLI.
//!
//! ## Core Components
//!
//! - **Identity**: Represents a logged-in user or system actor, including their
//!   unique identifier and associated roles/groups.
//! - **Permissions**: Fine-grained access control strings (e.g., `tables:read:public.*`)
//!   used to enforce security boundaries at the catalog, schema, and table levels.
//! - **Data Masking**: Policy-driven rules like `Redact`, `Hash`, or `KeepFirst(N)`
//!   that protect sensitive PII and confidential information in query results.
//!
//! ## Usage Example
//!
//! ```rust
//! use strake_common::auth::{AuthenticatedUser, MaskingRule};
//! use strake_common::models::ActorName;
//!
//! // Create a new identity with specific permissions
//! let mut user = AuthenticatedUser::default();
//! user.id = ActorName::from("alice");
//! user.permissions = vec!["sales:*".to_string()].into();
//!
//! // Check if the user has permission to read a specific table
//! if user.has_permission("sales:deals:read") {
//!     println!("Access granted");
//! }
//! ```
//!
//! ## Safety and Compliance
//!
//! Identity documentation and permission strings are designed to be easily
//! auditable. All sensitive identity metadata should be integrated with
//! the `secrecy` crate where appropriate to prevent accidental logging or
//! memory exposure.

use crate::models::ActorName;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};

/// Rules for masking sensitive data in query results.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskingRule {
    /// Completely redact the value, replacing it with a fixed placeholder or null.
    Redact,
    /// Apply a cryptographic hash to the value to preserve uniqueness while obscuring the original data.
    Hash,
    /// Keep only the first N characters of the value and redact any remaining characters.
    KeepFirst(usize),
    /// Replace the value with a specified default string value.
    Default(String),
    // Allow raw value for flexibility until strict schema is defined
    /// Apply a custom masking rule defined by a JSON-serializable structure.
    Custom(serde_json::Value),
}

/// A set of permissions parsed and optimized for efficient checking.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct PermissionSet {
    /// Exact string matches for permissions.
    exact: HashSet<String>,
    /// Prefix matches (e.g., "governance:*" results in "governance" prefix).
    prefixes: Vec<String>,
    /// Whether the user has top-level administrative access.
    is_admin: bool,
    /// Whether the user has a global wildcard permission.
    global_wildcard: bool,
    /// Original raw permission strings for serialization.
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

/// Row-level security and data masking rules for a table.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct TableRules {
    /// Optional SQL fragment used for row-level filtering.
    pub rls_filter: Option<String>,
    /// Optional mapping of column names to masking rules.
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

/// Representation of a user who has been successfully authenticated.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct AuthenticatedUser {
    /// Unique identifier for the user.
    pub id: ActorName,
    /// Set of permissions assigned to the user.
    pub permissions: PermissionSet,
    /// Map of table names to row-level security rules.
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
                if let Some(rest) = permission.get(prefix.len()..)
                    && rest.starts_with(':')
                {
                    return true;
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
            id: "test-user".into(),
            permissions: PermissionSet::from(vec!["admin".to_string()]),
            ..Default::default()
        };
        assert!(user.has_permission("any:action"));

        let system_admin = AuthenticatedUser {
            id: "sys-admin".into(),
            permissions: PermissionSet::from(vec!["system:admin".to_string()]),
            ..Default::default()
        };
        assert!(system_admin.has_permission("any:action"));
    }
}
