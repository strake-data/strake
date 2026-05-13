//! DataFusion session management and extension injection.
//!
//! This module provides the `SessionManager` which is responsible for
//! reconstructing `SessionContext` with appropriate Strake extensions.
//!
//! # Usage
//!
//! ```rust
//! // let manager = SessionManager::new(base_context);
//! // let context = manager.context_for_user(Some(user), collector);
//! ```
//!
//! # Performance Characteristics
//!
//! Session reconstruction involves shallow cloning of the `SessionState`. This is
//! a relatively lightweight operation but should be performed once per query
//! rather than once per record batch.
//!
//! # Safety
//!
//! This module uses no unsafe code.
//!
//! # Errors
//!
//! This module does not directly return errors, as session reconstruction is
//! primarily a configuration step. However, downstream planning may fail if
//! the session state is incorrectly configured.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use strake_common::auth::AuthenticatedUser;
use strake_common::warnings::WarningCollector;

/// Manages the reconstruction of DataFusion `SessionContext` with Strake extensions.
///
/// The `SessionManager` ensures that authentication context and warning collectors
/// are correctly injected into the session state without losing core components
/// like the custom `QueryPlanner`.
pub struct SessionManager {
    /// The base session context to use for reconstruction.
    pub base_context: SessionContext,
}

impl SessionManager {
    /// Create a new session manager with the given base context.
    pub fn new(base_context: SessionContext) -> Self {
        Self { base_context }
    }

    /// Create a specialized context for a specific user and query execution.
    ///
    /// This method clones the base state and injects:
    /// 1. `AuthExtension` if a user is provided.
    /// 2. `WarningExtension` with the provided collector.
    pub fn context_for_user(
        &self,
        user: Option<AuthenticatedUser>,
        collector: WarningCollector,
    ) -> SessionContext {
        let state = self.base_context.state();
        let mut config = state.config().clone();

        if let Some(user_opt) = user {
            config
                .options_mut()
                .extensions
                .insert(crate::extensions::auth_config::AuthExtension { user: user_opt });
        }

        config.options_mut().extensions.insert(
            strake_connectors::extensions::warnings::WarningExtension {
                collector: collector.clone(),
            },
        );

        let state = SessionStateBuilder::new_from_existing(state)
            .with_config(config)
            .with_query_planner(Arc::new(crate::query::planner::QueryPlanner::new()))
            .build();

        SessionContext::new_with_state(state)
    }
}
