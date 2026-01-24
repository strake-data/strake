//! CLI command implementations, split into logical modules for maintainability.

mod apply;
mod describe;
mod diff;
mod discovery;
mod domain;
mod helpers;
mod init;
mod validate;

// Re-export public command functions
pub use apply::apply;
pub use describe::{describe, test_connection};
pub use diff::diff;
pub use discovery::{add, introspect, search};
pub use domain::{list_domains, rollback, show_domain_history};

#[cfg(test)]
mod tests;

pub use init::init;
pub use validate::validate;
