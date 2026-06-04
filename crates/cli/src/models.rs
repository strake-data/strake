//! # CLI Models
//!
//! Re-exports internal data models from `strake_common`.
//!
//! ## Overview
//!
//! This module acts as a bridge, exporting common domain and schema structures
//! (such as `SourcesConfig`, `DomainName`, etc.) for local CLI use.
//!
//! ## Usage
//!
//! ```ignore
//! // use crate::models::*;
//! ```
//!
//! ## Performance Characteristics
//!
//! Pure compile-time re-export with no execution overhead.
//!
//! ## Safety
//!
//! Standard safe Rust.
//!
//! ## Errors
//!
//! No error conditions are defined in this module.

pub use strake_common::models::*;
