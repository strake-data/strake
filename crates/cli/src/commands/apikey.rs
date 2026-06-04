//! # API Key Management Commands
//!
//! CLI commands for creating, listing, and revoking database-backed API keys.
//!
//! ## Overview
//!
//! Uses Argon2id to hash keys at rest and exposes a prefix-based revocation
//! model. The full key is emitted exactly once to stdout during creation.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let opts = apikey::ApiKeyCreateOptions {
//!     name: "ingest".into(),
//!     description: None,
//!     user_id: "svc_01".into(),
//!     permissions: vec!["read".into()],
//! };
//! let code = apikey::create(&store, opts, format).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Key generation is CPU-bound (Argon2). The SQLite backend offloads this
//! via `spawn_blocking`. The Postgres backend uses async I/O directly.
//!
//! ## Errors
//!
//! Returns `Err` if the metadata store is unreachable, if the key prefix
//! collides (DB unique constraint), or if Argon2 hashing fails.
//!
//! ## References
//!
//! - [PHC String Format](https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md)

use crate::exit_codes;
use crate::metadata::MetadataStore;
use crate::output::{self, OutputFormat};
use anyhow::{Context, Result};
use argon2::password_hash::rand_core::RngCore;
use argon2::{
    Argon2, PasswordHasher,
    password_hash::{SaltString, rand_core::OsRng},
};
use owo_colors::OwoColorize;

/// Command line options for creating a new API key.
pub struct ApiKeyCreateOptions {
    /// The name of the API key
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// The user associated with the key
    pub user_id: String,
    /// Comma-separated permissions or list of permissions
    pub permissions: Vec<String>,
}

/// Structured response schema returned on API key creation in machine-readable modes.
#[derive(serde::Serialize)]
pub struct ApiKeyCreateResult {
    /// The human-readable name of the key
    pub name: String,
    /// The user ID associated with the key
    pub user_id: String,
    /// The 8-character prefix of the key
    pub key_prefix: String,
    /// The raw generated API key
    pub key: String,
}

/// Generates a cryptographically secure API key, hashes it, and stores it.
pub async fn create(
    store: &dyn MetadataStore,
    options: ApiKeyCreateOptions,
    format: OutputFormat,
) -> Result<i32> {
    // 1. Generate 32 alphanumeric cryptographically secure random characters using OsRng and rejection sampling
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut random_str = String::with_capacity(32);
    const MAX_ATTEMPTS: usize = CHARSET.len() * 10;
    let mut attempts = 0;
    while random_str.len() < 32 && attempts < MAX_ATTEMPTS {
        attempts += 1;
        let mut byte = [0u8; 1];
        OsRng
            .try_fill_bytes(&mut byte)
            .context("Failed to generate secure random bytes for API key")?;
        let b = byte[0];
        // 256 % 62 = 6, so reject values >= 248 to avoid modulo bias
        if b < 248 {
            let idx = (b % 62) as usize;
            random_str.push(CHARSET[idx] as char);
        }
    }

    if random_str.len() < 32 {
        anyhow::bail!(
            "Failed to generate secure API key after {} attempts",
            MAX_ATTEMPTS
        );
    }

    let full_key = format!("strk_{}", random_str);
    let prefix = &full_key[..crate::metadata::KEY_PREFIX_LEN];

    // 2. Compute Argon2 hash in a threadpool block to avoid starving Tokio async reactor
    let full_key_clone = full_key.clone();
    let key_hash = tokio::task::spawn_blocking(move || {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        argon2
            .hash_password(full_key_clone.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|e| anyhow::anyhow!("Argon2 hashing failed: {}", e))
    })
    .await
    .context("API key hashing task panicked")??;

    // 3. Store in the metadata database
    store
        .create_api_key(
            &options.name,
            options.description.as_deref(),
            &options.user_id,
            prefix,
            &key_hash,
            &options.permissions,
        )
        .await?;

    // 4. Output the key respecting OutputFormat contract
    if format.is_machine_readable() {
        let response = ApiKeyCreateResult {
            name: options.name,
            user_id: options.user_id,
            key_prefix: prefix.to_string(),
            key: full_key,
        };
        output::print_success(format, &response)?;
    } else {
        println!("{}", "API key created successfully!".green().bold());
        println!("Name:        {}", options.name);
        println!("User ID:     {}", options.user_id);
        println!("Key Prefix:  {}", prefix);
        println!();
        println!("Please save this API key. It will");
        println!("Only be displayed once:");
        println!();
        println!("  {}", full_key.yellow().bold());
        println!();
    }

    Ok(exit_codes::EXIT_OK)
}

/// Helper function to truncate strings with ellipsis.
///
/// Note: Truncates at Unicode code-point boundaries. May split complex grapheme clusters.
fn truncate_with_ellipsis(s: &str, max_len: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_len {
        s.to_string()
    } else if max_len <= 3 {
        s.chars().take(max_len).collect()
    } else {
        // Collect characters cleanly, avoiding stringly parsing penalties
        let truncated: String = s.chars().take(max_len - 3).collect();
        format!("{}...", truncated)
    }
}

/// Lists all API keys from the store.
pub async fn list(store: &dyn MetadataStore, format: OutputFormat) -> Result<i32> {
    let keys = store.list_api_keys().await?;

    if format.is_machine_readable() {
        output::print_success(format, &keys)?;
        return Ok(exit_codes::EXIT_OK);
    }

    if keys.is_empty() {
        println!("No API keys found.");
        return Ok(exit_codes::EXIT_OK);
    }

    println!(
        "{:<10} | {:<20} | {:<12} | {:<12} | {:<12} | {:<15}",
        "Prefix", "Name", "User", "Status", "Created", "Description"
    );
    // Dynamically calculate table separator line width
    println!(
        "{}",
        "-".repeat(10 + 3 + 20 + 3 + 12 + 3 + 12 + 3 + 12 + 3 + 15)
    );

    for key in keys {
        let status = if key.revoked_at.is_some() {
            "Revoked".red().to_string()
        } else {
            "Active".green().to_string()
        };

        // Parse multi-delimiter timestamps cleanly
        let created_short = key
            .created_at
            .split(['T', ' '])
            .next()
            .unwrap_or(&key.created_at);

        let display_name = truncate_with_ellipsis(&key.name, 20);
        let display_user_id = truncate_with_ellipsis(&key.user_id, 12);
        let display_description = truncate_with_ellipsis(&key.description.unwrap_or_default(), 15);

        println!(
            "{:<10} | {:<20} | {:<12} | {:<12} | {:<12} | {:<15}",
            key.key_prefix.blue(),
            display_name,
            display_user_id,
            status,
            created_short,
            display_description
        );
    }

    Ok(exit_codes::EXIT_OK)
}

/// Structured response schema returned on API key revocation in machine-readable modes.
#[derive(serde::Serialize)]
pub struct ApiKeyRevokeResult {
    /// The 8-character prefix of the key
    pub key_prefix: String,
    /// Whether the key was successfully revoked
    pub revoked: bool,
}

/// Revokes an API key using its prefix.
pub async fn revoke(store: &dyn MetadataStore, prefix: &str, format: OutputFormat) -> Result<i32> {
    if prefix.len() != crate::metadata::KEY_PREFIX_LEN {
        anyhow::bail!(
            "Invalid prefix length. Prefix must be exactly {} characters.",
            crate::metadata::KEY_PREFIX_LEN
        );
    }

    let success = store.revoke_api_key(prefix).await?;

    if format.is_machine_readable() {
        let response = ApiKeyRevokeResult {
            key_prefix: prefix.to_string(),
            revoked: success,
        };
        output::print_success(format, &response)?;
    } else if success {
        println!("✓ API key with prefix {} has been revoked.", prefix.red());
    } else {
        println!(
            "No active API key found matching prefix {}.",
            prefix.yellow()
        );
    }

    Ok(exit_codes::EXIT_OK)
}
