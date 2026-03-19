use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use std::collections::HashMap;
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Debug, Error, Serialize)]
pub enum ResolutionError {
    #[error("Malformed secret reference: {0}")]
    MalformedReference(String),
    #[error("Secret reference not found: {0}")]
    NotFound(String),
    #[error("Unsupported secret provider: {0}")]
    UnsupportedProvider(String),
    #[error("Secret resolution failed: {0}")]
    #[allow(dead_code)]
    InternalError(String),
    #[error("Offline mode: cannot resolve external secret reference: {0}")]
    OfflineUnsupported(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Segment<'a> {
    Literal(&'a str),
    Reference(SecretReference),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecretReference {
    pub provider: SecretProvider,
    pub key: String,
    pub raw: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SecretProvider {
    Env,
    DotEnv,
    Vault,
    Aws,
    Unsupported,
    Invalid,
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct SecretBuffer {
    inner: String,
}

impl SecretBuffer {
    pub fn new() -> Self {
        Self {
            inner: String::new(),
        }
    }

    pub fn push_literal(&mut self, s: &str) {
        self.inner.push_str(s);
    }

    pub fn push_resolved(&mut self, s: &str) {
        self.inner.push_str(s);
    }

    /// Consume the inner buffer and return a non-copy SecretString.
    pub fn into_secret(mut self) -> SecretString {
        let val = std::mem::take(&mut self.inner);
        SecretString::from(val)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResolverContext {
    pub system_env: HashMap<String, String>,
    pub dotenv: HashMap<String, String>,
    pub offline: bool,
}

pub struct SecretResolver;

impl SecretResolver {
    pub fn parse(input: &str) -> Vec<Segment<'_>> {
        let mut segments = Vec::new();
        let mut chars = input.char_indices().peekable();
        let mut current_pos = 0;

        while let Some((i, c)) = chars.next() {
            if c == '$'
                && let Some(&(ji, '{')) = chars.peek()
            {
                if i > current_pos {
                    segments.push(Segment::Literal(&input[current_pos..i]));
                }

                chars.next(); // Consume '{'
                let mut depth = 1;
                let mut end_idx = None;

                for (ki, kc) in chars.by_ref() {
                    if kc == '{' && input.as_bytes().get(ki.saturating_sub(1)) == Some(&b'$') {
                        depth += 1;
                    } else if kc == '}' {
                        depth -= 1;
                        if depth == 0 {
                            end_idx = Some(ki);
                            break;
                        }
                    }
                }

                if let Some(end) = end_idx {
                    let raw = &input[i..end + 1];
                    let inner = &input[ji + 1..end];
                    if let Some(reference) = Self::parse_reference(inner, raw) {
                        segments.push(Segment::Reference(reference));
                    } else {
                        // Fallback to Env provider for compatibility with untagged references
                        segments.push(Segment::Reference(SecretReference {
                            provider: SecretProvider::Env,
                            key: inner.to_string(),
                            raw: raw.to_string(),
                        }));
                    }
                    current_pos = end + 1;
                } else {
                    // Unclosed brace
                    current_pos = i;
                    break;
                }
            }
        }

        if current_pos < input.len() {
            segments.push(Segment::Literal(&input[current_pos..]));
        }

        segments
    }

    fn parse_reference(inner: &str, raw: &str) -> Option<SecretReference> {
        if let Some(key) = inner.strip_prefix("env:") {
            if key.is_empty() {
                return Some(SecretReference {
                    provider: SecretProvider::Invalid,
                    key: inner.to_string(),
                    raw: raw.to_string(),
                });
            }
            return Some(SecretReference {
                provider: SecretProvider::Env,
                key: key.to_string(),
                raw: raw.to_string(),
            });
        }
        if let Some(key) = inner.strip_prefix("dotenv:") {
            if key.is_empty() {
                return Some(SecretReference {
                    provider: SecretProvider::Invalid,
                    key: inner.to_string(),
                    raw: raw.to_string(),
                });
            }
            return Some(SecretReference {
                provider: SecretProvider::DotEnv,
                key: key.to_string(),
                raw: raw.to_string(),
            });
        }
        if inner.starts_with("vault://") {
            return Some(SecretReference {
                provider: SecretProvider::Vault,
                key: inner.to_string(),
                raw: raw.to_string(),
            });
        }
        if let Some(key) = inner.strip_prefix("aws:secretsmanager:") {
            if key.is_empty() {
                return Some(SecretReference {
                    provider: SecretProvider::Invalid,
                    key: inner.to_string(),
                    raw: raw.to_string(),
                });
            }
            return Some(SecretReference {
                provider: SecretProvider::Aws,
                key: key.to_string(),
                raw: raw.to_string(),
            });
        }
        if inner.contains(':') {
            return Some(SecretReference {
                provider: SecretProvider::Unsupported,
                key: inner.to_string(),
                raw: raw.to_string(),
            });
        }
        None
    }

    pub fn resolve(input: &str, ctx: &ResolverContext) -> Result<SecretString, ResolutionError> {
        let segments = Self::parse(input);
        let mut buffer = SecretBuffer::new();

        for segment in segments {
            match segment {
                Segment::Literal(s) => buffer.push_literal(s),
                Segment::Reference(r) => {
                    let resolved = Self::resolve_single(&r, ctx)?;
                    buffer.push_resolved(resolved.expose_secret());
                }
            }
        }

        Ok(buffer.into_secret())
    }

    fn resolve_single(
        reference: &SecretReference,
        ctx: &ResolverContext,
    ) -> Result<SecretString, ResolutionError> {
        match reference.provider {
            SecretProvider::Env => {
                if let Some(val) = ctx.system_env.get(&reference.key) {
                    Ok(SecretString::from(val.clone()))
                } else if let Some(val) = ctx.dotenv.get(&reference.key) {
                    Ok(SecretString::from(val.clone()))
                } else {
                    Err(ResolutionError::NotFound(reference.raw.clone()))
                }
            }
            SecretProvider::DotEnv => {
                if let Some(val) = ctx.dotenv.get(&reference.key) {
                    Ok(SecretString::from(val.clone()))
                } else {
                    Err(ResolutionError::NotFound(reference.raw.clone()))
                }
            }
            SecretProvider::Vault => {
                if ctx.offline {
                    Err(ResolutionError::OfflineUnsupported(reference.raw.clone()))
                } else {
                    // Vault support planned for Phase 1.1b
                    Err(ResolutionError::UnsupportedProvider(reference.raw.clone()))
                }
            }
            SecretProvider::Aws => {
                if ctx.offline {
                    Err(ResolutionError::OfflineUnsupported(reference.raw.clone()))
                } else {
                    Err(ResolutionError::UnsupportedProvider(reference.raw.clone()))
                }
            }
            SecretProvider::Unsupported => {
                Err(ResolutionError::UnsupportedProvider(reference.raw.clone()))
            }
            SecretProvider::Invalid => {
                Err(ResolutionError::MalformedReference(reference.raw.clone()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        let segments = SecretResolver::parse("plain text");
        assert_eq!(segments, vec![Segment::Literal("plain text")]);
    }

    #[test]
    fn test_parse_references() {
        let segments = SecretResolver::parse("url: ${env:DB_URL}, pass: ${dotenv:DB_PASS}");
        assert_eq!(
            segments,
            vec![
                Segment::Literal("url: "),
                Segment::Reference(SecretReference {
                    provider: SecretProvider::Env,
                    key: "DB_URL".to_string(),
                    raw: "${env:DB_URL}".to_string()
                }),
                Segment::Literal(", pass: "),
                Segment::Reference(SecretReference {
                    provider: SecretProvider::DotEnv,
                    key: "DB_PASS".to_string(),
                    raw: "${dotenv:DB_PASS}".to_string()
                }),
            ]
        );
    }

    #[test]
    fn test_resolve_combined() {
        let mut system_env = HashMap::new();
        system_env.insert("HOST".to_string(), "localhost".to_string());
        let mut dotenv = HashMap::new();
        dotenv.insert("PASS".to_string(), "secret".to_string());

        let ctx = ResolverContext {
            system_env,
            dotenv,
            offline: false,
        };

        let resolved =
            SecretResolver::resolve("postgres://user:${dotenv:PASS}@${env:HOST}/db", &ctx).unwrap();
        assert_eq!(
            resolved.expose_secret(),
            "postgres://user:secret@localhost/db"
        );
    }

    #[test]
    fn test_precedence() {
        let mut system_env = HashMap::new();
        system_env.insert("VAR".to_string(), "env_val".to_string());
        let mut dotenv = HashMap::new();
        dotenv.insert("VAR".to_string(), "dotenv_val".to_string());

        let ctx = ResolverContext {
            system_env,
            dotenv,
            offline: false,
        };

        let resolved = SecretResolver::resolve("${env:VAR}", &ctx).unwrap();
        assert_eq!(resolved.expose_secret(), "env_val");
    }

    #[test]
    fn test_unsupported_error() {
        let ctx = ResolverContext {
            system_env: HashMap::new(),
            dotenv: HashMap::new(),
            offline: false,
        };

        let result = SecretResolver::resolve("${vault://secret/path}", &ctx);
        assert!(matches!(
            result,
            Err(ResolutionError::UnsupportedProvider(_))
        ));
    }

    #[test]
    fn test_offline_error() {
        let ctx = ResolverContext {
            system_env: HashMap::new(),
            dotenv: HashMap::new(),
            offline: true,
        };

        let result = SecretResolver::resolve("${vault://secret/path}", &ctx);
        assert!(matches!(
            result,
            Err(ResolutionError::OfflineUnsupported(_))
        ));
    }

    use proptest::prelude::*;
    proptest! {
        #[test]
        fn test_parse_does_not_panic(s in "\\PC*") {
            let _ = SecretResolver::parse(&s);
        }

        #[test]
        fn test_parse_identifies_simple_refs(key in "[a-zA-Z0-9_]+") {
            let input = format!("${{env:{}}}", key);
            let segments = SecretResolver::parse(&input);
            assert_eq!(segments.len(), 1);
            if let Segment::Reference(r) = &segments[0] {
                assert_eq!(r.provider, SecretProvider::Env);
                assert_eq!(r.key, key);
            } else {
                panic!("Expected reference segment");
            }
        }
    }
}
