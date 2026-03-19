use crate::commands::helpers::{expand_secrets, parse_yaml};
use crate::secrets::ResolverContext;
use serial_test::serial;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

fn get_test_ctx(vars: Vec<(&str, &str)>) -> ResolverContext {
    let mut system_env = HashMap::new();
    for (k, v) in vars {
        system_env.insert(k.to_string(), v.to_string());
    }
    ResolverContext {
        system_env,
        dotenv: HashMap::new(),
        offline: false,
    }
}

#[test]
#[serial]
fn test_expand_secrets_basic() {
    let ctx = get_test_ctx(vec![("TEST_VAR", "secret_value")]);
    let input = "This is a ${TEST_VAR}.";
    let output = expand_secrets(input, &ctx);
    assert_eq!(output, "This is a secret_value.");
}

#[test]
#[serial]
fn test_expand_secrets_multiple() {
    let ctx = get_test_ctx(vec![("VAR1", "foo"), ("VAR2", "bar")]);
    let input = "${VAR1} and ${VAR2}";
    let output = expand_secrets(input, &ctx);
    assert_eq!(output, "foo and bar");
}

#[test]
fn test_expand_secrets_missing() {
    let ctx = ResolverContext::default();
    let input = "This is ${MISSING_VAR}.";
    let output = expand_secrets(input, &ctx);
    // Should keep the original placeholder if missing
    assert_eq!(output, "This is ${MISSING_VAR}.");
}

fn create_temp_config(content: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let filename = format!(
        "strake_test_{}.yaml",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    path.push(filename);
    let mut file = fs::File::create(&path).unwrap();
    write!(file, "{}", content).unwrap();
    path
}

#[tokio::test]
async fn test_parse_yaml_valid() {
    let ctx = ResolverContext::default();
    let content = r#"
domain: test_domain
sources:
  - name: source1
    type: postgres
    url: postgres://localhost:5432/db
    tables: []
"#;
    let path = create_temp_config(content);
    let result = parse_yaml(path.to_str().unwrap(), &ctx).await;
    fs::remove_file(&path).unwrap(); // Cleanup

    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.domain.as_deref(), Some("test_domain"));
    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].name, "source1");
}

#[tokio::test]
#[serial]
async fn test_parse_yaml_with_secrets() {
    let ctx = get_test_ctx(vec![("DB_URL", "postgres://secret:5432/db")]);
    let content = r#"
domain: test_domain
sources:
  - name: source1
    type: postgres
    url: ${DB_URL}
    tables: []
"#;
    let path = create_temp_config(content);
    let result = parse_yaml(path.to_str().unwrap(), &ctx).await;
    fs::remove_file(&path).unwrap();

    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(
        config.sources[0].url.as_deref(),
        Some("postgres://secret:5432/db")
    );
}

#[tokio::test]
async fn test_parse_yaml_invalid_syntax() {
    let ctx = ResolverContext::default();
    let content = r#"
domain: [ unclosed brackets
sources:
"#;
    let path = create_temp_config(content);
    let result = parse_yaml(path.to_str().unwrap(), &ctx).await;
    fs::remove_file(&path).unwrap();

    assert!(result.is_err());
}

use crate::commands::helpers::get_client;
use crate::config::CliConfig;

#[test]
fn test_get_client_no_token() {
    let config = CliConfig {
        token: None,
        api_url: "http://localhost".to_string(),
        database_url: None,
        metadata: None,
    };
    let client = get_client(&config);
    assert!(client.is_ok());
}

#[test]
fn test_get_client_with_token() {
    let config = CliConfig {
        token: Some("secret_token".to_string()),
        api_url: "http://localhost".to_string(),
        database_url: None,
        metadata: None,
    };
    let client = get_client(&config);
    assert!(client.is_ok());
}
