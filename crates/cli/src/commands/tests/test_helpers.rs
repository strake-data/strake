use crate::commands::helpers::expand_secrets;
use serial_test::serial;
use std::env;

#[test]
#[serial]
fn test_expand_secrets_basic() {
    env::set_var("TEST_VAR", "secret_value");
    let input = "This is a ${TEST_VAR}.";
    let output = expand_secrets(input);
    assert_eq!(output, "This is a secret_value.");
}

#[test]
#[serial]
fn test_expand_secrets_multiple() {
    env::set_var("VAR1", "foo");
    env::set_var("VAR2", "bar");
    let input = "${VAR1} and ${VAR2}";
    let output = expand_secrets(input);
    assert_eq!(output, "foo and bar");
}

#[test]
fn test_expand_secrets_missing() {
    let input = "This is ${MISSING_VAR}.";
    let output = expand_secrets(input);
    // Should keep the original placeholder if missing
    assert_eq!(output, "This is ${MISSING_VAR}.");
}

use crate::commands::helpers::parse_yaml;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

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

#[test]
fn test_parse_yaml_valid() {
    let content = r#"
domain: test_domain
sources:
  - name: source1
    type: postgres
    url: postgres://localhost:5432/db
    tables: []
"#;
    let path = create_temp_config(content);
    let result = parse_yaml(path.to_str().unwrap());
    fs::remove_file(&path).unwrap(); // Cleanup

    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.domain.as_deref(), Some("test_domain"));
    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].name, "source1");
}

#[test]
#[serial]
fn test_parse_yaml_with_secrets() {
    env::set_var("DB_URL", "postgres://secret:5432/db");
    let content = r#"
domain: test_domain
sources:
  - name: source1
    type: postgres
    url: ${DB_URL}
    tables: []
"#;
    let path = create_temp_config(content);
    let result = parse_yaml(path.to_str().unwrap());
    fs::remove_file(&path).unwrap();

    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(
        config.sources[0].url.as_deref(),
        Some("postgres://secret:5432/db")
    );
}

#[test]
fn test_parse_yaml_invalid_syntax() {
    let content = r#"
domain: [ unclosed brackets
sources:
"#;
    let path = create_temp_config(content);
    let result = parse_yaml(path.to_str().unwrap());
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
