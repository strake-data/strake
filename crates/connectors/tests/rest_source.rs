//! REST source integration tests.
//!
//! Tests REST API configuration parsing for all auth types.

use strake_connectors::sources::rest::{AuthConfig, PaginationConfig, RestSourceConfig};

// ============================================================================
// Config Parsing Tests
// ============================================================================

#[test]
fn test_rest_config_parsing() {
    let yaml = r#"
        base_url: "http://example.com/api"
        method: "POST"
        headers:
          Authorization: "Bearer token"
        pagination:
          type: header
          header_name: "Link"
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    assert_eq!(config.base_url, "http://example.com/api");
    assert_eq!(config.method, "POST");
    assert_eq!(config.headers.get("Authorization").unwrap(), "Bearer token");
    match config.pagination {
        Some(PaginationConfig::Header { header_name }) => assert_eq!(header_name, "Link"),
        _ => panic!("Wrong pagination type"),
    }
}

#[test]
fn test_rest_bearer_auth_config() {
    let yaml = r#"
        base_url: "https://api.github.com"
        auth:
          type: bearer
          token: "ghp_xxxxxxxxxxxx"
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    match config.auth {
        Some(AuthConfig::Bearer { token }) => {
            assert_eq!(token, "ghp_xxxxxxxxxxxx");
        }
        _ => panic!("Wrong auth type"),
    }
}

#[test]
fn test_rest_basic_auth_config() {
    let yaml = r#"
        base_url: "https://api.example.com"
        auth:
          type: basic
          username: "user"
          password: "pass"
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    match config.auth {
        Some(AuthConfig::Basic { username, password }) => {
            assert_eq!(username, "user");
            assert_eq!(password, Some("pass".to_string()));
        }
        _ => panic!("Wrong auth type"),
    }
}

#[test]
fn test_rest_oauth_client_credentials_config() {
    let yaml = r#"
        base_url: "https://api.salesforce.com"
        auth:
          type: oauth_client_credentials
          client_id: "my_client"
          client_secret: "my_secret"
          token_url: "https://login.salesforce.com/oauth/token"
          scopes:
            - "api"
            - "refresh_token"
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    match config.auth {
        Some(AuthConfig::OAuthClientCredentials {
            client_id,
            client_secret,
            token_url,
            scopes,
        }) => {
            assert_eq!(client_id, "my_client");
            assert_eq!(client_secret, "my_secret");
            assert_eq!(token_url, "https://login.salesforce.com/oauth/token");
            assert_eq!(scopes.len(), 2);
            assert!(scopes.contains(&"api".to_string()));
        }
        _ => panic!("Wrong auth type"),
    }
}

#[test]
fn test_rest_jwt_assertion_config() {
    let yaml = r#"
        base_url: "https://bigquery.googleapis.com"
        auth:
          type: jwt_assertion
          issuer: "service@project.iam.gserviceaccount.com"
          audience: "https://bigquery.googleapis.com/"
          private_key_pem: "-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----"
          algorithm: RS256
          expiry_secs: 1800
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    match config.auth {
        Some(AuthConfig::JwtAssertion {
            issuer,
            audience,
            algorithm,
            expiry_secs,
            ..
        }) => {
            assert_eq!(issuer, "service@project.iam.gserviceaccount.com");
            assert_eq!(audience, "https://bigquery.googleapis.com/");
            assert_eq!(algorithm, "RS256");
            assert_eq!(expiry_secs, 1800);
        }
        _ => panic!("Wrong auth type"),
    }
}

#[test]
fn test_rest_pagination_indices_config() {
    let yaml = r#"
        base_url: "https://api.example.com/items"
        pagination:
          type: indices
          param_offset: "offset"
          param_limit: "limit"
          limit: 100
          initial_offset: 0
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    match config.pagination {
        Some(PaginationConfig::Indices {
            param_offset,
            param_limit,
            limit,
            initial_offset,
        }) => {
            assert_eq!(param_offset, "offset");
            assert_eq!(param_limit, "limit");
            assert_eq!(limit, 100);
            assert_eq!(initial_offset, 0);
        }
        _ => panic!("Wrong pagination type"),
    }
}

#[test]
fn test_rest_pushdown_config() {
    let yaml = r#"
        base_url: "https://api.example.com"
        pushdown:
          - column: "created_at"
            operator: ">="
            param: "since"
          - column: "status"
            operator: "="
            param: "status"
    "#;
    let config: RestSourceConfig = serde_yaml::from_str(yaml).expect("Failed to parse config");
    let pushdowns = config.pushdown.expect("Missing pushdown config");
    assert_eq!(pushdowns.len(), 2);
    assert_eq!(pushdowns[0].column, "created_at");
    assert_eq!(pushdowns[0].operator, ">=");
    assert_eq!(pushdowns[0].param, "since");
}
