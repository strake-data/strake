use crate::commands::validate::validate_contracts;
use crate::config::CliConfig;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_validate_contracts_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/validate-contracts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "valid": true,
            "errors": []
        })))
        .mount(&mock_server)
        .await;

    let config = CliConfig {
        token: None,
        api_url: mock_server.uri(),
        database_url: None,
    };

    let result = validate_contracts("sources: []", "contracts.yaml", &config).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_contracts_failure() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/validate-contracts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "valid": false,
            "errors": ["Contract violation: field mismatch"]
        })))
        .mount(&mock_server)
        .await;

    let config = CliConfig {
        token: None,
        api_url: mock_server.uri(),
        database_url: None,
    };

    let result = validate_contracts("sources: []", "contracts.yaml", &config).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Contract violation"));
}
