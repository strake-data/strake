use crate::commands::discovery::{add, search};
use crate::config::CliConfig;
use crate::output::OutputFormat;
use std::fs;
use std::path::PathBuf;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn create_temp_path() -> PathBuf {
    let mut path = std::env::temp_dir();
    let filename = format!(
        "strake_discovery_test_{}.yaml",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    path.push(filename);
    path
}

#[tokio::test]
async fn test_search_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        // Note: regex matching for path would be better but wiremock path matcher is simple
        // assuming default domain and test source
        .and(path("/introspect/default/test_source"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            { "schema": "public", "name": "users", "columns": [], "constraints": [] }
        ])))
        .mount(&mock_server)
        .await;

    let config = CliConfig {
        token: None,
        api_url: mock_server.uri(),
        database_url: None,
        metadata: None,
    };

    let result = search(
        "test_source",
        "sources.yaml",
        None,
        OutputFormat::Json,
        &config,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_add_new_source() {
    let mock_server = MockServer::start().await;
    let output_path = create_temp_path();

    // Mock response for add (POST)
    Mock::given(method("POST"))
        .and(path("/introspect/default/new_source/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
             "domain": "default",
             "sources": [
                 {
                     "name": "new_source",
                     "type": "postgres",
                     "url": "postgres://localhost/db",
                     "tables": [
                         { "schema": "public", "name": "users", "columns": [] }
                     ]
                 }
             ]
        })))
        .mount(&mock_server)
        .await;

    let config = CliConfig {
        token: None,
        api_url: mock_server.uri(),
        database_url: None,
        metadata: None,
    };

    let result = add(
        "new_source",
        "public.users",
        None,
        output_path.to_str().unwrap(),
        OutputFormat::Json,
        &config,
    )
    .await;

    assert!(result.is_ok());
    assert!(output_path.exists());

    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("new_source"));

    fs::remove_file(output_path).unwrap();
}
