use crate::commands::init;
use crate::output::OutputFormat;
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_init_creates_all_files() {
    let temp_dir = tempdir().unwrap();
    let sources_path = temp_dir.path().join("sources.yaml");
    let config_path = temp_dir.path().join("strake.yaml");
    let readme_path = temp_dir.path().join("README.md");

    // Change current directory to temp_dir because init creates strake.yaml/README.md in CWD
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let result = init::init(None, &sources_path, false, OutputFormat::Human).await;

    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    assert!(sources_path.exists());
    assert!(config_path.exists());
    assert!(readme_path.exists());

    let sources_content = fs::read_to_string(&sources_path).unwrap();
    assert!(sources_content.contains("type: JDBC"));

    let config_content = fs::read_to_string(&config_path).unwrap();
    assert!(config_content.contains("backend: sqlite"));
    assert!(config_content.contains("path: strake_metadata.db"));

    let readme_content = fs::read_to_string(&readme_path).unwrap();
    assert!(readme_content.contains("# Strake Workspace"));
    assert!(readme_content.contains("Running in Embedded Mode"));
}

#[tokio::test]
async fn test_init_with_template() {
    let temp_dir = tempdir().unwrap();
    let sources_path = temp_dir.path().join("sources_sql.yaml");

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let result = init::init(
        Some("sql".to_string()),
        &sources_path,
        true,
        OutputFormat::Human,
    )
    .await;

    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    let content = fs::read_to_string(&sources_path).unwrap();
    assert!(content.contains("dialect: postgres"));

    // With sources_only=true, strake.yaml and README.md should NOT exist
    assert!(!temp_dir.path().join("strake.yaml").exists());
    assert!(!temp_dir.path().join("README.md").exists());
}

#[tokio::test]
async fn test_init_sources_only() {
    let temp_dir = tempdir().unwrap();
    let sources_path = temp_dir.path().join("sources.yaml");

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let result = init::init(None, &sources_path, true, OutputFormat::Human).await;

    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    assert!(sources_path.exists());
    assert!(!temp_dir.path().join("strake.yaml").exists());
    assert!(!temp_dir.path().join("README.md").exists());
}
