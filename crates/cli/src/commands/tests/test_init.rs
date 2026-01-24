use crate::commands::init;
use crate::output::OutputFormat;
use std::fs;
use std::path::PathBuf;

fn get_temp_path() -> PathBuf {
    let mut path = std::env::temp_dir();
    let filename = format!(
        "strake_init_test_{}.yaml",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    path.push(filename);
    path
}

#[tokio::test]
async fn test_init_creates_file() {
    let output_path = get_temp_path();
    // Ensure it doesn't exist
    if output_path.exists() {
        fs::remove_file(&output_path).unwrap();
    }

    let result = init::init(None, &output_path, OutputFormat::Human).await;

    assert!(result.is_ok());
    assert!(output_path.exists());

    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("type: JDBC")); // Basic check for default template content

    fs::remove_file(output_path).unwrap();
}

#[tokio::test]
async fn test_init_with_template() {
    let output_path = get_temp_path();

    let result = init::init(Some("sql".to_string()), &output_path, OutputFormat::Human).await;

    assert!(result.is_ok());
    let content = fs::read_to_string(&output_path).unwrap();
    // SQL template usually has jdbc or postgres type
    assert!(content.contains("dialect: postgres"));

    fs::remove_file(output_path).unwrap();
}
