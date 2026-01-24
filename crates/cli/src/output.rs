//! Structured output handling for CLI commands.

use serde::Serialize;

#[derive(clap::ValueEnum, Clone, Debug, Default, PartialEq, Eq, Copy)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
    Yaml,
}

impl OutputFormat {
    /// Returns true if the output format is intended for machine consumption
    pub fn is_machine_readable(&self) -> bool {
        match self {
            OutputFormat::Human => false,
            OutputFormat::Json | OutputFormat::Yaml => true,
        }
    }
}

/// Helper struct for JSON output responses
#[derive(Serialize)]
pub struct CommandResponse<T> {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(flatten)]
    pub data: T,
}

impl<T> CommandResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            status: "success".to_string(),
            message: None,
            exit_code: Some(0),
            data,
        }
    }

    pub fn error(message: String, exit_code: i32, data: T) -> Self {
        Self {
            status: "error".to_string(),
            message: Some(message),
            exit_code: Some(exit_code),
            data,
        }
    }
}

/// Print the output to stdout in the requested format
pub fn print_output<T: Serialize>(format: OutputFormat, data: T) -> anyhow::Result<()> {
    match format {
        OutputFormat::Human => {
            // In human mode, we assume the command has already printed its
            // progress and human-readable output to stdout/stderr.
            // So we do nothing here.
        }
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&data)?;
            println!("{}", json);
        }
        OutputFormat::Yaml => {
            let yaml = serde_yaml::to_string(&data)?;
            println!("{}", yaml);
        }
    }
    Ok(())
}

/// Print a structured success response for machine outputs
pub fn print_success<T: Serialize>(format: OutputFormat, data: T) -> anyhow::Result<()> {
    if format == OutputFormat::Human {
        return Ok(());
    }

    let response = CommandResponse::success(data);
    print_output(format, response)
}

/// Print a structured error response for machine outputs
/// Note: In Human mode, errors are typically printed to stderr by main's error handler.
pub fn print_error<T: Serialize + Default>(
    format: OutputFormat,
    message: &str,
    exit_code: i32,
) -> anyhow::Result<()> {
    if format == OutputFormat::Human {
        // Human error printing is handled by main/eprintln
        return Ok(());
    }

    let response = CommandResponse::error(message.to_string(), exit_code, T::default());
    print_output(format, response)
}
