use crate::exit_codes;
use crate::output::{self, OutputFormat};
use crate::secrets::{ResolutionError, ResolverContext, SecretResolver, Segment};
use anyhow::{Result, anyhow};
use owo_colors::OwoColorize;
use serde::Serialize;

#[derive(Serialize)]
pub struct SecretValidateResult {
    pub file: String,
    pub valid: bool,
    pub details: Vec<SecretResolutionDetail>,
}

#[derive(Serialize)]
pub struct SecretResolutionDetail {
    pub reference: String,
    pub status: String,
    pub error: Option<ResolutionError>,
}

pub async fn validate_secrets(
    file_path: &str,
    ctx: &ResolverContext,
    format: OutputFormat,
) -> Result<i32> {
    let content = tokio::fs::read_to_string(file_path)
        .await
        .map_err(|e| anyhow!("Failed to read file {}: {}", file_path, e))?;

    let segments = SecretResolver::parse(&content);
    let mut details = Vec::new();
    let mut all_valid = true;

    for segment in segments {
        if let Segment::Reference(r) = segment {
            match SecretResolver::resolve(&r.raw, ctx) {
                Ok(_) => {
                    details.push(SecretResolutionDetail {
                        reference: r.raw.clone(),
                        status: "Resolved".to_string(),
                        error: None,
                    });
                }
                Err(e) => {
                    all_valid = false;
                    details.push(SecretResolutionDetail {
                        reference: r.raw.clone(),
                        status: "Failed".to_string(),
                        error: Some(e),
                    });
                }
            }
        }
    }

    if format.is_machine_readable() {
        let result = SecretValidateResult {
            file: file_path.to_string(),
            valid: all_valid,
            details,
        };
        if !all_valid {
            output::print_error(
                format,
                "One or more secret references could not be resolved",
                1,
                Some(&result),
            )?;
            return Ok(exit_codes::EXIT_ERROR);
        }
        output::print_success(format, &result)?;
        return Ok(exit_codes::EXIT_OK);
    }

    // Human output
    println!(
        "{} {} {}",
        "[Secrets:".dimmed(),
        file_path.yellow(),
        "] Validating references...".bold().cyan()
    );

    if details.is_empty() {
        println!("No secret references found.");
        return Ok(exit_codes::EXIT_OK);
    }

    for detail in &details {
        match &detail.error {
            None => {
                println!("  {} {} — OK", "✔".green(), detail.reference.dimmed());
            }
            Some(e) => {
                println!(
                    "  {} {} — {}",
                    "✘".red(),
                    detail.reference.bold().red(),
                    e.to_string().red()
                );
            }
        }
    }

    if all_valid {
        println!("\n{}", "All secret references are valid.".green().bold());
        Ok(exit_codes::EXIT_OK)
    } else {
        println!("\n{}", "Secret validation failed.".red().bold());
        Err(anyhow!(
            "One or more secret references could not be resolved"
        ))
    }
}
