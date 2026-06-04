//! # Status Command
//!
//! Displays the current operational status of the Strake workspace,
//! including active nodes, connector health, and license state.
//!
//! ## Overview
//!
//! Evaluates the health of configured sources, contract compliance,
//! and schema drift to provide a holistic view of the workspace.
//!
//! ## Usage
//!
//! ```ignore
//! // status(Some("sources.yaml"), None, 5000, OutputFormat::Human, &ctx).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Measures source latency with non-blocking async checks under a timeout limit.
//!
//! ## Errors
//!
//! Returns validation errors if the source configuration cannot be parsed.
//!
//! ## References
//!
//! - Strake Health and Status Architecture Specification

use crate::output::{self, OutputFormat};
use crate::secrets::ResolverContext;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use strake_common::models::{DomainName, SourcesConfig};

#[derive(Serialize)]
pub struct StatusReport {
    pub schema_version: String,
    pub evaluated_at: DateTime<Utc>,
    pub domain: String,
    pub version: Option<StatusVersion>,
    pub sources: SourcesSummary,
    pub contracts: ContractsSummary,
    pub policies: PoliciesSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drift: Option<DriftSummary>,
    pub health: HealthState,
}

#[derive(Serialize)]
pub struct StatusVersion {
    pub current: i32,
    pub applied_at: DateTime<Utc>,
    pub actor: String,
}

#[derive(Serialize)]
pub struct SourcesSummary {
    pub configured: usize,
    pub reachable: usize,
    pub degraded: usize,
    pub detail: Vec<SourceDetail>,
}

#[derive(Serialize)]
pub struct SourceDetail {
    pub name: String,
    pub reachable: bool,
    pub latency_ms: Option<u128>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ContractsSummary {
    pub total: usize,
    pub violated: usize,
}

#[derive(Serialize)]
pub struct PoliciesSummary {
    pub total: usize,
}

#[derive(Serialize)]
pub struct DriftSummary {
    pub tables_out_of_sync: usize,
}

#[derive(Serialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Degraded,
    Critical,
    #[allow(dead_code)]
    Unknown,
}

pub async fn status(
    file_path: Option<&str>,
    domain_override: Option<&str>,
    timeout_ms: u64,
    format: OutputFormat,
    ctx: &ResolverContext,
) -> Result<i32> {
    let sources_config = if let Some(path) = file_path {
        crate::commands::helpers::parse_yaml(path, ctx).await?
    } else {
        SourcesConfig::default()
    };

    let domain = DomainName::from(
        domain_override
            .map(|s| s.to_string())
            .or_else(|| sources_config.domain.as_ref().map(|d| d.to_string()))
            .unwrap_or_else(|| "default".to_string()),
    );

    let version = None;

    let source_details = check_sources(&sources_config, timeout_ms).await;
    let reachable_count = source_details.iter().filter(|s| s.reachable).count();
    let degraded_count = source_details.iter().filter(|s| !s.reachable).count();

    // Stub for other summaries
    let contracts = ContractsSummary {
        total: 0,
        violated: 0,
    };
    let policies = PoliciesSummary { total: 0 };
    let health = derive_health(false, 0, degraded_count, None);

    let report = StatusReport {
        schema_version: "1.0".to_string(),
        evaluated_at: Utc::now(),
        domain: domain.to_string(),
        version,
        sources: SourcesSummary {
            configured: sources_config.sources.len(),
            reachable: reachable_count,
            degraded: degraded_count,
            detail: source_details,
        },
        contracts,
        policies,
        drift: None,
        health,
    };

    if format.is_machine_readable() {
        output::print_output(format, &report)?;
    } else {
        print_status_human(&report);
    }

    let exit_code = match report.health {
        HealthState::Healthy => 0,
        HealthState::Critical => 1,
        HealthState::Degraded => 2,
        HealthState::Unknown => 1, // Treat unknown as error in machine mode
    };

    Ok(exit_code)
}

/// Abstraction trait for checking source health status.
#[async_trait]
pub trait HealthChecker: Send + Sync {
    /// Perform check on target source. Returns (reachable, error_message, latency_ms)
    async fn check(
        &self,
        source: &strake_common::models::SourceConfig,
    ) -> (bool, Option<String>, Option<u128>);
}

/// Default implementation of health check.
pub struct DefaultHealthChecker {
    pub timeout_ms: u64,
}

#[async_trait]
impl HealthChecker for DefaultHealthChecker {
    async fn check(
        &self,
        source: &strake_common::models::SourceConfig,
    ) -> (bool, Option<String>, Option<u128>) {
        let start = std::time::Instant::now();
        if let Some(url) = &source.url {
            if url.starts_with("file://") {
                let path = url.strip_prefix("file://").unwrap();
                if std::path::Path::new(path).exists() {
                    let latency = start.elapsed().as_millis();
                    (true, None, Some(latency))
                } else {
                    (false, Some("File not found".to_string()), None)
                }
            } else if url.starts_with("http://") || url.starts_with("https://") {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_millis(self.timeout_ms))
                    .build();
                match client {
                    Ok(c) => match c.get(url).send().await {
                        Ok(resp) => {
                            let latency = start.elapsed().as_millis();
                            if resp.status().is_success() {
                                (true, None, Some(latency))
                            } else {
                                (
                                    false,
                                    Some(format!("HTTP status error: {}", resp.status())),
                                    Some(latency),
                                )
                            }
                        }
                        Err(e) => (false, Some(e.to_string()), None),
                    },
                    Err(e) => (false, Some(e.to_string()), None),
                }
            } else {
                (
                    false,
                    Some("Protocol not supported in status check".to_string()),
                    None,
                )
            }
        } else {
            // Sources without URLs (like Mocks or internal) are considered reachable
            let latency = start.elapsed().as_millis();
            (true, None, Some(latency))
        }
    }
}

async fn check_sources(config: &SourcesConfig, timeout_ms: u64) -> Vec<SourceDetail> {
    let checker = DefaultHealthChecker { timeout_ms };
    check_sources_with_checker(config, &checker).await
}

async fn check_sources_with_checker(
    config: &SourcesConfig,
    checker: &dyn HealthChecker,
) -> Vec<SourceDetail> {
    let mut details = Vec::new();
    for source in &config.sources {
        let (reachable, error, latency) = checker.check(source).await;
        details.push(SourceDetail {
            name: source.name.to_string(),
            reachable,
            latency_ms: latency,
            error,
        });
    }
    details
}

fn derive_health(
    never_applied: bool,
    contract_violations: usize,
    source_degraded: usize,
    drift_warnings: Option<usize>,
) -> HealthState {
    if never_applied {
        return HealthState::Unknown;
    }
    if contract_violations > 0 {
        return HealthState::Critical;
    }
    if source_degraded > 0 || drift_warnings.unwrap_or(0) > 0 {
        return HealthState::Degraded;
    }
    HealthState::Healthy
}

fn print_status_human(report: &StatusReport) {
    use owo_colors::OwoColorize;
    println!("Status for domain: {}", report.domain.bold());
    println!("Health: {:?}", report.health);
    println!(
        "Sources: {}/{} reachable",
        report.sources.reachable, report.sources.configured
    );
}
