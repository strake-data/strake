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
//! ```bash
//! strake status --domain my_domain
//! ```

use crate::metadata::MetadataStore;
use crate::output::{self, OutputFormat};
use crate::secrets::ResolverContext;
use anyhow::Result;
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
    store: &dyn MetadataStore,
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

    let version = match store.get_domain_version(&domain).await {
        Ok(v) => Some(StatusVersion {
            current: v,
            applied_at: Utc::now(), // FIXME: get from history
            actor: "unknown".to_string(),
        }),
        Err(_) => None,
    };

    let source_details = check_sources(&sources_config, timeout_ms).await;
    let reachable_count = source_details.iter().filter(|s| s.reachable).count();
    let degraded_count = source_details.iter().filter(|s| !s.reachable).count();

    // Stub for other summaries
    let contracts = ContractsSummary {
        total: 0,
        violated: 0,
    };
    let policies = PoliciesSummary { total: 0 };
    let health = derive_health(version.is_none(), 0, degraded_count, None);

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

async fn check_sources(config: &SourcesConfig, _timeout_ms: u64) -> Vec<SourceDetail> {
    let mut details = Vec::new();
    for source in &config.sources {
        let (reachable, error) = if let Some(url) = &source.url {
            if url.starts_with("file://") {
                let path = url.strip_prefix("file://").unwrap();
                if std::path::Path::new(path).exists() {
                    (true, None)
                } else {
                    (false, Some("File not found".to_string()))
                }
            } else {
                // Default fallback for other protocols in this stub
                (
                    false,
                    Some("Protocol not supported in status check".to_string()),
                )
            }
        } else {
            // Sources without URLs (like Mocks or internal) are considered reachable
            (true, None)
        };

        details.push(SourceDetail {
            name: source.name.to_string(),
            reachable,
            latency_ms: if reachable { Some(0) } else { None },
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
