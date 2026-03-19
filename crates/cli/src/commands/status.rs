use crate::commands::helpers::resolve_manifest_paths;
use crate::commands::validate::validate_source;
use crate::exit_codes;
use crate::metadata::MetadataStore;
use crate::output::{self, OutputFormat};
use crate::secrets::ResolverContext;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use strake_common::models::{ContractsConfig, SourceConfig};

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
    pub active: usize,
    pub violations: usize,
    pub last_evaluated_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct PoliciesSummary {
    pub active: usize,
    pub types: Vec<String>,
}

#[derive(Serialize)]
pub struct DriftSummary {
    pub warnings: usize,
    pub last_checked_at: DateTime<Utc>,
    pub hint: String,
}

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Degraded,
    Critical,
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
    let manifest_paths = resolve_manifest_paths(file_path).await?;
    let configured_sources = crate::commands::helpers::parse_yaml(
        manifest_paths.sources.to_string_lossy().as_ref(),
        ctx,
    )
    .await
    .ok();

    let domain = domain_override
        .map(str::to_string)
        .or_else(|| {
            configured_sources
                .as_ref()
                .and_then(|cfg| cfg.domain.clone())
        })
        .or(manifest_paths.default_domain)
        .unwrap_or_else(|| "default".to_string());

    let history = match store.get_history(&domain, 1).await {
        Ok(history) => history,
        Err(_) => {
            let report = StatusReport {
                schema_version: "1".to_string(),
                evaluated_at: Utc::now(),
                domain,
                version: None,
                sources: SourcesSummary {
                    configured: 0,
                    reachable: 0,
                    degraded: 0,
                    detail: Vec::new(),
                },
                contracts: ContractsSummary {
                    active: 0,
                    violations: 0,
                    last_evaluated_at: None,
                },
                policies: PoliciesSummary {
                    active: 0,
                    types: Vec::new(),
                },
                drift: None,
                health: HealthState::Unknown,
            };

            if format.is_machine_readable() {
                output::print_output(format, report)?;
            } else {
                println!("Domain:        {}", report.domain);
                println!("Sources:       UNREACHABLE backend");
                println!("Health:        unknown");
            }
            return Ok(exit_codes::EXIT_ERROR);
        }
    };

    let applied_config = store.get_sources(&domain).await.unwrap_or_else(|_| {
        configured_sources
            .clone()
            .unwrap_or(strake_common::models::SourcesConfig {
                domain: Some(domain.clone()),
                sources: Vec::new(),
            })
    });

    let source_details = check_sources(applied_config.sources.clone(), timeout_ms).await;
    let reachable = source_details
        .iter()
        .filter(|detail| detail.reachable)
        .count();
    let degraded = source_details.len().saturating_sub(reachable);

    let contract_summary = read_contract_summary(&manifest_paths.contracts, history.first()).await;
    let policy_summary = read_policy_summary(&manifest_paths.policies).await;

    let version = history.first().and_then(|entry| {
        entry.timestamp.map(|timestamp| StatusVersion {
            current: entry.version,
            applied_at: timestamp,
            actor: entry.user_id.clone(),
        })
    });

    let health = derive_health(
        history.is_empty(),
        contract_summary.violations,
        degraded,
        None,
    );
    let report = StatusReport {
        schema_version: "1".to_string(),
        evaluated_at: Utc::now(),
        domain,
        version,
        sources: SourcesSummary {
            configured: source_details.len(),
            reachable,
            degraded,
            detail: source_details,
        },
        contracts: contract_summary,
        policies: policy_summary,
        drift: None,
        health,
    };

    if format.is_machine_readable() {
        output::print_output(format, &report)?;
    } else {
        print_status_human(&report);
    }

    let exit_code = match report.health {
        HealthState::Healthy => exit_codes::EXIT_OK,
        HealthState::Degraded => exit_codes::EXIT_WARNINGS,
        HealthState::Critical | HealthState::Unknown => exit_codes::EXIT_ERROR,
    };
    Ok(exit_code)
}

fn print_status_human(report: &StatusReport) {
    println!("Domain:        {}", report.domain);
    match &report.version {
        Some(version) => println!(
            "Version:       {}  (applied {} by {})",
            version.current,
            version.applied_at.to_rfc3339(),
            version.actor
        ),
        None => println!("Version:       never applied"),
    }
    println!(
        "Sources:       {} configured  |  {} reachable  |  {} degraded",
        report.sources.configured, report.sources.reachable, report.sources.degraded
    );
    println!(
        "Contracts:     {} active  |  {} violations",
        report.contracts.active, report.contracts.violations
    );
    let policy_types = if report.policies.types.is_empty() {
        "none".to_string()
    } else {
        report.policies.types.join(" + ")
    };
    println!(
        "Policies:      {} active  |  {}",
        report.policies.active, policy_types
    );
    match &report.drift {
        Some(drift) => println!(
            "Drift:         {} warning(s)  ->  {}",
            drift.warnings, drift.hint
        ),
        None => println!("Drift:         unknown"),
    }
}

async fn check_sources(sources: Vec<SourceConfig>, timeout_ms: u64) -> Vec<SourceDetail> {
    let futures = sources.into_iter().map(|source| async move {
        let name = source.name.clone();
        let started = Instant::now();
        match tokio::time::timeout(Duration::from_millis(timeout_ms), validate_source(&source))
            .await
        {
            Ok(Ok(())) => SourceDetail {
                name,
                reachable: true,
                latency_ms: Some(started.elapsed().as_millis()),
                error: None,
            },
            Ok(Err(err)) => SourceDetail {
                name,
                reachable: false,
                latency_ms: None,
                error: Some(err.to_string()),
            },
            Err(_) => SourceDetail {
                name,
                reachable: false,
                latency_ms: None,
                error: Some(format!("timeout after {}ms", timeout_ms)),
            },
        }
    });

    futures::future::join_all(futures).await
}

fn derive_health(
    never_applied: bool,
    contract_violations: usize,
    source_degraded: usize,
    drift_warnings: Option<usize>,
) -> HealthState {
    if never_applied {
        return HealthState::Critical;
    }
    if contract_violations > 0 {
        return HealthState::Critical;
    }
    if source_degraded > 0 || drift_warnings.unwrap_or(0) > 0 {
        return HealthState::Degraded;
    }
    HealthState::Healthy
}

async fn read_contract_summary(
    path: &std::path::Path,
    latest_history: Option<&crate::metadata::models::ApplyLogEntry>,
) -> ContractsSummary {
    let active = tokio::fs::read_to_string(path)
        .await
        .ok()
        .and_then(|raw| serde_yaml::from_str::<ContractsConfig>(&raw).ok())
        .map(|contracts| contracts.contracts.len())
        .unwrap_or(0);

    ContractsSummary {
        active,
        violations: 0,
        last_evaluated_at: latest_history.and_then(|entry| entry.timestamp),
    }
}

async fn read_policy_summary(path: &std::path::Path) -> PoliciesSummary {
    #[derive(serde::Deserialize)]
    struct PolicyFile {
        #[serde(default)]
        roles: Vec<RoleDef>,
    }

    #[derive(serde::Deserialize)]
    struct RoleDef {
        #[allow(dead_code)]
        #[serde(default)]
        roles: Vec<RoleDef>,
        #[serde(default)]
        policies: Vec<PolicyDef>,
    }

    #[derive(serde::Deserialize)]
    struct PolicyDef {
        #[serde(default)]
        rls_filter: Option<String>,
        #[serde(default)]
        masking: std::collections::HashMap<String, String>,
    }

    let Some(raw) = tokio::fs::read_to_string(path).await.ok() else {
        return PoliciesSummary {
            active: 0,
            types: Vec::new(),
        };
    };
    let Some(policy_file) = serde_yaml::from_str::<PolicyFile>(&raw).ok() else {
        return PoliciesSummary {
            active: 0,
            types: Vec::new(),
        };
    };

    let mut types = BTreeSet::new();
    let active = policy_file
        .roles
        .iter()
        .map(|role| {
            for policy in &role.policies {
                if policy.rls_filter.is_some() {
                    types.insert("rls".to_string());
                }
                if !policy.masking.is_empty() {
                    types.insert("rbac".to_string());
                }
            }
            role.policies.len()
        })
        .sum();

    PoliciesSummary {
        active,
        types: types.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::{HealthState, derive_health};

    #[test]
    fn health_is_critical_when_never_applied() {
        assert_eq!(derive_health(true, 0, 0, None), HealthState::Critical);
    }

    #[test]
    fn health_is_critical_when_contracts_fail() {
        assert_eq!(derive_health(false, 1, 0, None), HealthState::Critical);
    }

    #[test]
    fn health_is_degraded_when_sources_are_degraded() {
        assert_eq!(derive_health(false, 0, 1, None), HealthState::Degraded);
    }

    #[test]
    fn health_is_healthy_when_everything_is_clean() {
        assert_eq!(derive_health(false, 0, 0, Some(0)), HealthState::Healthy);
    }
}
