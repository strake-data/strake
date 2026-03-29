use crate::commands::helpers::resolve_manifest_paths;
use crate::exit_codes;
use crate::output::{self, OutputFormat};
use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Serialize)]
pub struct RemoveResult {
    pub schema_version: String,
    pub source: String,
    pub table: String,
    pub dry_run: bool,
    pub removed: bool,
    pub checks: RemoveChecks,
    pub warnings: Vec<RemoveWarning>,
    pub errors: Vec<String>,
}

#[derive(Serialize)]
pub struct RemoveChecks {
    pub source_exists: bool,
    pub table_exists: bool,
    pub contract_orphaned: bool,
    pub policy_orphaned: bool,
    pub last_table: bool,
}

#[derive(Clone, Serialize)]
pub struct RemoveWarning {
    #[serde(rename = "type")]
    pub warning_type: String,
    pub detail: String,
}

pub async fn remove(
    source: &str,
    table: Option<&str>,
    file_path: Option<&str>,
    dry_run: bool,
    force: bool,
    source_only: bool,
    format: OutputFormat,
) -> Result<i32> {
    if source_only {
        eprintln!("'strake remove <source> --source-only' is not yet implemented.");
        return Ok(exit_codes::EXIT_ERROR);
    }

    let table =
        table.ok_or_else(|| anyhow!("Table name is required unless --source-only is used"))?;
    let manifest_paths = resolve_manifest_paths(file_path).await?;
    let raw = tokio::fs::read_to_string(&manifest_paths.sources)
        .await
        .with_context(|| format!("Failed to read {}", manifest_paths.sources.display()))?;
    let config: strake_common::models::SourcesConfig =
        serde_yaml::from_str(&raw).context("Failed to parse sources YAML")?;

    let Some(source_config) = config
        .sources
        .iter()
        .find(|entry| entry.name.as_ref() == source)
    else {
        emit_remove_result(
            format,
            RemoveResult {
                schema_version: "1".to_string(),
                source: source.to_string(),
                table: table.to_string(),
                dry_run,
                removed: false,
                checks: RemoveChecks {
                    source_exists: false,
                    table_exists: false,
                    contract_orphaned: false,
                    policy_orphaned: false,
                    last_table: false,
                },
                warnings: Vec::new(),
                errors: vec![format!("Source '{}' not found in sources.yaml.", source)],
            },
        )?;
        return Ok(exit_codes::EXIT_ERROR);
    };

    let table_exists = source_config.tables.iter().any(|entry| entry.name == table);
    if !table_exists {
        emit_remove_result(
            format,
            RemoveResult {
                schema_version: "1".to_string(),
                source: source.to_string(),
                table: table.to_string(),
                dry_run,
                removed: false,
                checks: RemoveChecks {
                    source_exists: true,
                    table_exists: false,
                    contract_orphaned: false,
                    policy_orphaned: false,
                    last_table: false,
                },
                warnings: Vec::new(),
                errors: vec![format!(
                    "Table '{}' not found under source '{}'.",
                    table, source
                )],
            },
        )?;
        return Ok(exit_codes::EXIT_ERROR);
    }

    let target = format!("{}.{}", source, table);
    let contract_warnings = contract_warnings(&manifest_paths.contracts, &target).await;
    let policy_warnings = policy_warnings(&manifest_paths.policies, &target).await;
    let last_table = source_config.tables.len() == 1;

    let mut warnings = Vec::new();
    warnings.extend(contract_warnings.iter().cloned());
    warnings.extend(policy_warnings.iter().cloned());
    if last_table {
        warnings.push(RemoveWarning {
            warning_type: "last_table".to_string(),
            detail: format!("'{}' is the last table under source '{}'.", table, source),
        });
    }

    if !force && (!contract_warnings.is_empty() || !policy_warnings.is_empty()) {
        emit_remove_result(
            format,
            RemoveResult {
                schema_version: "1".to_string(),
                source: source.to_string(),
                table: table.to_string(),
                dry_run,
                removed: false,
                checks: RemoveChecks {
                    source_exists: true,
                    table_exists: true,
                    contract_orphaned: !contract_warnings.is_empty(),
                    policy_orphaned: !policy_warnings.is_empty(),
                    last_table,
                },
                warnings,
                errors: vec![
                    "Removal blocked by orphaned contract or policy references.".to_string(),
                ],
            },
        )?;
        return Ok(exit_codes::EXIT_ERROR);
    }

    if dry_run {
        if !format.is_machine_readable() {
            println!("Dry run - no changes written.\n");
            println!("Would remove: {}.{}", source, table);
            println!("  Source file: {}", manifest_paths.sources.display());
        }
        emit_remove_result(
            format,
            RemoveResult {
                schema_version: "1".to_string(),
                source: source.to_string(),
                table: table.to_string(),
                dry_run: true,
                removed: false,
                checks: RemoveChecks {
                    source_exists: true,
                    table_exists: true,
                    contract_orphaned: !contract_warnings.is_empty(),
                    policy_orphaned: !policy_warnings.is_empty(),
                    last_table,
                },
                warnings,
                errors: Vec::new(),
            },
        )?;
        return Ok(exit_codes::EXIT_DRY_RUN);
    }

    let updated = remove_table_block(&raw, source, table)?;
    atomic_write(&manifest_paths.sources, &updated)?;

    let result = RemoveResult {
        schema_version: "1".to_string(),
        source: source.to_string(),
        table: table.to_string(),
        dry_run: false,
        removed: true,
        checks: RemoveChecks {
            source_exists: true,
            table_exists: true,
            contract_orphaned: !contract_warnings.is_empty(),
            policy_orphaned: !policy_warnings.is_empty(),
            last_table,
        },
        warnings,
        errors: Vec::new(),
    };
    emit_remove_result(format, result)?;

    if !contract_warnings.is_empty() || !policy_warnings.is_empty() || last_table {
        Ok(exit_codes::EXIT_WARNINGS)
    } else {
        Ok(exit_codes::EXIT_OK)
    }
}

fn emit_remove_result(format: OutputFormat, result: RemoveResult) -> Result<()> {
    if format.is_machine_readable() {
        output::print_output(format, result)?;
    } else if !result.errors.is_empty() {
        eprintln!("{}", result.errors.join("\n"));
    }
    Ok(())
}

#[derive(Clone, serde::Deserialize)]
struct ContractFile {
    #[serde(default)]
    contracts: Vec<ContractEntry>,
}

#[derive(Clone, serde::Deserialize)]
struct ContractEntry {
    table: String,
    #[serde(default)]
    strict: bool,
    #[serde(default)]
    columns: Vec<serde_yaml::Value>,
}

async fn contract_warnings(path: &Path, target: &str) -> Vec<RemoveWarning> {
    let Some(raw) = tokio::fs::read_to_string(path).await.ok() else {
        return Vec::new();
    };
    let Some(contract_file) = serde_yaml::from_str::<ContractFile>(&raw).ok() else {
        return Vec::new();
    };

    contract_file
        .contracts
        .into_iter()
        .filter(|contract| contract.table == target)
        .map(|contract| RemoveWarning {
            warning_type: "contract_orphaned".to_string(),
            detail: format!(
                "{} has an active contract (strict: {}, {} column constraints).",
                contract.table,
                contract.strict,
                contract.columns.len()
            ),
        })
        .collect()
}

#[derive(serde::Deserialize)]
struct PolicyFile {
    #[serde(default)]
    roles: Vec<RoleDef>,
}

#[derive(serde::Deserialize)]
struct RoleDef {
    name: String,
    #[serde(default)]
    policies: Vec<PolicyDef>,
}

#[derive(serde::Deserialize)]
struct PolicyDef {
    resource: String,
    #[serde(default)]
    rls_filter: Option<String>,
    #[serde(default)]
    masking: std::collections::HashMap<String, String>,
}

async fn policy_warnings(path: &Path, target: &str) -> Vec<RemoveWarning> {
    let Some(raw) = tokio::fs::read_to_string(path).await.ok() else {
        return Vec::new();
    };
    let Some(policy_file) = serde_yaml::from_str::<PolicyFile>(&raw).ok() else {
        return Vec::new();
    };

    let mut warnings = Vec::new();
    for role in policy_file.roles {
        for policy in role.policies {
            if policy.resource == target {
                let policy_type = if policy.rls_filter.is_some() {
                    "rls_filter"
                } else if !policy.masking.is_empty() {
                    "masking"
                } else {
                    "policy"
                };
                warnings.push(RemoveWarning {
                    warning_type: "policy_orphaned".to_string(),
                    detail: format!("{} references {} ({})", role.name, target, policy_type),
                });
            }
        }
    }
    warnings
}

fn remove_table_block(raw: &str, source: &str, table: &str) -> Result<String> {
    let lines: Vec<&str> = raw.lines().collect();
    let source_start =
        find_named_block(&lines, 0, source).ok_or_else(|| anyhow!("Source block not found"))?;
    let source_end = find_block_end(&lines, source_start);

    let tables_line = ((source_start + 1)..source_end)
        .find(|idx| lines[*idx].trim_start().starts_with("tables:"))
        .ok_or_else(|| anyhow!("Source '{}' does not contain a tables section", source))?;

    let table_start = ((tables_line + 1)..source_end)
        .find(|idx| {
            let trimmed = lines[*idx].trim_start();
            trimmed.starts_with("- name:") && extract_name(trimmed) == Some(table)
        })
        .ok_or_else(|| anyhow!("Table '{}' not found under '{}'", table, source))?;

    let table_end = find_block_end(&lines, table_start);
    let mut output = Vec::new();
    output.extend_from_slice(&lines[..table_start]);
    output.extend_from_slice(&lines[table_end..]);
    Ok(format!("{}\n", output.join("\n")))
}

fn find_named_block(lines: &[&str], start: usize, target_name: &str) -> Option<usize> {
    let mut sources_section = false;
    for (idx, line) in lines.iter().enumerate().skip(start) {
        let trimmed = line.trim_start();
        if trimmed == "sources:" {
            sources_section = true;
            continue;
        }
        if sources_section
            && trimmed.starts_with("- name:")
            && extract_name(trimmed) == Some(target_name)
        {
            return Some(idx);
        }
    }
    None
}

fn find_block_end(lines: &[&str], start: usize) -> usize {
    let indent = leading_spaces(lines[start]);
    let mut idx = start + 1;
    while idx < lines.len() {
        let line = lines[idx];
        if !line.trim().is_empty() && leading_spaces(line) <= indent {
            break;
        }
        idx += 1;
    }
    idx
}

fn extract_name(trimmed: &str) -> Option<&str> {
    trimmed.strip_prefix("- name:").map(str::trim)
}

fn leading_spaces(line: &str) -> usize {
    line.chars().take_while(|ch| *ch == ' ').count()
}

fn atomic_write(path: &Path, content: &str) -> Result<()> {
    serde_yaml::from_str::<strake_common::models::SourcesConfig>(content)
        .context("Updated YAML failed validation before write")?;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temp_path(
        parent,
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sources.yaml"),
    );
    fs::write(&temp_path, content)
        .with_context(|| format!("Failed to write temporary file {}", temp_path.display()))?;
    std::fs::rename(&temp_path, path)
        .with_context(|| format!("Failed to replace {}", path.display()))?;
    Ok(())
}

fn temp_path(parent: &Path, file_name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!(".{}.{}.tmp", file_name, nanos))
}

#[cfg(test)]
mod tests {
    use super::remove_table_block;

    #[test]
    fn removes_single_table_block_without_reformatting_rest() {
        let raw = r#"domain: finance
sources:
  - name: warehouse
    type: postgres
    tables:
      - name: orders
        schema: public
      - name: customers
        schema: public
"#;

        let updated = remove_table_block(raw, "warehouse", "orders").unwrap();
        assert!(!updated.contains("name: orders"));
        assert!(updated.contains("name: customers"));
        assert!(updated.contains("name: warehouse"));
    }
}
