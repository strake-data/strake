use crate::{db, models};
use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use regex::Regex;
use std::env;
use std::fs;
use std::path::Path;
use tokio_postgres::{Client, NoTls};

fn expand_secrets(content: &str) -> String {
    let re = Regex::new(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}").unwrap();
    re.replace_all(content, |caps: &regex::Captures| {
        let var_name = &caps[1];
        env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name))
    })
    .to_string()
}

pub fn get_api_url() -> String {
    // 1. Check environment variable
    if let Ok(url) = std::env::var("STRAKE_API_URL") {
        return url;
    }

    // 2. Check strake.yaml
    if let Ok(app_config) = strake_core::config::AppConfig::from_file("config/strake.yaml") {
        return app_config.server.api_url;
    }

    // 3. Fallback to default
    "http://localhost:8080/api/v1".to_string()
}

pub async fn init(template: Option<String>) -> Result<()> {
    let path = Path::new("sources.yaml");
    if path.exists() {
        println!("sources.yaml already exists.");
        if !confirm_overwrite() {
            return Ok(());
        }
    }

    let default_config = match template.as_deref() {
        Some("sql") => include_str!("../templates/sql.yaml"),
        Some("rest") => include_str!("../templates/rest.yaml"),
        Some("file") => include_str!("../templates/file.yaml"),
        Some("grpc") => include_str!("../templates/grpc.yaml"),
        _ => include_str!("../templates/default.yaml"),
    };

    fs::write(path, default_config).context("Failed to write sources.yaml")?;
    println!(
        "Initialized sources.yaml with {:?} template",
        template.unwrap_or_else(|| "default".to_string())
    );
    Ok(())
}

fn confirm_overwrite() -> bool {
    // Simple mock for now, or just assume no if existing.
    // In a real CLI we would read stdin.
    println!("File exists. skipping initialization.");
    false
}

pub async fn validate(file_path: &str, offline: bool) -> Result<()> {
    println!(
        "{} {} {}",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Validating...".bold().cyan()
    );
    let config = parse_yaml(file_path)?;

    println!("Structure is valid.");

    if offline {
        println!(
            "{}",
            "Skipping semantic validation (offline mode).".dimmed()
        );
        return Ok(());
    }

    println!("{}", "Starting Semantic Validation...".bold().cyan());

    let mut validation_errors = Vec::new();

    // Data Contract Validation
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.set_message("Checking Data Contracts (Server-Side)...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let sources_yaml = fs::read_to_string(file_path)?;
    if let Err(e) = validate_contracts(&sources_yaml, "contracts.yaml").await {
        pb.finish_with_message(format!("{} Contract Validation Failed", "✘".red()));
        validation_errors.push(format!("Contract Validation Failed: {}", e));
    } else {
        pb.finish_with_message(format!("{} Contracts: OK", "✔".green()));
    }

    for source in config.sources {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        pb.set_message(format!("Checking source '{}'...", source.name));
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        match validate_source(&source).await {
            Ok(_) => {
                pb.finish_with_message(format!("{} source '{}': OK", "✔".green(), source.name))
            }
            Err(e) => {
                pb.finish_with_message(format!("{} source '{}': FAILED", "✘".red(), source.name));
                validation_errors.push(format!("Source '{}': {}", source.name, e));
            }
        }
    }

    if validation_errors.is_empty() {
        println!("{}", "Semantic validation passed.".green().bold());
        Ok(())
    } else {
        println!("\n{}", "Validation Errors:".red().bold());
        for err in validation_errors {
            println!("{} {}", "•".red(), err);
        }
        Err(anyhow!("Validation failed with errors"))
    }
}

async fn validate_source(source: &models::SourceConfig) -> Result<()> {
    let url = source.url.as_deref().unwrap_or("");

    if source.source_type == "JDBC" && url.contains("postgresql") {
        validate_postgres_source(source, url).await
    } else if source.source_type == "JSON" || url.starts_with("file://") {
        validate_file_source(url).await
    } else {
        // Unknown or unsupported type for validation, just skip or warn
        // For now, we allow it but maybe warn?
        Ok(())
    }
}

use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

async fn validate_file_source(url: &str) -> Result<()> {
    let path_str = url.trim_start_matches("file://");
    let path = Path::new(path_str);

    if !path.exists() {
        return Err(anyhow!("File not found: {}", path_str));
    }

    let file = tokio::fs::File::open(path)
        .await
        .context(format!("Failed to open file: {}", path_str))?;
    let mut reader = BufReader::new(file);
    let mut buffer = String::new();

    // Read up to 3 lines to verify readability and header presence
    for _ in 0..3 {
        let bytes = reader
            .read_line(&mut buffer)
            .await
            .context("Failed to read line from file")?;
        if bytes == 0 {
            break; // EOF
        }
    }

    if buffer.trim().is_empty() {
        return Err(anyhow!("File is empty or not readable: {}", path_str));
    }

    Ok(())
}

async fn validate_postgres_source(source: &models::SourceConfig, url: &str) -> Result<()> {
    // Handle jdbc:postgresql:// -> postgres:// conversion
    let pg_url = url.replace("jdbc:postgresql://", "postgres://");

    let (client, connection) = tokio_postgres::connect(&pg_url, NoTls)
        .await
        .context("Failed to connect to Postgres source")?;

    tokio::spawn(async move {
        if let Err(_e) = connection.await {
            // Connection error handling
        }
    });

    for table in &source.tables {
        // Check if table exists
        let table_exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                &[&table.schema, &table.name],
            )
            .await
            .context("Failed to query information_schema.tables")?
            .get(0);

        if !table_exists {
            return Err(anyhow!(
                "Table '{}.{}' not found in upstream",
                table.schema,
                table.name
            ));
        }

        // Check columns
        for col in &table.columns {
            let col_exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
                    &[&table.schema, &table.name, &col.name],
                )
                .await
                .context("Failed to query information_schema.columns")?
                .get(0);

            if !col_exists {
                return Err(anyhow!(
                    "Column '{}' not found in table '{}.{}'",
                    col.name,
                    table.schema,
                    table.name
                ));
            }
        }
    }

    Ok(())
}

async fn validate_contracts(sources_yaml: &str, contracts_path: &str) -> Result<()> {
    let contracts_yaml = if std::path::Path::new(contracts_path).exists() {
        Some(fs::read_to_string(contracts_path)?)
    } else {
        None
    };

    let client = reqwest::Client::new();
    let api_url = get_api_url();

    let response = client
        .post(format!("{}/validate", api_url))
        .json(&strake_core::models::ValidationRequest {
            sources_yaml: sources_yaml.to_string(),
            contracts_yaml,
        })
        .send()
        .await
        .context("Failed to connect to Strake Validation API. Ensure the server is running.")?;

    let result: strake_core::models::ValidationResponse = response
        .json()
        .await
        .context("Failed to parse validation response from server")?;

    if result.valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "\n      - {}",
            result.errors.join("\n      - ")
        ))
    }
}

pub async fn apply(
    client: &Client,
    file_path: &str,
    force: bool,
    dry_run: bool,
    expected_version: Option<i32>,
) -> Result<()> {
    println!(
        "{} {} {}",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Applying configuration...".bold().cyan()
    );

    let config = parse_yaml(file_path)?;
    let domain = config.domain.as_deref().unwrap_or("default");

    db::init_db(client).await?; // Ensure schema exists before any logic or diffing

    if dry_run {
        println!("\n--- DRY RUN MODE ---");
        println!("Target Domain: {}", domain);
        validate(file_path, false).await?;
        println!();
        diff_internal(client, file_path).await?;
        println!("\nNo changes applied (dry-run mode).");
        return Ok(());
    }

    db::init_db(client).await?; // Ensure schema exists

    // 1. Optimistic Locking
    let current_version_to_update = match expected_version {
        Some(v) => v,
        None => db::get_domain_version(client, domain).await?,
    };

    // 2. Increment version (Locking)
    let new_version = db::increment_domain_version(client, domain, current_version_to_update)
        .await
        .context(
            "Failed to increment domain version. Another user may have modified the domain.",
        )?;

    // 3. Import
    let (added, deleted) = db::import_sources(client, &config, force).await?;

    // 4. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());
    let raw_yaml = fs::read_to_string(file_path)?;
    let config_hash = format!("{:x}", md5::compute(&raw_yaml)); // Simple hash

    db::log_apply_event(
        client,
        domain,
        new_version,
        &user_id,
        &added,
        &deleted,
        &config_hash,
        &raw_yaml,
    )
    .await?;

    println!(
        "{} configuration applied successfully to domain '{}' (New version: {}).",
        "✔".green(),
        domain.bold(),
        format!("v{}", new_version).yellow()
    );
    Ok(())
}

pub async fn rollback(client: &Client, domain: &str, to_version: i32) -> Result<()> {
    println!(
        "{} Rolling back domain '{}' to version {}...",
        "⟲".yellow(),
        domain.bold(),
        format!("v{}", to_version).yellow()
    );

    // 1. Fetch old config
    let config_yaml = db::get_history_config(client, domain, to_version).await?;
    let config: models::SourcesConfig = serde_yaml::from_str(&config_yaml)?;

    // 2. Optimistic Locking
    let current_version = db::get_domain_version(client, domain).await?;
    let new_version = db::increment_domain_version(client, domain, current_version).await?;

    // 3. Import
    let (added, deleted) = db::import_sources(client, &config, true).await?;

    // 4. Audit Log
    let user_id = std::env::var("USER").unwrap_or_else(|_| "cli-user".to_string());
    let config_hash = format!("{:x}", md5::compute(&config_yaml));

    db::log_apply_event(
        client,
        domain,
        new_version,
        &user_id,
        &added,
        &deleted,
        &config_hash,
        &config_yaml,
    )
    .await?;

    println!(
        "{} Domain '{}' rolled back successfully to version {} (New version: {}).",
        "✔".green(),
        domain.bold(),
        format!("v{}", to_version).yellow(),
        format!("v{}", new_version).yellow()
    );
    Ok(())
}

pub async fn diff(client: &Client, file_path: &str) -> Result<()> {
    diff_internal(client, file_path).await
}

async fn diff_internal(client: &Client, file_path: &str) -> Result<()> {
    let local_config = parse_yaml(file_path)?;
    let domain = local_config.domain.as_deref();
    let db_config = db::get_all_sources(client, domain).await?;

    println!(
        "{} {} {} domain '{}'...",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Computing difference for".bold().cyan(),
        domain.unwrap_or("default").bold()
    );

    let mut changes = 0;

    // Compare sources
    for local_source in &local_config.sources {
        match db_config
            .sources
            .iter()
            .find(|s| s.name == local_source.name)
        {
            None => {
                println!("{} ADD source: {}", "+".green(), local_source.name.bold());
                changes += 1;
            }
            Some(db_source) => {
                changes += diff_sources(local_source, db_source);
            }
        }
    }

    // Find deletions
    for db_source in &db_config.sources {
        if !local_config
            .sources
            .iter()
            .any(|s| s.name == db_source.name)
        {
            println!("{} DELETE source: {}", "-".red(), db_source.name.bold());
            changes += 1;
        }
    }

    if changes == 0 {
        println!(
            "{}",
            "No changes detected (schema matches metadata store).".dimmed()
        );
    } else {
        println!("\nDetected {} change(s).", changes.yellow());
    }

    Ok(())
}

fn diff_sources(local: &models::SourceConfig, db: &models::SourceConfig) -> usize {
    let mut changes = 0;

    if local.source_type != db.source_type {
        println!(
            "  ~ MODIFY source {} type: {} -> {}",
            local.name, db.source_type, local.source_type
        );
        changes += 1;
    }

    if local.url != db.url {
        println!(
            "  ~ MODIFY source {} url: {:?} -> {:?}",
            local.name, db.url, local.url
        );
        changes += 1;
    }

    // Tables
    for local_table in &local.tables {
        match db
            .tables
            .iter()
            .find(|t| t.name == local_table.name && t.schema == local_table.schema)
        {
            None => {
                println!(
                    "  {} ADD table: {}.{}",
                    "+".green(),
                    local_table.schema.dimmed(),
                    local_table.name.bold()
                );
                changes += 1;
            }
            Some(db_table) => {
                changes += diff_tables(local_table, db_table);
            }
        }
    }

    for db_table in &db.tables {
        if !local
            .tables
            .iter()
            .any(|t| t.name == db_table.name && t.schema == db_table.schema)
        {
            println!(
                "  {} DELETE table: {}.{}",
                "-".red(),
                db_table.schema.dimmed(),
                db_table.name.bold()
            );
            changes += 1;
        }
    }

    changes
}

fn diff_tables(local: &models::TableConfig, db: &models::TableConfig) -> usize {
    let mut changes = 0;

    if local.partition_column != db.partition_column {
        println!(
            "    ~ MODIFY table {}.{} partition_column: {:?} -> {:?}",
            local.schema, local.name, db.partition_column, local.partition_column
        );
        changes += 1;
    }

    // Columns
    for local_col in &local.columns {
        match db.columns.iter().find(|c| c.name == local_col.name) {
            None => {
                println!("    {} ADD column: {}", "+".green(), local_col.name.bold());
                changes += 1;
            }
            Some(db_col) => {
                if local_col.data_type != db_col.data_type {
                    println!(
                        "    ~ MODIFY column {} type: {} -> {}",
                        local_col.name, db_col.data_type, local_col.data_type
                    );
                    changes += 1;
                }
                // Add more column diffs if needed (PK, length, etc.)
            }
        }
    }

    for db_col in &db.columns {
        if !local.columns.iter().any(|c| c.name == db_col.name) {
            println!("    {} DELETE column: {}", "-".red(), db_col.name.bold());
            changes += 1;
        }
    }

    changes
}

pub fn parse_yaml(path: &str) -> Result<models::SourcesConfig> {
    let raw_content =
        fs::read_to_string(path).context(format!("Failed to read config file: {}", path))?;
    let content = expand_secrets(&raw_content);
    serde_yaml::from_str(&content).context("Failed to parse YAML structure")
}

pub async fn search(source: &str, file_path: &str, domain: Option<&str>) -> Result<()> {
    let domain = domain.unwrap_or("default");
    println!(
        "{} {} {} source '{}' [Domain: {}]...",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Searching for tables in".bold().cyan(),
        source.bold(),
        domain.bold()
    );

    let client = reqwest::Client::new();
    let api_url = get_api_url();

    let url = format!("{}/introspect/{}/{}", api_url, domain, source);
    let response = client
        .get(&url)
        .send()
        .await
        .context("Failed to connect to Strake API. Ensure the server is running.")?;

    if !response.status().is_success() {
        return Err(anyhow!("Search failed: {}", response.text().await?));
    }

    let tables: Vec<strake_core::models::TableDiscovery> = response.json().await?;

    println!("\n{}", "DISCOVERED TABLES:".bold().underline());
    println!("{:<20} {:<20}", "SCHEMA".bold(), "NAME".bold());
    println!("{}", "-".repeat(40).dimmed());
    for table in tables {
        println!("{:<20} {:<20}", table.schema, table.name);
    }

    println!(
        "\nUse 'strake-cli add {} <schema>.<name>' to import a specific table.",
        source
    );
    Ok(())
}

pub async fn add(
    source: &str,
    table_full_name: &str,
    domain: Option<&str>,
    output_path: &str,
) -> Result<()> {
    let domain = domain.unwrap_or("default");
    println!(
        "{} {} {} table '{}' from source '{}' into domain '{}'...",
        "[Config:".dimmed(),
        output_path.yellow(),
        "] Importing".bold().cyan(),
        table_full_name.bold(),
        source.bold(),
        domain.bold()
    );

    let client = reqwest::Client::new();
    let api_url = get_api_url();

    let url = format!("{}/introspect/{}/{}/tables", api_url, domain, source);
    let response = client
        .post(&url)
        .json(&vec![table_full_name.to_string()])
        .send()
        .await
        .context("Failed to connect to Strake API.")?;

    if !response.status().is_success() {
        return Err(anyhow!("Import failed: {}", response.text().await?));
    }

    let imported_config: strake_core::models::SourcesConfig = response.json().await?;

    // Append to local sources.yaml
    let mut current_config = if std::path::Path::new(output_path).exists() {
        parse_yaml(output_path)?
    } else {
        strake_core::models::SourcesConfig {
            domain: Some(domain.into()),
            sources: vec![],
        }
    };

    // simplified merge logic: find source, merge tables
    if let Some(imported_source) = imported_config.sources.first() {
        if let Some(target_source) = current_config.sources.iter_mut().find(|s| s.name == source) {
            // Update URL if missing or if imported one exists
            if target_source.url.is_none() && imported_source.url.is_some() {
                target_source.url = imported_source.url.clone();
            }

            for t in &imported_source.tables {
                if let Some(existing_table) = target_source
                    .tables
                    .iter_mut()
                    .find(|existing| existing.name == t.name && existing.schema == t.schema)
                {
                    // Update columns if existing table has none
                    if existing_table.columns.is_empty() {
                        existing_table.columns = t.columns.clone();
                    }
                } else {
                    target_source.tables.push(t.clone());
                }
            }
        } else {
            // Source doesn't exist in config, add it entirely
            current_config.sources.push(imported_source.clone());
        }
    }

    let yaml = serde_yaml::to_string(&current_config)?;
    fs::write(output_path, yaml)?;

    println!(
        "{} Successfully added '{}' to {}.",
        "✔".green(),
        table_full_name.bold(),
        output_path.yellow()
    );
    Ok(())
}

pub async fn introspect(
    source: &str,
    file_path: &str,
    _registered: bool,
    _client: Option<&Client>,
) -> Result<()> {
    search(source, file_path, None).await
}

pub async fn test_connection(file_path: &str) -> Result<()> {
    println!(
        "{} {} {}",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Testing connections...".bold().cyan()
    );
    let content = fs::read_to_string(file_path).context("Failed to read config file")?;
    let config: models::SourcesConfig =
        serde_yaml::from_str(&content).context("Failed to parse YAML structure")?;

    for source in config.sources {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        pb.set_message(format!("Testing source '{}'...", source.name));
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        match validate_source(&source).await {
            Ok(_) => {
                pb.finish_with_message(format!("{} source '{}': OK", "✔".green(), source.name))
            }
            Err(e) => pb.finish_with_message(format!(
                "{} source '{}': FAILED - {}",
                "✘".red(),
                source.name,
                e
            )),
        }
    }
    Ok(())
}

pub async fn describe(client: &Client, file_path: &str, domain: Option<&str>) -> Result<()> {
    println!(
        "{} {} {} domain '{}':",
        "[Config:".dimmed(),
        file_path.yellow(),
        "] Current Configuration in Metadata Store for"
            .bold()
            .cyan(),
        domain.unwrap_or("default").bold()
    );
    let config = db::get_all_sources(client, domain).await?;
    if config.sources.is_empty() {
        println!("No sources configured.");
        return Ok(());
    }

    for source in config.sources {
        println!(
            "{} {} (Type: {})",
            "Source:".bold().blue(),
            source.name.bold(),
            source.source_type.dimmed()
        );
        if let Some(url) = &source.url {
            println!("  {} {}", "URL:".dimmed(), url);
        }
        for table in source.tables {
            println!(
                "  {} {}.{}",
                "Table:".bold().cyan(),
                table.schema.dimmed(),
                table.name.bold()
            );
            if let Some(pc) = &table.partition_column {
                println!("    {} {}", "Partition Column:".dimmed(), pc.yellow());
            }
            println!("    {}", "Columns:".dimmed());
            for col in table.columns {
                let mut attribs = Vec::new();
                if col.primary_key {
                    attribs.push("PK".red().to_string());
                }
                if col.unique {
                    attribs.push("UNIQUE".yellow().to_string());
                }
                if col.is_not_null {
                    attribs.push("NOT NULL".dimmed().to_string());
                }

                let attrib_str = if attribs.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", attribs.join(", "))
                };

                println!(
                    "      {} {}: {}{}",
                    "•".cyan(),
                    col.name.bold(),
                    col.data_type,
                    attrib_str
                );
            }
        }
        println!();
    }
    Ok(())
}

pub async fn list_domains(client: &Client) -> Result<()> {
    let rows = client
        .query(
            "SELECT name, version, created_at FROM domains ORDER BY name",
            &[],
        )
        .await?;
    println!(
        "{:<20} {:<10} {:<20}",
        "DOMAIN".bold(),
        "VERSION".bold(),
        "CREATED AT".bold()
    );
    println!("{}", "-".repeat(50).dimmed());
    for row in rows {
        let name: String = row.get(0);
        let version: i32 = row.get(1);
        let created_at: chrono::DateTime<chrono::Utc> = row.get(2);
        println!(
            "{:<20} v{:<9} {}",
            name.bold().cyan(),
            version.yellow(),
            created_at.format("%Y-%m-%d %H:%M:%S").dimmed()
        );
    }
    Ok(())
}

pub async fn show_domain_history(client: &Client, domain: String) -> Result<()> {
    let rows = client.query(
        "SELECT version, user_id, timestamp, sources_added, sources_deleted FROM apply_history WHERE domain_name = $1 ORDER BY version DESC LIMIT 10",
        &[&domain]
    ).await?;

    println!(
        "{} '{}':",
        "Apply History for Domain".bold().cyan(),
        domain.bold()
    );
    println!(
        "{:<10} {:<15} {:<20} {:<20}",
        "VERSION".bold(),
        "USER".bold(),
        "TIMESTAMP".bold(),
        "CHANGES".bold()
    );
    println!("{}", "-".repeat(70).dimmed());

    for row in rows {
        let version: i32 = row.get(0);
        let user: String = row.get(1);
        let ts: chrono::DateTime<chrono::Utc> = row.get(2);
        let added: serde_json::Value = row.get(3);
        let deleted: serde_json::Value = row.get(4);

        let added_list = added.as_array().map(|a| a.len()).unwrap_or(0);
        let deleted_list = deleted.as_array().map(|d| d.len()).unwrap_or(0);

        println!(
            "{:<18} {:<15} {:<20} {} / {}",
            format!("v{}", version).yellow().bold(),
            user.dimmed(),
            ts.format("%Y-%m-%d %H:%M").dimmed(),
            format!("+{}", added_list).green(),
            format!("-{}", deleted_list).red()
        );
    }
    Ok(())
}
