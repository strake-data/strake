use crate::models::{self, SourcesConfig};
use anyhow::{anyhow, Context, Result};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

const DEFAULT_DOMAIN: &str = "default";

pub async fn connect(db_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(db_url, NoTls)
        .await
        .context("Failed to connect to database")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn init_db(client: &Client) -> Result<()> {
    let schema_v1 = include_str!("../migrations/001_initial_schema.sql");
    client
        .batch_execute(schema_v1)
        .await
        .context("Failed to execute initial schema")?;

    println!("Database initialized (Migration v4 successful).");
    Ok(())
}

pub async fn import_sources(
    client: &Client,
    config: &SourcesConfig,
    force: bool,
) -> Result<(Vec<String>, Vec<String>)> {
    let domain = config.domain.as_deref().unwrap_or(DEFAULT_DOMAIN);

    // Ensure domain exists
    client
        .execute(
            "INSERT INTO domains (name) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&domain],
        )
        .await?;

    // Track IDs and names for reporting/pruning
    let mut active_source_ids: Vec<i32> = Vec::new();
    let mut added_sources = Vec::new();

    for source in &config.sources {
        // Upsert Source with domain_name
        let source_rows = client.query(
            "INSERT INTO sources (name, type, url, username, password, domain_name) 
             VALUES ($1, $2, $3, $4, $5, $6) 
             ON CONFLICT (name) DO UPDATE SET type=$2, url=$3, username=$4, password=$5, domain_name=$6
             RETURNING id, (xmax = 0) as is_new", // xmax=0 is a common pg trick to detect if INSERT vs UPDATE
            &[&source.name, &source.source_type, &source.url, &source.username, &source.password, &domain],
        ).await.context("Failed to upsert source")?;

        let source_id: i32 = source_rows[0].get(0);
        let is_new: bool = source_rows[0].get(1);
        active_source_ids.push(source_id);

        if is_new {
            added_sources.push(source.name.clone());
        }
        println!(
            "Synced Source: {} (ID: {}) [Domain: {}]",
            source.name, source_id, domain
        );

        let mut active_table_ids: Vec<i32> = Vec::new();

        for table in &source.tables {
            // Upsert Table
            let table_rows = client
                .query(
                    "INSERT INTO tables (source_id, name, schema_name, partition_column)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (source_id, schema_name, name) 
                 DO UPDATE SET partition_column=$4
                 RETURNING id",
                    &[
                        &source_id,
                        &table.name,
                        &table.schema,
                        &table.partition_column,
                    ],
                )
                .await
                .context("Failed to upsert table")?;

            let table_id: i32 = table_rows[0].get(0);
            active_table_ids.push(table_id);

            let mut active_column_names: Vec<String> = Vec::new();

            for (idx, col) in table.columns.iter().enumerate() {
                // Upsert Column
                client.execute(
                    "INSERT INTO columns (table_id, name, data_type, length, is_primary_key, is_unique, is_not_null, position)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (table_id, name)
                     DO UPDATE SET data_type=$3, length=$4, is_primary_key=$5, is_unique=$6, is_not_null=$7, position=$8",
                    &[&table_id, &col.name, &col.data_type, &col.length, &col.primary_key, &col.unique, &col.is_not_null, &(idx as i32)],
                ).await.context("Failed to upsert column")?;
                active_column_names.push(col.name.clone());
            }

            // Prune columns not in YAML for this table
            // Only prune if we have at least one column defined, otherwise strict pruning might wipe out stuff if YAML is partial?
            // Assumption: YAML is source of truth. If specific columns are listed, others should be removed.
            // If No columns listed, maybe we shouldn't prune? For now, implementing strict pruning.
            if !active_column_names.is_empty() {
                let deleted = client
                    .execute(
                        "DELETE FROM columns WHERE table_id = $1 AND name != ALL($2)",
                        &[&table_id, &active_column_names],
                    )
                    .await
                    .context("Failed to prune columns")?;
                if deleted > 0 {
                    println!(
                        "  Pruned {} obsolete columns from table {}.{}",
                        deleted, table.schema, table.name
                    );
                }
            }
        }

        // Prune tables not in YAML for this source
        if !active_table_ids.is_empty() {
            let deleted = client
                .execute(
                    "DELETE FROM tables WHERE source_id = $1 AND id != ALL($2)",
                    &[&source_id, &active_table_ids],
                )
                .await
                .context("Failed to prune tables")?;
            if deleted > 0 {
                println!(
                    "  Pruned {} obsolete tables from source {}",
                    deleted, source.name
                );
            }
        }
    }

    // Prune sources not in YAML FOR THIS DOMAIN
    let mut deleted_sources = Vec::new();
    if !active_source_ids.is_empty() {
        // Fetch names of sources to be deleted for audit log
        let to_delete = client
            .query(
                "SELECT name FROM sources WHERE domain_name = $1 AND id != ALL($2)",
                &[&domain, &active_source_ids],
            )
            .await?;
        for row in to_delete {
            deleted_sources.push(row.get(0));
        }

        let deleted = client
            .execute(
                "DELETE FROM sources WHERE domain_name = $1 AND id != ALL($2)",
                &[&domain, &active_source_ids],
            )
            .await
            .context("Failed to prune sources")?;
        if deleted > 0 {
            println!(
                "Pruned {} obsolete sources from domain {}.",
                deleted, domain
            );
        }
    } else if config.sources.is_empty() {
        if !force {
            return Err(anyhow::anyhow!(
                    "\n\
                    \x1b[31;1m[CRITICAL WARNING] You are about to DELETE ALL SOURCES from domain '{}'!\x1b[0m\n\
                    \n\
                    The provided configuration file defines NO sources. GitOps synchronization would remove everything.\n\
                    \n\
                    If you REALLY intend to wipe the domain, you must run:\n\
                    \x1b[1mstrake-cli apply --force\x1b[0m\n\
                    ", domain
                ));
        }

        let to_delete = client
            .query(
                "SELECT name FROM sources WHERE domain_name = $1",
                &[&domain],
            )
            .await?;
        for row in to_delete {
            deleted_sources.push(row.get(0));
        }

        let deleted = client
            .execute("DELETE FROM sources WHERE domain_name = $1", &[&domain])
            .await
            .context("Failed to prune all sources in domain")?;
        if deleted > 0 {
            println!(
                "Pruned all {} sources from domain {} (config was empty).",
                deleted, domain
            );
        }
    }

    println!(
        "Synchronization for domain '{}' completed successfully.",
        domain
    );
    Ok((added_sources, deleted_sources))
}

pub async fn get_domain_version(client: &Client, domain: &str) -> Result<i32> {
    let row = client
        .query_opt("SELECT version FROM domains WHERE name = $1", &[&domain])
        .await
        .context("Failed to query domain version")?;

    match row {
        Some(r) => Ok(r.get(0)),
        None => {
            // Ensure domain exists if it was never seen before
            client
                .execute(
                    "INSERT INTO domains (name, version) VALUES ($1, 1) ON CONFLICT DO NOTHING",
                    &[&domain],
                )
                .await?;
            Ok(1)
        }
    }
}

pub async fn increment_domain_version(
    client: &Client,
    domain: &str,
    expected_version: i32,
) -> Result<i32> {
    let rows = client
        .execute(
            "UPDATE domains SET version = version + 1 WHERE name = $1 AND version = $2",
            &[&domain, &expected_version],
        )
        .await
        .context("Failed to increment domain version")?;

    if rows == 0 {
        return Err(anyhow!(
            "Optimistic locking failure: Domain '{}' version has changed (expected v{})",
            domain,
            expected_version
        ));
    }

    Ok(expected_version + 1)
}

pub struct ApplyLogEntry<'a> {
    pub domain: &'a str,
    pub version: i32,
    pub user_id: &'a str,
    pub added: &'a [String],
    pub deleted: &'a [String],
    pub config_hash: &'a str,
    pub config_yaml: &'a str,
}

pub async fn log_apply_event(client: &Client, entry: ApplyLogEntry<'_>) -> Result<()> {
    client.execute(
        "INSERT INTO apply_history (domain_name, version, user_id, sources_added, sources_deleted, config_hash, config_yaml)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        &[&entry.domain, &entry.version, &entry.user_id, &json!(entry.added), &json!(entry.deleted), &entry.config_hash, &entry.config_yaml],
    ).await.context("Failed to log apply history")?;
    Ok(())
}

pub async fn get_history_config(client: &Client, domain: &str, version: i32) -> Result<String> {
    let row = client
        .query_opt(
            "SELECT config_yaml FROM apply_history WHERE domain_name = $1 AND version = $2",
            &[&domain, &version],
        )
        .await
        .context("Failed to query apply history for config")?;

    match row {
        Some(r) => Ok(r.get(0)),
        None => Err(anyhow!(
            "Version {} not found for domain '{}'",
            version,
            domain
        )),
    }
}

pub async fn get_all_sources(
    client: &Client,
    domain: Option<&str>,
) -> Result<models::SourcesConfig> {
    let domain = domain.unwrap_or(DEFAULT_DOMAIN);
    let rows = client
        .query(
            "SELECT id, name, type, url FROM sources WHERE domain_name = $1",
            &[&domain],
        )
        .await
        .context("Failed to query sources")?;

    let mut sources = Vec::new();

    for row in rows {
        let source_id: i32 = row.get("id");
        let name: String = row.get("name");
        let source_type: String = row.get("type");
        let url: Option<String> = row.get("url");

        let table_rows = client
            .query(
                "SELECT id, name, schema_name, partition_column FROM tables WHERE source_id = $1",
                &[&source_id],
            )
            .await
            .context("Failed to query tables")?;

        let mut tables = Vec::new();
        for table_row in table_rows {
            let table_id: i32 = table_row.get("id");
            let table_name: String = table_row.get("name");
            let schema: String = table_row.get("schema_name");
            let partition_column: Option<String> = table_row.get("partition_column");

            let column_rows = client
                .query(
                    "SELECT name, data_type, length, is_primary_key, is_unique, is_not_null FROM columns WHERE table_id = $1 ORDER BY position",
                    &[&table_id],
                )
                .await
                .context("Failed to query columns")?;

            let mut columns = Vec::new();
            for col_row in column_rows {
                columns.push(models::ColumnConfig {
                    name: col_row.get("name"),
                    data_type: col_row.get("data_type"),
                    length: col_row.get("length"),
                    primary_key: col_row.get("is_primary_key"),
                    unique: col_row.get("is_unique"),
                    is_not_null: col_row.get("is_not_null"),
                });
            }

            tables.push(models::TableConfig {
                name: table_name,
                schema,
                partition_column,
                columns,
            });
        }

        sources.push(models::SourceConfig {
            name,
            source_type,
            url,
            username: None,
            password: None,
            tables,
        });
    }

    Ok(models::SourcesConfig {
        domain: Some(domain.to_string()),
        sources,
    })
}
