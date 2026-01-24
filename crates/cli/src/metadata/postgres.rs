use super::{
    models::{ApplyLogEntry, ApplyResult},
    MetadataStore,
};
use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use strake_common::models::{ColumnConfig, SourceConfig, SourcesConfig, TableConfig};
use tokio_postgres::{Client, NoTls};

pub struct PostgresStore {
    client: Client,
}

impl PostgresStore {
    pub async fn new(db_url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(db_url, NoTls)
            .await
            .context("Failed to connect to database")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self { client })
    }
}

const DEFAULT_DOMAIN: &str = "default";

impl MetadataStore for PostgresStore {
    fn init(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let schema_v1 = include_str!("../../migrations/001_initial_schema.sql");
            self.client
                .batch_execute(schema_v1)
                .await
                .context("Failed to execute initial schema")?;
            Ok(())
        })
    }

    fn get_domain_version<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<i32>> {
        Box::pin(async move {
            let row = self
                .client
                .query_opt("SELECT version FROM domains WHERE name = $1", &[&domain])
                .await
                .context("Failed to query domain version")?;

            match row {
                Some(r) => Ok(r.get(0)),
                None => {
                    self.client
                        .execute(
                            "INSERT INTO domains (name, version) VALUES ($1, 1) ON CONFLICT DO NOTHING",
                            &[&domain],
                        )
                        .await?;
                    Ok(1)
                }
            }
        })
    }

    fn increment_domain_version<'a>(
        &'a self,
        domain: &'a str,
        expected_version: i32,
    ) -> BoxFuture<'a, Result<i32>> {
        Box::pin(async move {
            let rows = self
                .client
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
        })
    }

    fn apply_sources<'a>(
        &'a self,
        config: &'a SourcesConfig,
        _force: bool,
    ) -> BoxFuture<'a, Result<ApplyResult>> {
        Box::pin(async move {
            let domain = config.domain.as_deref().unwrap_or(DEFAULT_DOMAIN);

            // Ensure domain exists
            self.client
                .execute(
                    "INSERT INTO domains (name) VALUES ($1) ON CONFLICT DO NOTHING",
                    &[&domain],
                )
                .await?;

            let mut active_source_ids: Vec<i32> = Vec::new();
            let mut sources_added = Vec::new();

            for source in &config.sources {
                // Upsert Source
                let source_rows = self.client.query(
                    "INSERT INTO sources (name, type, url, username, password, domain_name) 
                     VALUES ($1, $2, $3, $4, $5, $6) 
                     ON CONFLICT (domain_name, name) DO UPDATE SET type=$2, url=$3, username=$4, password=$5
                     RETURNING id, (xmax = 0) as is_new",
                    &[&source.name, &source.source_type, &source.url, &source.username, &source.password, &domain],
                ).await.context("Failed to upsert source")?;

                let source_id: i32 = source_rows[0].get(0);
                let is_new: bool = source_rows[0].get(1);
                active_source_ids.push(source_id);

                if is_new {
                    sources_added.push(source.name.clone());
                }

                let mut active_table_ids: Vec<i32> = Vec::new();

                for table in &source.tables {
                    // Upsert Table
                    let table_rows = self
                        .client
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
                        self.client.execute(
                            "INSERT INTO columns (table_id, name, data_type, length, is_primary_key, is_unique, is_not_null, position)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                             ON CONFLICT (table_id, name)
                             DO UPDATE SET data_type=$3, length=$4, is_primary_key=$5, is_unique=$6, is_not_null=$7, position=$8",
                            &[&table_id, &col.name, &col.data_type, &col.length, &col.primary_key, &col.unique, &col.is_not_null, &(idx as i32)],
                        ).await.context("Failed to upsert column")?;
                        active_column_names.push(col.name.clone());
                    }

                    if !active_column_names.is_empty() {
                        self.client
                            .execute(
                                "DELETE FROM columns WHERE table_id = $1 AND name != ALL($2)",
                                &[&table_id, &active_column_names],
                            )
                            .await
                            .context("Failed to prune columns")?;
                    }
                }

                if !active_table_ids.is_empty() {
                    self.client
                        .execute(
                            "DELETE FROM tables WHERE source_id = $1 AND id != ALL($2)",
                            &[&source_id, &active_table_ids],
                        )
                        .await
                        .context("Failed to prune tables")?;
                }
            }

            // Prune sources
            let mut sources_deleted = Vec::new();
            if !active_source_ids.is_empty() {
                let to_delete = self
                    .client
                    .query(
                        "SELECT name FROM sources WHERE domain_name = $1 AND id != ALL($2)",
                        &[&domain, &active_source_ids],
                    )
                    .await?;
                for row in to_delete {
                    sources_deleted.push(row.get(0));
                }

                self.client
                    .execute(
                        "DELETE FROM sources WHERE domain_name = $1 AND id != ALL($2)",
                        &[&domain, &active_source_ids],
                    )
                    .await
                    .context("Failed to prune sources")?;
            } else if config.sources.is_empty() {
                let to_delete = self
                    .client
                    .query(
                        "SELECT name FROM sources WHERE domain_name = $1",
                        &[&domain],
                    )
                    .await?;
                for row in to_delete {
                    sources_deleted.push(row.get(0));
                }
                self.client
                    .execute("DELETE FROM sources WHERE domain_name = $1", &[&domain])
                    .await?;
            }

            Ok(ApplyResult {
                sources_added,
                sources_deleted,
            })
        })
    }

    fn log_apply_event<'a>(&'a self, entry: ApplyLogEntry) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.client.execute(
                "INSERT INTO apply_history (domain_name, version, user_id, sources_added, sources_deleted, tables_modified, config_hash, config_yaml)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[&entry.domain, &entry.version, &entry.user_id, &entry.sources_added, &entry.sources_deleted, &entry.tables_modified, &entry.config_hash, &entry.config_yaml],
            ).await.context("Failed to log apply history")?;
            Ok(())
        })
    }

    fn get_history<'a>(
        &'a self,
        domain: &'a str,
        limit: i64,
    ) -> BoxFuture<'a, Result<Vec<ApplyLogEntry>>> {
        Box::pin(async move {
            let rows = self.client.query(
                "SELECT domain_name, version, user_id, sources_added, sources_deleted, tables_modified, config_hash, config_yaml, timestamp 
                 FROM apply_history WHERE domain_name = $1 ORDER BY version DESC LIMIT $2",
                &[&domain, &limit],
            ).await?;

            let mut entries = Vec::new();
            for row in rows {
                entries.push(ApplyLogEntry {
                    domain: row.get("domain_name"),
                    version: row.get("version"),
                    user_id: row.get("user_id"),
                    sources_added: row.get("sources_added"),
                    sources_deleted: row.get("sources_deleted"),
                    tables_modified: row.get("tables_modified"),
                    config_hash: row.get("config_hash"),
                    config_yaml: row.get("config_yaml"),
                    timestamp: row.get("timestamp"),
                });
            }
            Ok(entries)
        })
    }

    fn get_sources<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<SourcesConfig>> {
        Box::pin(async move {
            let rows = self
                .client
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

                let table_rows = self.client
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

                    let column_rows = self.client
                        .query(
                            "SELECT name, data_type, length, is_primary_key, is_unique, is_not_null FROM columns WHERE table_id = $1 ORDER BY position",
                            &[&table_id],
                        )
                        .await
                        .context("Failed to query columns")?;

                    let mut columns = Vec::new();
                    for col_row in column_rows {
                        columns.push(ColumnConfig {
                            name: col_row.get("name"),
                            data_type: col_row.get("data_type"),
                            length: col_row.get("length"),
                            primary_key: col_row.get("is_primary_key"),
                            unique: col_row.get("is_unique"),
                            is_not_null: col_row.get("is_not_null"),
                        });
                    }

                    tables.push(TableConfig {
                        name: table_name,
                        schema,
                        partition_column,
                        columns,
                    });
                }

                sources.push(SourceConfig {
                    name,
                    source_type,
                    url,
                    username: None,
                    password: None,
                    tables,
                });
            }

            Ok(SourcesConfig {
                domain: Some(domain.to_string()),
                sources,
            })
        })
    }

    fn get_history_config<'a>(
        &'a self,
        domain: &'a str,
        version: i32,
    ) -> BoxFuture<'a, Result<String>> {
        Box::pin(async move {
            let row = self
                .client
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
        })
    }

    fn list_domains(&self) -> BoxFuture<'_, Result<Vec<super::models::DomainStatus>>> {
        Box::pin(async move {
            let rows = self
                .client
                .query(
                    "SELECT name, version, created_at FROM domains ORDER BY name",
                    &[],
                )
                .await?;

            let mut results = Vec::new();
            for row in rows {
                results.push(super::models::DomainStatus {
                    name: row.get("name"),
                    version: row.get("version"),
                    created_at: row.get("created_at"),
                });
            }
            Ok(results)
        })
    }
}
