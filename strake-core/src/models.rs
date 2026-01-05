use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourcesConfig {
    pub domain: Option<String>,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String, // e.g., JDBC
    pub url: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    pub partition_column: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnConfig>,
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub length: Option<i32>,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default, rename = "not_null")]
    pub is_not_null: bool,
}

// Data Contracts
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractsConfig {
    pub contracts: Vec<Contract>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub table: String, // e.g., "public.orders"
    #[serde(default)]
    pub strict: bool,
    pub columns: Vec<ContractColumn>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub nullable: Option<bool>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Constraint {
    #[serde(rename = "type")]
    pub constraint_type: String, // e.g., "gt", "lt", "regex"
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidationRequest {
    pub sources_yaml: String,
    pub contracts_yaml: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidationResponse {
    pub valid: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableDiscovery {
    pub name: String,
    pub schema: String,
}
