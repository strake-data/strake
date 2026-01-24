-- SQLite Initial Schema

CREATE TABLE IF NOT EXISTS domains (
    name TEXT PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO domains (name) VALUES ('default');

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    domain_name TEXT NOT NULL DEFAULT 'default' REFERENCES domains(name),
    type TEXT NOT NULL,
    url TEXT,
    username TEXT,
    password TEXT,
    properties TEXT, -- JSON
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (domain_name, name)
);

CREATE TABLE IF NOT EXISTS tables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    schema_name TEXT DEFAULT 'public',
    partition_column TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_id, schema_name, name)
);

CREATE TABLE IF NOT EXISTS columns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    length INTEGER,
    is_primary_key BOOLEAN DEFAULT 0,
    is_unique BOOLEAN DEFAULT 0,
    is_not_null BOOLEAN DEFAULT 0,
    position INTEGER,
    UNIQUE (table_id, name)
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY, -- UUID as HEX/Text
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL,
    user_id TEXT NOT NULL,
    permissions TEXT DEFAULT '{}', -- Array as JSON or comma-separated
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT,
    last_used_at TEXT,
    revoked_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS apply_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_name TEXT NOT NULL REFERENCES domains(name),
    version INTEGER NOT NULL,
    user_id TEXT NOT NULL,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
    sources_added TEXT, -- JSON
    sources_deleted TEXT, -- JSON
    tables_modified TEXT, -- JSON
    config_hash TEXT,
    config_yaml TEXT
);
