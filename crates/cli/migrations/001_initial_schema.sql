-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS domains (
    name TEXT PRIMARY KEY,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure a default domain exists
INSERT INTO domains (name) VALUES ('default') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    domain_name TEXT NOT NULL DEFAULT 'default' REFERENCES domains(name),
    type VARCHAR(50) NOT NULL, -- e.g., 'JDBC'
    url TEXT,                  -- Connection URL
    username VARCHAR(255),
    password VARCHAR(255),     -- In a real app, this should be encrypted or a reference to a secret store
    properties JSONB,          -- Extra properties
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (domain_name, name)
);

CREATE TABLE IF NOT EXISTS tables (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) DEFAULT 'public',
    partition_column VARCHAR(255), -- For info about partitions
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_id, schema_name, name)
);

CREATE TABLE IF NOT EXISTS columns (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    length INTEGER,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_unique BOOLEAN DEFAULT FALSE,
    is_not_null BOOLEAN DEFAULT FALSE,
    position INTEGER, -- Column order
    UNIQUE (table_id, name)
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL,
    user_id TEXT NOT NULL,
    permissions TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    revoked_at TIMESTAMP WITH TIME ZONE,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS apply_history (
    id SERIAL PRIMARY KEY,
    domain_name TEXT NOT NULL REFERENCES domains(name),
    version INT NOT NULL,
    user_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    sources_added JSONB,
    sources_deleted JSONB,
    tables_modified JSONB,
    config_hash TEXT,
    config_yaml TEXT
);

