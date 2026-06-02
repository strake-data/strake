-- SQLite Initial Schema

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY, -- UUID as HEX/Text
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    permissions TEXT DEFAULT '{}', -- Array as JSON or comma-separated
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT,
    last_used_at TEXT,
    revoked_at TEXT,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;

