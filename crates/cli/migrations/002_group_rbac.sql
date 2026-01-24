-- Migration: 002_group_rbac_and_contracts
-- Description: Adds support for Group-based RBAC (Entra/LDAP) and Dynamic Contracts

-- 1. Table for mapping External Group IDs (e.g. Entra Object IDs) to RLS Policies
CREATE TABLE IF NOT EXISTS group_table_permissions (
    id TEXT PRIMARY KEY,            -- UUID (Text for SQLite compat)
    group_id TEXT NOT NULL,         -- External Group ID (GUID or Name)
    table_name TEXT NOT NULL,       -- Table this applies to
    action TEXT DEFAULT 'SELECT',   -- Operation type
    rls_filter TEXT,                -- SQL Predicate (e.g., "region = 'US'")
    masked_columns TEXT,            -- Column masking rules (JSON stored as Text)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Optimize lookup by group_id since we query WHERE group_id = ANY($1)
    CONSTRAINT idx_group_lookup UNIQUE (group_id, table_name, action)
);

CREATE INDEX idx_group_permissions_group_id ON group_table_permissions(group_id);

-- 2. Table for Dynamic Data Contracts (See Step 1)
-- Stores the JSON configuration of contracts so they can be reloaded without restart.
CREATE TABLE IF NOT EXISTS contracts (
    id TEXT PRIMARY KEY,            -- UUID (Text for SQLite compat)
    table_name TEXT NOT NULL UNIQUE,
    content TEXT NOT NULL,          -- The full Contract object (JSON stored as Text)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT
);
