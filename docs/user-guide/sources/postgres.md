# PostgreSQL Connector

Strake connects natively to PostgreSQL databases using an asynchronous, pooled wire client (`tokio-postgres` and `bb8-postgres`). It supports full predicate/limit pushdown and custom schema mappings.

---

## 1. Connection Syntax

The PostgreSQL connection string is defined under the `url` (or `connection`) field of your configuration. It supports standard PostgreSQL URI formats. You can set credentials either by embedding them in the URI or separating them into top-level parameters:

### Method 1: Embedded in the URI URL
```yaml
url: "postgres://db_user:secure_password@localhost:5432/production_db?sslmode=prefer"
```

### Method 2: Separated Top-Level Parameters (Safe for Environment Variables)
```yaml
url: "postgres://localhost:5432/production_db?sslmode=prefer"
username: "db_user"
password: "${env:POSTGRES_PASSWORD}"
```

---

## 2. Configuration Parameters

The PostgreSQL database is configured as a `postgres` source type (or `sql` with `dialect: postgres`):

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `type` | string | **Yes** | - | Must be `postgres` (or `sql` with `dialect: postgres`). |
| `url` | string | **Yes** | - | PostgreSQL connection URI. Also accepts `connection` as a fallback. |
| `username` | string | No | - | Optional username if not embedded in the connection URL. |
| `password` | string | No | - | Optional password. Supports environment variables via `${env:VAR}` format. |
| `pool_size` | integer | No | `10` | Maximum size of the asynchronous connection pool. |
| `retry` | object | No | - | Retry settings for queries and pool connection. |
| `schema_mapping.default_schemas` | list[string] | No | `["public", "default"]` | Schemas treated as default — tables in these schemas are pushed down without qualification. See [Schema Mapping](../configuration.md#schema_mapping-settings-sql-sources-only). |

---

## 3. Configuration Snippets

### Option A: Embedded Credentials
```yaml
sources:
  - name: internal_pg
    type: postgres
    url: "postgres://db_user:secure_password@localhost:5432/production_db?sslmode=prefer"
    pool_size: 15
    retry:
      max_attempts: 3
      initial_backoff_ms: 100
    tables:
      - name: users
        schema: public
      - name: telemetry
        schema: public
```

### Option B: Separated Credentials (Environment Variables)
```yaml
sources:
  - name: internal_pg_secure
    type: postgres
    url: "postgres://localhost:5432/production_db?sslmode=prefer"
    username: "db_user"
    password: "${env:POSTGRES_PASSWORD}"
    pool_size: 15
    tables:
      - name: users
        schema: public
```
