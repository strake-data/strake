# PostgreSQL Connector

Strake connects natively to PostgreSQL databases using an asynchronous, pooled wire client (`tokio-postgres` and `bb8-postgres`). It supports full predicate/limit pushdown and custom schema mappings.

---

## 1. Connection Syntax

The PostgreSQL connection string is defined under the `connection` field of your SQL configuration. It supports standard PostgreSQL URI formats:

```yaml
connection: "postgres://<username>:<password>@<host>:<port>/<database_name>?sslmode=<prefer|require|disable>"
```

### Connection Example
```yaml
connection: "postgres://db_user:secure_password@localhost:5432/production_db?sslmode=prefer"
```

---

## 2. Configuration Parameters

The PostgreSQL database is configured as a `sql` source type with `dialect: postgres`:

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `dialect` | string | **Yes** | - | Must be `postgres`. |
| `connection` | string | **Yes** | - | PostgreSQL connection URI containing credentials. |
| `pool_size` | integer | No | `10` | Maximum size of the asynchronous connection pool. |
| `retry` | object | No | - | Retry settings for queries and pool connection. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a PostgreSQL source:

```yaml
sources:
  - name: internal_pg
    type: sql
    config:
      dialect: postgres
      connection: "postgres://db_user:secure_password@localhost:5432/production_db?sslmode=prefer"
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
