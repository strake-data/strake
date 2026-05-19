# ClickHouse Connector

Strake supports querying high-performance ClickHouse OLAP databases. It leverages dialect-specific translations to push down analytical aggregations and filters to speed up execution over billions of rows.

---

## 1. Connection Syntax

ClickHouse connection URI is defined under the `connection` field of your SQL configuration, supporting ClickHouse wire and HTTP connections:

```yaml
connection: "clickhouse://<username>:<password>@<host>:<port>/<database_name>"
```

### Connection Example
```yaml
connection: "clickhouse://default:secure_password@clickhouse-server:8123/default"
```

---

## 2. Configuration Parameters

ClickHouse database is registered as a `sql` source type with `dialect: clickhouse`:

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `dialect` | string | **Yes** | - | Must be `clickhouse`. |
| `connection` | string | **Yes** | - | ClickHouse connection URI containing host, credentials, and port. |
| `pool_size` | integer | No | `10` | Maximum size of the query connection pool. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a ClickHouse source:

```yaml
sources:
  - name: warehouse_clickhouse
    type: sql
    config:
      dialect: clickhouse
      connection: "clickhouse://default:secure_password@localhost:8123/default"
      pool_size: 15
      tables:
        - name: events
          schema: analytical_logs
```
