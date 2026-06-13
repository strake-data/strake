# DuckDB Connector

Strake supports connecting to local persistent or in-memory DuckDB engines, allowing you to run analytical queries over native DuckDB relational tables directly inside the Strake mesh.

---

## 1. Connection Syntax

DuckDB connection strings point to a local persistent DuckDB database file or specify an in-memory database using the `url` (or `connection`) field:

```yaml
# Persistent local file connection
url: "duckdb:///<absolute_path_to_duckdb_file>"

# In-memory transient database
url: "duckdb:///:memory:"
```

### Connection Example
```yaml
url: "duckdb:///workspaces/rust-postgres/data/analytics.duckdb"
```

---

## 2. Configuration Parameters

The DuckDB database is configured as a `duckdb` source type (or `sql` with `dialect: duckdb`):

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `type` | string | **Yes** | - | Must be `duckdb` (or `sql` with `dialect: duckdb`). |
| `url` | string | **Yes** | - | Path URI pointing to your persistent file or `:memory:`. Also accepts `connection` as a fallback. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a DuckDB database:

```yaml
sources:
  - name: local_duckdb
    type: duckdb
    url: "duckdb:///workspaces/rust-postgres/data/analytics.duckdb"
```
