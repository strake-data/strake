# SQLite Connector

Strake supports querying local SQLite databases natively. SQLite is ideal for high-performance local caching, application testing, and embedded setups since it requires zero network overhead.

---

## 1. Connection Syntax

SQLite connection string points directly to a local database file on your system using the `sqlite://` scheme:

```yaml
connection: "sqlite:///<absolute_path_to_db_file>"
```

### Connection Example
```yaml
connection: "sqlite:///workspaces/rust-postgres/data/app_cache.db"
```

---

## 2. Configuration Parameters

The SQLite database is registered as a `sql` source type with `dialect: sqlite`:

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `dialect` | string | **Yes** | - | Must be `sqlite`. |
| `connection` | string | **Yes** | - | Absolute local file URI pointing to your `.db` file. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a local SQLite source:

```yaml
sources:
  - name: cache_sqlite
    type: sql
    config:
      dialect: sqlite
      connection: "sqlite:///workspaces/rust-postgres/data/app_cache.db"
```
