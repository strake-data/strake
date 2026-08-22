# SQLite Connector

Strake supports querying local SQLite databases natively. SQLite is ideal for high-performance local caching, application testing, and embedded setups since it requires zero network overhead.

---

## 1. Connection Syntax

SQLite connection string points directly to a local database file on your system using the `url` (or `connection`) field with the `sqlite://` scheme:

```yaml
url: "sqlite:///<absolute_path_to_db_file>"
```

### Connection Example
```yaml
url: "sqlite:///workspaces/rust-postgres/data/app_cache.db"
```

---

## 2. Configuration Parameters

The SQLite database is configured as a `sqlite` source type (or `sql` with `dialect: sqlite`):

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `type` | string | **Yes** | - | Must be `sqlite` (or `sql` with `dialect: sqlite`). |
| `url` | string | **Yes** | - | Absolute local file URI pointing to your `.db` file. Also accepts `connection` as a fallback. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a local SQLite source:

```yaml
sources:
  - name: cache_sqlite
    type: sqlite
    url: "sqlite:///workspaces/rust-postgres/data/app_cache.db"
```
