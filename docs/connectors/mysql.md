# MySQL Connector

Strake supports querying MySQL and MariaDB transactional databases. It manages connection pooling asynchronously, optimizes SQL dialect translations, and pushes filters directly to the database.

---

## 1. Connection Syntax

The MySQL connection string is defined under the `url` (or `connection`) field of your configuration. It supports standard MySQL URI formats. You can set credentials either by embedding them in the URI or separating them into top-level parameters:

### Method 1: Embedded in the URI URL
```yaml
url: "mysql://db_user:secure_password@mysql-host:3306/sales_db"
```

### Method 2: Separated Top-Level Parameters (Safe for Environment Variables)
```yaml
url: "mysql://mysql-host:3306/sales_db"
username: "db_user"
password: "${env:MYSQL_PASSWORD}"
```

---

## 2. Configuration Parameters

The MySQL database is configured as a `mysql` source type (or `sql` with `dialect: mysql`):

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `type` | string | **Yes** | - | Must be `mysql` (or `sql` with `dialect: mysql`). |
| `url` | string | **Yes** | - | MySQL connection URI. Also accepts `connection` as a fallback. |
| `username` | string | No | - | Optional username if not embedded in the connection URL. |
| `password` | string | No | - | Optional password. Supports environment variables via `${env:VAR}` format. |
| `pool_size` | integer | No | `10` | Maximum size of the asynchronous connection pool. |

---

## 3. Configuration Snippets

### Option A: Embedded Credentials
```yaml
sources:
  - name: store_mysql
    type: mysql
    url: "mysql://db_user:secure_password@localhost:3306/ecom_store"
    pool_size: 10
    tables:
      - name: products
        schema: store
      - name: inventory
        schema: store
```

### Option B: Separated Credentials (Environment Variables)
```yaml
sources:
  - name: store_mysql_secure
    type: mysql
    url: "mysql://localhost:3306/ecom_store"
    username: "db_user"
    password: "${env:MYSQL_PASSWORD}"
    pool_size: 10
    tables:
      - name: products
        schema: store
```
