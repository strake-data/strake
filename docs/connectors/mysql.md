# MySQL Connector

Strake supports querying MySQL and MariaDB transactional databases. It manages connection pooling asynchronously, optimizes SQL dialect translations, and pushes filters directly to the database.

---

## 1. Connection Syntax

The MySQL connection string is defined under the `connection` field of your SQL configuration. It supports standard MySQL URI formats:

```yaml
connection: "mysql://<username>:<password>@<host>:<port>/<database_name>"
```

### Connection Example
```yaml
connection: "mysql://db_user:secure_password@mysql-host:3306/sales_db"
```

---

## 2. Configuration Parameters

The MySQL database is configured as a `sql` source type with `dialect: mysql`:

| Parameter | Type | Required | Default | Description |
|:---|:---|:---|:---|:---|
| `dialect` | string | **Yes** | - | Must be `mysql`. |
| `connection` | string | **Yes** | - | MySQL connection URI containing credentials. |
| `pool_size` | integer | No | `10` | Maximum size of the asynchronous connection pool. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a MySQL database:

```yaml
sources:
  - name: store_mysql
    type: sql
    config:
      dialect: mysql
      connection: "mysql://db_user:secure_password@localhost:3306/ecom_store"
      pool_size: 10
      tables:
        - name: products
          schema: store
        - name: inventory
          schema: store
```
