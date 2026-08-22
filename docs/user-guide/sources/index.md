# Data Sources (Connectors) Overview

Strake is a high-performance, federation-first query engine. By translating query plans and pushing predicate filters directly to where your data lives, Strake allows you to query diverse databases, files, object stores, and APIs as a single logical data warehouse.

---

## 1. Connector Feature Matrix

Before exploring individual setups, use this comparison matrix to identify the capabilities supported by each connector:

| Connector | Source Type | Pushdown Support | Auth Methods | Schema Discovery |
|---|---|---|---|---|
| **[PostgreSQL](postgres.md)** | Relational (`sql`) | Filter, Projection, Limit | Password, TLS | Automatic |
| **[MySQL](mysql.md)** | Relational (`sql`) | Filter, Projection, Limit | Password | Automatic |
| **[SQLite](sqlite.md)** | Relational (`sql`) | Filter, Projection, Limit | None | Automatic |
| **[DuckDB](duckdb.md)** | Relational (`sql`) | Filter, Projection, Limit | None | Automatic |
| **[ClickHouse](clickhouse.md)** | Relational (`sql`) | Filter, Projection, Limit | Password | Automatic |
| **[Oracle](oracle.md)** | Relational (`sql`) | Filter, Projection, Limit | Password | Automatic |
| **[Files & Cloud Storage](file.md)** | Object Store (`file`) | Filter (Parquet), Projection, Limit | IAM, Access Keys | Auto-infer / Explicit |
| **[Apache Iceberg](iceberg.md)** | Lakehouse (`iceberg_rest`) | Filter, Projection | OAuth 2.0 | Automatic |
| **[Arrow Flight SQL](flight_sql.md)** | Vectorized Warehouse | Query delegation | Token-based | Remote |
| **[REST APIs](rest.md)** | HTTP Service (`rest`) | Pushdown mappings | Basic, Bearer, OAuth, JWT | Declarative (YAML) |
| **[gRPC Services](grpc.md)** | Microservice (`grpc`) | Parameter injection | None / Custom | Protobuf / Descriptor |

---

## 2. Modular Connector Directory

Select a connector below for detailed configuration parameters, authentication schemes, connection string syntaxes, and driver setups:

### Relational Databases (`type: sql`)
- **[PostgreSQL](postgres.md):** Active wire integration with deep predicate/limit pushdown.
- **[MySQL & MariaDB](mysql.md):** Optimized SQL dialect translation and connection pooling.
- **[SQLite](sqlite.md):** Ultra-lightweight local caching and zero-config testing.
- **[DuckDB](duckdb.md):** Persistent local file or in-memory analytics.
- **[ClickHouse](clickhouse.md):** OLAP-optimized translations for fast table queries.
- **[Oracle](oracle.md):** Thin connection syntax and detailed Oracle Instant Client driver requirements.

### Storage, API & Microservices
- **[Files & Cloud Object Storage](file.md) (`type: file`):** OpenDAL backend supporting Parquet, CSV, JSON, Avro, and Excel across S3, GCS, and Azure buckets.
- **[Apache Iceberg](iceberg.md) (`type: iceberg_rest`):** *Experimental* REST catalog queries and time-travel snapshots.
- **[Arrow Flight SQL](flight_sql.md) (`type: flight_sql`):** Low-overhead vectorized warehouse querying (Snowflake, Dremio).
- **[REST APIs & SaaS](rest.md) (`type: rest`):** Declarative endpoint mapping, auth headers, pagination, and URL pushdowns.
- **[gRPC Services](grpc.md) (`type: grpc`):** Microservice calls using Protobuf reflection or FileDescriptorSet binaries.

---

## 3. Unified `sources.yaml`

This comprehensive `sources.yaml` registers and configures all supported data sources inside a single Strake project directory:

```yaml
# sources.yaml
sources:
  # ----------------------------------------------------
  # 1. PostgreSQL (Relational Database)
  # ----------------------------------------------------
  - name: internal_pg
    type: sql
    url: "postgres://db_user:secure_password@localhost:5432/production_db?sslmode=prefer"
    config:
      dialect: postgres
      pool_size: 15
      tables:
        - name: users
          schema: public

  # ----------------------------------------------------
  # 2. Oracle Database (Requires Instant Client Driver)
  # ----------------------------------------------------
  - name: legacy_oracle
    type: sql
    url: "oracle://system:OraclePassword123@oracle-free:1521/FREEPDB1"
    config:
      dialect: oracle
      pool_size: 10
      # Non-default schemas (e.g. GWS_DWH) are automatically qualified in SQL.
      # Use schema_mapping.default_schemas: [] to force qualification for all schemas.
      tables:
        - name: orders
          schema: sales

  # ----------------------------------------------------
  # 3. SQLite (Local Database)
  # ----------------------------------------------------
  - name: cache_sqlite
    type: sql
    url: "sqlite:///workspaces/rust-postgres/data/app_cache.db"
    config:
      dialect: sqlite

  # ----------------------------------------------------
  # 4. Amazon S3 Parquet (Files & Cloud Storage)
  # ----------------------------------------------------
  - name: telemetry_s3
    type: file
    format: parquet
    predicate_cache: true
    url: "s3://my-company-analytics-bucket/logs/"
    config:
      options:
        aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
        aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        region: "us-west-2"
      tables:
        - name: clickstream
          schema: public
          path: "s3://my-company-analytics-bucket/logs/clickstream.parquet"

  # ----------------------------------------------------
  # 5. Apache Iceberg (Experimental)
  # ----------------------------------------------------
  - name: analytics_iceberg
    type: iceberg_rest
    url: "http://polaris:8181/api/catalog"
    config:
      warehouse: "s3://my-iceberg-lakehouse/"
      region: "us-east-1"
      oauth_client_id: "strake-client"
      oauth_client_secret: "super_secret_oauth_token"
      oauth_token_url: "http://polaris:8181/api/catalog/v1/oauth/tokens"

  # ----------------------------------------------------
  # 6. Arrow Flight SQL (Warehouse Federation)
  # ----------------------------------------------------
  - name: warehouse_flight
    type: flight_sql
    config:
      url: "grpc://localhost:32010"

  # ----------------------------------------------------
  # 7. Generic REST API (SaaS Integration)
  # ----------------------------------------------------
  - name: stripe_api
    type: rest
    config:
      base_url: "https://api.stripe.com/v1/charges"
      method: "GET"
      headers:
        Accept: "application/json"
      auth:
        type: bearer
        token: "sk_test_51Nz..."
      pagination:
        type: token
        token_path: "next_page_token"
        param_name: "starting_after"
      pushdown:
        - column: "customer_id"
          operator: "="
          param: "customer"

  # ----------------------------------------------------
  # 8. gRPC Service (Microservice Federation)
  # ----------------------------------------------------
  - name: user_service_grpc
    type: grpc
    config:
      url: "http://user-service.internal:50051"
      service: "my.company.UserService"
      method: "GetActiveUsers"
      descriptor_set: "/workspaces/rust-postgres/data/user_service_desc.bin"
      request_body: '{"status": "ACTIVE"}'
      columns:
        - name: "user_id"
          type: "Int64"
        - name: "email"
          type: "Utf8"
```

---

## 3. Python Script (`main.py`)

Save the following Python script alongside your `sources.yaml` to initialize the Strake engine in **embedded mode**, inspect your federated catalogs, and run cross-source database joins:

```python
#!/usr/bin/env python3
import os
import sys
import strake
import pandas as pd

def main():
    config_path = "sources.yaml"
    
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.", file=sys.stderr)
        print("Please save the sources.yaml configuration in this directory.", file=sys.stderr)
        sys.exit(1)

    print("====================================================")
    print("🚀 Initializing Strake Embedded Federation Engine...")
    print("====================================================")

    # 1. Connect (Embedded Library Mode)
    try:
        conn = strake.StrakeConnection(config_path)
        print("✓ Successfully initialized embedded Strake connection.")
    except Exception as e:
        print(f"✗ Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Introspect Registered Tables
    print("\n----------------------------------------------------")
    print("🔍 Introspecting Available Federated Tables:")
    print("----------------------------------------------------")
    try:
        # Describe fetches and prints a text grid of all catalogs
        schema_description = conn.describe()
        print(schema_description)
    except Exception as e:
        print(f"Warning: Could not fetch automatic description: {e}")

    # 3. Perform a Cross-Source Federated Join
    print("\n----------------------------------------------------")
    print("⚡ Running Cross-Source Federated JOIN...")
    print("----------------------------------------------------")
    
    # Joining:
    # - Postgres users (internal_pg.public.users)
    # - Oracle orders (legacy_oracle.sales.orders)
    # - S3 Clickstream parquet files (telemetry_s3.public.clickstream)
    query = """
        SELECT 
            u.user_id,
            u.email,
            o.order_id,
            o.amount,
            c.page_path,
            c.session_duration
        FROM strake.internal_pg.public.users u
        INNER JOIN strake.legacy_oracle.sales.orders o 
            ON u.user_id = o.user_id
        LEFT JOIN strake.telemetry_s3.public.clickstream c 
            ON u.email = c.user_email
        WHERE o.amount > 100.0
        ORDER BY o.amount DESC
        LIMIT 10
    """

    print(f"Executing Query:\n{query}\n")
    
    try:
        # conn.sql returns a Strake Table
        result_table = conn.sql(query)
        
        # 4. Convert Arrow Results directly to a Pandas DataFrame
        df = result_table.to_pandas()
        
        print("====================================================")
        if df.empty:
            print("Query completed successfully, but returned 0 rows.")
        else:
            print(f"✓ Query returned {len(df)} rows:")
            print(df.to_string(index=False))
        print("====================================================")
        
    except Exception as e:
        print(f"✗ Query execution failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```
