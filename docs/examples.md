# Examples

Practical, real-world examples demonstrating how to write federated SQL queries and orchestrate diverse data sources.

---

## 1. Cross-Source Join: Postgres + S3 Parquet

Join hot transactional user records stored in a local **PostgreSQL** database with historical clickstream logs stored in **AWS S3** as Parquet files.

### Scenario
Your enterprise analytics team has:
*   A `users` table inside **PostgreSQL** containing active accounts and user profile details.
*   A `clickstream` dataset inside **AWS S3** stored as monthly Parquet objects containing log activity.

### Configuration (`sources.yaml`)

```yaml
sources:
  - name: internal_pg
    type: sql
    url: "postgres://db_user:secure_password@localhost:5432/production_db?sslmode=prefer"
    config:
      dialect: postgres
      tables:
        - name: users
          schema: public

  - name: telemetry_s3
    type: file
    format: parquet
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
```

### Python Execution Script

Run a single federated SQL statement that joins both sources in-memory and extracts the active users with the most event triggers:

```python
import strake

# 1. Connect using the local configuration
conn = strake.StrakeConnection("sources.yaml")

# 2. Run the federated cross-source join query
query = """
    SELECT 
        u.email, 
        count(c.event_id) as event_count
    FROM strake.internal_pg.public.users u
    INNER JOIN strake.telemetry_s3.public.clickstream c 
        ON u.email = c.user_email
    WHERE c.event_date > '2023-01-01'
    GROUP BY u.email
    ORDER BY event_count DESC
    LIMIT 10
"""

print("Executing cross-source query...")
result = conn.sql(query)

# 3. Convert results to a pandas DataFrame and display
df = result.to_pandas()
print(df.to_string(index=False))
```
