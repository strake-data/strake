# Files & Cloud Storage Connector

Strake supports reading structured, semi-structured, and tabular files directly from local storage or cloud object buckets using the Apache OpenDAL abstraction engine.

---

## 1. Supported File Formats

- **Apache Parquet (`source_type: parquet`):** Vectorized reading with **predicate caching** and row-group pruning.
- **CSV (`source_type: csv`):** Delimited files with tunable delimiters (`delimiter`) and header support (`has_header`).
- **JSON (`source_type: json`):** NDJSON/semi-structured parsing.
- **Apache Avro (`source_type: avro`):** Binary schema serialization.
- **Microsoft Excel (`source_type: excel`):** XLSX parser (*Enterprise Edition only*).

---

## 2. Cloud Storage Schemes & Credentials

OpenDAL parses standard URL schemes from the `path` argument and dynamically maps credential options in the config block:

### AWS S3 (`s3://`)
Connects to Amazon S3 or S3-compatible endpoints (such as MinIO, Cloudflare R2, or Backblaze B2).
- **Required parameters:** `aws_access_key_id`, `aws_secret_access_key`, `region`.
- **Optional parameter:** `endpoint` (for non-AWS engines).

### Google Cloud Storage (`gs://` or `gcs://`)
Connects to Google Cloud GCS buckets.
- **Required parameters:** `google_application_credentials` (path to service account JSON), `region`.

### Azure Blob Storage (`az://` or `azblob://`)
Connects to Azure Container Storage.
- **Required parameters:** `account_name`, `account_key`.

### SFTP (`sftp://`)
Connects to secure SFTP servers.
- **Required parameters:** `user`, `password` or `key_path`.

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a Parquet source backed by AWS S3 with predicate caching enabled:

```yaml
sources:
  - name: telemetry_s3
    type: file
    source_type: parquet
    predicate_cache: true
    config:
      path: "s3://my-company-analytics-bucket/logs/"
      options:
        aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
        aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        region: "us-west-2"
      tables:
        - name: clickstream
          schema: public
          path: "s3://my-company-analytics-bucket/logs/clickstream.parquet"
```
