# Integration Tests Overview

This document provides a summary of the integration test suite for Strake, covering both Embedded and Remote execution modes.

## Test Files

| File | Description | Execution Mode | Key Tests |
|------|-------------|----------------|-----------|
| `tests/test_embedded.py` | Validates the Strake engine running in embedded mode (no server). | Embedded | JSON Query, Postgres Query, Cross-Source Join, Complex BI Query |
| `tests/test_s3.py` | Verifies S3 connectivity and Flight SQL protocol adherence. | Remote (gRPC) | S3 Glob Query, Simple Connectivity |
| `tests/test_strake.py` | Core functional tests for the Strake server. | Remote (gRPC) | Connection, Cross-Source Join, REST Source (Pagination), gRPC Source |
| `tests/test_rbac.py` | Validates Row-Level Security (RLS) and Role-Based Access Control. | Remote (gRPC) | Analyst Role Restrictions, Admin Access |

## Detailed Test Cases

### 1. Embedded Mode (`tests/test_embedded.py`)
These tests instantiate the `FederationEngine` directly within the Python process using `strakepy`.
- **`test_embedded_json`**: Queries a local JSON file to verify file-based source support.
- **`test_embedded_postgres`**: Connects to a Postgres database via the embedded engine.
- **`test_embedded_join_trace`**: Performs a join between Postgres and JSON sources, verifying the `trace()` functionality.
- **`test_complex_bi_query`**: Executes a complex analytical query involving CTEs, aggregations, and joins across multiple sources.

### 2. S3 Integration (`tests/test_s3.py`)
These tests connect to a running Strake server via Flight SQL to test S3 capabilities.
- **`test_s3_glob_query`**: Verifies that the server can correctly handle S3 path globbing (e.g., `s3://bucket/data/*.parquet`).
- **`test_simple_query`**: Basic connectivity check (`SELECT 1`).

### 3. Core Server Features (`tests/test_strake.py`)
General functional tests for the running server.
- **`test_connection`**: simple handshake to verify server reachability.
- **`test_cross_source_join`**: Joins data from two different registered sources (e.g., Postgres + JSON) to verify federation logic.
- **`test_rest_source_pagination`**: (Skipped if mock server missing) Tests the Generic REST source's ability to handle paginated responses.
- **`test_grpc_source`**: (Skipped if mock server missing) Tests the gRPC source connector.

### 4. Security (`tests/test_rbac.py`)
- **`test_rbac_analyst`**: Ensures that a user with the 'analyst' role is restricted by RLS policies (e.g., cannot see certain rows).
- **`test_rbac_admin`**: Placeholder to verify admin privileges.

## Running Tests

To run the full suite:
```bash
python3 -m pytest tests/
```

To run a specific test file:
```bash
python3 -m pytest tests/test_embedded.py
```
