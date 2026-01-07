# Installation

Strake consists of two main components:
1.  **Strake Client (Python)**: For writing queries and interacting with the engine.
2.  **Strake Server (Rust)**: The core engine that runs the queries (can also be embedded in the Python client).

## Prerequisites

*   **Linux or macOS** (Windows support is experimental).
*   **Python 3.9+**
*   **Rust 1.75+** (Only required if building from source).

---

## 1. Installing the Python Client

Strake provides a high-performance Python client written in Rust.

### From PyPI (Recommended)
    ```bash
    ## uv
    uv init strake-demo
    uv pip add strake
    ```

    ```bash
    ## pip
    pip install strake
    ```

## 2. Universal Install Script (Linux/macOS)

The fastest way to install Strake binaries is via the universal install script:

```bash
# Install CLI only (default)
curl -sSfL https://strakedata.com/install.sh | sh

# Install all binaries (CLI, Server, Enterprise)
curl -sSfL https://strakedata.com/install.sh | sh -s -- --all

# Install specific components
curl -sSfL https://strakedata.com/install.sh | sh -s -- --cli --server

# Install a specific version
curl -sSfL https://strakedata.com/install.sh | sh -s -- --cli --version v0.1.0
```

### Options

| Flag | Description |
|------|-------------|
| `--all`, `-a` | Install all binaries (cli, server, enterprise) |
| `--cli` | Install `strake-cli` only |
| `--server` | Install `strake-server` only |
| `--enterprise` | Install `strake-enterprise` only |
| `--version`, `-v` | Install a specific version (e.g., `v0.1.0`) |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STRAKE_INSTALL_DIR` | `~/.local/bin` | Installation directory |

> **Note:** Make sure `~/.local/bin` is in your `PATH`. The script will remind you if it's not.

## 2. Building from Source
To build the Python bindings locally:

1.  Navigate to the `strake-python` directory:
    ```bash
    cd strake-python
    ```
2.  Create a virtual environment:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```
3.  Install build dependencies and build:
    ```bash
    pip install maturin
    maturin develop --release
    ```

---

## 3. Running the Server

You can run Strake as a standalone server exposing an Apache Arrow Flight SQL interface.

### Open Source Edition (OSS)
Run the standard server for local development or basic deployments.

```bash
# From the project root
cargo run --package strake-server --release
```

The server will start on `0.0.0.0:50051`.

### Server Configuration

The Strake Server can be configured via environment variables or a `.env` file. These settings override values in `config/strake.yaml`.

| Variable | Default | Description |
|----------|---------|-------------|
| `STRAKE_SERVER__LISTEN_ADDR` | `0.0.0.0:50051` | The address and port for the gRPC server |
| `STRAKE_SERVER__HEALTH_ADDR` | `0.0.0.0:8080` | The address and port for the health check and API |
| `STRAKE_SERVER__CATALOG` | `strake` | The default catalog name |
| `STRAKE_SERVER__GLOBAL_CONNECTION_BUDGET` | `100` | Max concurrent connection budget for the server |
| `STRAKE_API_URL` | `http://localhost:8080/api/v1` | Public URL for the Strake API |
| `STRAKE_AUTH__ENABLED` | `false` | Enable/Disable authentication checks |
| `STRAKE_AUTH__API_KEY` | `dev-key` | Static API key for auth (if enabled) |
| `STRAKE_RETRY__MAX_ATTEMPTS` | `5` | Max retries for establishing upstream connections |
| `STRAKE_QUERY_LIMITS__MAX_OUTPUT_ROWS` | `None` | Hard limit on rows returned by a query |

