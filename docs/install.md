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
    uv init strake-data
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

