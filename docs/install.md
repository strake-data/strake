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

Strake provides a high-performance Python client based on PyO3.

### From PyPI (Recommended)
*Coming soon! Strake is currently in active development.*

### Building from Source
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

## 2. Running the Server

You can run Strake as a standalone server exposing an Apache Arrow Flight SQL interface.

### Open Source Edition (OSS)
Run the standard server for local development or basic deployments.

```bash
# From the project root
cargo run --package strake-server --release
```

The server will start on `0.0.0.0:50051`.

### Enterprise Edition
The Enterprise Edition includes OIDC SSO, RLS, and advanced governance.

```bash
# Set your license key (or use the dev bypass for testing)
export STRAKE_LICENSE_KEY="sk_ent_dev_bypass"

# Run the enterprise server
cargo run --package strake-enterprise --release
```

---

## 3. Installing the CLI

The `strake-cli` helps manage configuration and validating GitOps workflows.

```bash
cargo install --path strake-cli
```

Verify installation:
```bash
strake-cli --version
```
