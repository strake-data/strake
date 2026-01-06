# Contributing to Strake

Thank you for your interest in contributing to Strake! We welcome contributions from the community to help make Strake better.

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally:
    ```bash
    git clone https://github.com/your-username/strake.git
    cd strake
    ```
3.  **Create a new branch** for your feature or bug fix:
    ```bash
    git checkout -b feature/my-awesome-feature
    ```

## Development Workflow

Strake is a Rust project managed by Cargo.

-   **Build the project**:
    ```bash
    cargo build
    ```
-   **Run tests**:
    ```bash
    cargo test
    ```
-   **Run lints**:
    ```bash
    cargo clippy --workspace --all-targets --all-features
    cargo fmt --all -- --check
    ```

## Pull Request Process

1.  Ensure your code adheres to the existing style and conventions.
2.  Make sure all tests pass and there are no linting errors.
3.  Push your changes to your fork.
4.  Open a Pull Request against the `main` branch of the `strake` repository.
5.  Provide a clear description of your changes and the problem they solve.

## Code of Conduct

Please note that this project is released with a [Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project, you agree to abide by its terms.

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 license, as defined in the `LICENSE` file.
