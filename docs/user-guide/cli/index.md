# CLI Reference Overview

The `strake-cli` is the primary tool for managing your Strake configuration, validating metadata, and synchronizing schema definitions.

## Global Options

These options can be used with any command.

- `--output`: `human | json | yaml`. *Default: human*.
  Sets the output format. Machine-readable formats (`json`, `yaml`) are suitable for CI/CD and automation.
- `--token`: `str`. *Default: STRAKE_TOKEN environment variable*.
  API token for authentication with the Strake server.
- `--profile`: `str`. *Default: STRAKE_PROFILE environment variable*.
  Specifies the configuration profile to use from `strake.yaml`.
