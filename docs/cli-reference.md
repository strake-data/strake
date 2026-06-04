# CLI Reference

The `strake-cli` is the primary tool for managing your Strake configuration, validating metadata, and synchronizing schema definitions.

## Global Options

These options can be used with any command.

- `--output`: `human | json | yaml`. *Default: human*.
  Sets the output format. Machine-readable formats (`json`, `yaml`) are suitable for CI/CD and automation.
- `--token`: `str`. *Default: STRAKE_TOKEN environment variable*.
  API token for authentication with the Strake server.
- `--profile`: `str`. *Default: STRAKE_PROFILE environment variable*.
  Specifies the configuration profile to use from `strake.yaml`.

---

## Project Lifecycle

### `init`

<div class="api-signature">
<code>strake-cli init [file]</code>
<span class="type">command</span>
</div>

Initialize a new Strake project with a template configuration.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path where the new configuration file should be created.

`--template` : `sql | rest | file | grpc`, *optional*
:   The type of project template to generate as a starting point.

`--sources-only` : `bool`, *default: false*
:   Only create `sources.yaml`, skipping `strake.yaml` and `README.md`. Also skips metadata database initialization.

---

## GitOps & Schema Synchronization

### `validate`

<div class="api-signature">
<code>strake-cli validate [file]</code>
<span class="type">command</span>
</div>

Verify configuration validity, detect live schema drift, and optionally emit a machine-readable CI receipt or notify a webhook.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to validate.

`--offline` : `bool`, *default: false*
:   Skip semantic validation (network calls) and perform only local syntax validation.

`--fail-on-warnings` : `bool`, *default: false*
:   Strict mode: treat warnings (coercions, drift) as hard validation failures and exit with code `1`.

`--dry-run` : `bool`, *default: false*
:   Compute and print the live schema diff, but skip sending webhook notifications. Exits with code `3`.

`--notify-url` : `str`, *optional*
:   Callback URL to POST the `ValidateReceipt` JSON to on success (only fires if not a dry-run).

---

#### Machine-Readable Output

When running with the global `--output json` flag, `validate` emits a structured JSON receipt (`ValidateReceipt`) that is versioned and ideal for consumption by CI/CD pipelines:

```json
{
  "receipt_version": 1,
  "validated_at": "2026-06-04T19:58:15Z",
  "actor": "git-sha-or-profile",
  "domain": "my-domain",
  "valid": true,
  "errors": [],
  "warnings": [],
  "dry_run": false,
  "drift_detected": false,
  "duration_ms": 142
}
```

**JSON Fields:**
- `receipt_version` (`u32`): Schema version of the receipt format (currently `1`).
- `validated_at` (`string`): ISO 8601 timestamp at which validation completed.
- `actor` (`string`): Identity derived from the `STRAKE_ACTOR` or `STRAKE_PROFILE` environment variables.
- `domain` (`string`): Domain name defined in `sources.yaml`, or `"default"` if absent.
- `valid` (`bool`): Whether the configuration passed all validation checks.
- `errors` (`array[string]`): List of validation errors, if any.
- `warnings` (`array[string]`): Non-fatal warnings (e.g., coercible drift, missing optional fields).
- `dry_run` (`bool`): Indicates if this was run with the `--dry-run` flag.
- `drift_detected` (`bool`): Whether column/schema drift was detected between the local configuration and live remote databases.
- `duration_ms` (`u64`): Total execution time of the validation run in milliseconds.

#### Exit Codes

The command returns distinct exit codes for CI integration:
- `0` (Success): Configuration is valid, with no errors or warnings.
- `1` (Error): Validation failed, or warnings were treated as errors via `--fail-on-warnings`.
- `2` (Warnings): Validation passed, but non-fatal warnings/drift were detected.
- `3` (Dry Run): Command was run with `--dry-run`; diff was printed, webhook skipped.

---

### `diff`

<div class="api-signature">
<code>strake-cli diff [file]</code>
<span class="type">command</span>
</div>

Preview the differences between the local schema definitions in `sources.yaml` and live remote database schemas. It performs stateless introspection to detect table additions, deletions, column type mismatches, and nullability drift without relying on database-stored metadata state.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to compare.

`--impact` : `bool`, *default: false*
:   Enable inline impact annotation for changes.

---

### `sync`

<div class="api-signature">
<code>strake-cli sync [file]</code>
<span class="type">command</span>
</div>

Introspect live remote database schemas and update the local `sources.yaml` schema definitions in-place, preserving hand-written table and column descriptions.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file (e.g. `sources.yaml`) to sync.

---

### `status`

<div class="api-signature">
<code>strake-cli status [file]</code>
<span class="type">command</span>
</div>

Aggregated health view of a domain, including source reachability, contract violations, and drift.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the local configuration file (used to resolve domain).

`--domain` : `str`, *optional*
:   Explicitly specify the domain to check.

`--timeout` : `int`, *default: 5000*
:   Timeout for reachability checks in milliseconds.

---

## Discovery & Inspection

### `search`

<div class="api-signature">
<code>strake-cli search &lt;source&gt; [file]</code>
<span class="type">command</span>
</div>

Search for tables and schemas in an upstream source.

**Options:**

`source` : `str`
:   The name of the source to search.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file (used to find source connection details).

`--domain` : `str`, *optional*
:   Narrow the search scope to a specific domain.

---

### `add`

<div class="api-signature">
<code>strake-cli add &lt;source&gt; [table] [file]</code>
<span class="type">command</span>
</div>

Automatically adds one or more discovered tables into your `sources.yaml`.

**Options:**

`source` : `str`
:   The name of the source.

`table` : `str`, *optional*
:   The full name of the table to add (e.g., `schema.table`). Required unless `--all`, `--pattern`, or `--stdin` is used.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to update.

`--full` : `bool`, *default: false*
:   Perform deep introspection of the source (e.g., fetching constraints and native types).

`--ai-descriptions` : `bool`, *default: false*
:   Connect to the configured AI provider to generate natural language descriptions for the table and its columns. See the [AI-Powered Metadata Enrichment Guide](./metadata-enrichment.md) for details.

`--merge` : `bool`, *default: true*
:   Fill in missing fields from introspection but preserve any existing manual edits in `sources.yaml`.

`--overwrite` : `bool`, *default: false*
:   Completely replace the existing table entry with fresh introspection results. If both `--merge` and `--overwrite` are provided, the last one wins.

`--pattern` : `str`, *optional*
:   Bulk add: Add all tables matching a glob pattern (e.g., `public.*`).

`--all` : `bool`, *default: false*
:   Bulk add: Add every table discoverable from the source.

`--stdin` : `bool`, *default: false*
:   Bulk add: Read a list of schema-qualified table names from standard input.

`--yes` : `bool`, *default: false*
:   Skip confirmation prompts for destructive changes or large bulk operations.

`--dry-run` : `bool`, *default: false*
:   Show the changes that would be made without persisting them.

`--to-contracts` : `bool`, *default: false*
:   Automatically promote the introspected schema to `contracts.yaml`.

---

### `remove`

<div class="api-signature">
<code>strake-cli remove &lt;source&gt; [table] [file]</code>
<span class="type">command</span>
</div>

Safely remove a table or source entry from `sources.yaml`. Checks for orphaned contract or policy references.

**Options:**

`source` : `str`
:   The name of the source.

`table` : `str`, *optional*
:   The name of the table to remove. Required unless `--source-only` is used.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to update.

`--dry-run` : `bool`, *default: false*
:   Perform checks without writing changes to the file.

`--force` : `bool`, *default: false*
:   Proceed with removal even if orphaned contract or policy references are detected.

`--source-only` : `bool`, *default: false*
:   Remove the entire source entry (currently a stub).

---

### `test-connection`

<div class="api-signature">
<code>strake-cli test-connection [file]</code>
<span class="type">command</span>
</div>

Read the configuration and attempt to connect to every defined source to verify credentials and network reachability.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to test.

---

### `describe`

<div class="api-signature">
<code>strake-cli describe [file]</code>
<span class="type">command</span>
</div>

Shows the current configuration and metadata stored in the metadata database for a domain.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the local configuration file (used to resolve domain).

`--domain` : `str`, *optional*
:   Explicitly specify the domain to describe.

---



## Security

### `secrets validate`

<div class="api-signature">
<code>strake-cli secrets validate [file]</code>
<span class="type">command</span>
</div>

Validate secret references (${env:VAR}, etc.) in your configuration.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to validate.

`--offline` : `bool`, *default: false*
:   Skip external secret provider checks, only validate local environment and syntax.

---

## Database Management

### `db init`

<div class="api-signature">
<code>strake-cli db init</code>
<span class="type">command</span>
</div>

Initialize a fresh metadata database schema. This creates all necessary system tables and runs initial setup.

---

### `db migrate`

<div class="api-signature">
<code>strake-cli db migrate</code>
<span class="type">command</span>
</div>

Run outstanding database schema migrations to upgrade the metadata store to the latest version.

---

## API Key Management

### `apikey create`

<div class="api-signature">
<code>strake-cli apikey create --name &lt;name&gt; --user &lt;user&gt;</code>
<span class="type">command</span>
</div>

Generate a new cryptographically secure API key, hash it with Argon2id, and register it in the metadata store. The full secret key is output exactly once.

**Options:**

`--name` : `str`
:   **Required**. A human-readable name to easily identify the key later.

`--user` : `str`
:   **Required**. The user ID or actor name associated with the key.

`--description` : `str`, *optional*
:   An optional description of the key's purpose.

`--permissions` : `str`, *default: read*
:   Comma-separated list of permissions (e.g., `read,write`).

---

### `apikey list`

<div class="api-signature">
<code>strake-cli apikey list</code>
<span class="type">command</span>
</div>

List all registered API keys in the store, ordered by creation time descending. Truncates long fields for alignment in human-readable mode.

---

### `apikey revoke`

<div class="api-signature">
<code>strake-cli apikey revoke &lt;prefix&gt;</code>
<span class="type">command</span>
</div>

Revoke an active API key using its unique 8-character prefix. In JSON/YAML mode, returns a structured object indicating revocation success.

**Options:**

`prefix` : `str`
:   **Required**. The 8-character prefix of the API key to revoke.
