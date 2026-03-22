# CLI Reference

The `strake-cli` is the primary tool for managing your Strake configuration, validating metadata, and performing GitOps-style deployments.

## Global Options

These options can be used with any command.

- `--output`, `-o`: `human | json | yaml`. *Default: human*.
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

## GitOps & Deployment

### `validate`

<div class="api-signature">
<code>strake-cli validate [file]</code>
<span class="type">command</span>
</div>

Check your configuration for syntax and connectivity errors without applying changes.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to validate.

`--offline` : `bool`, *default: false*
:   Skip network connectivity checks and perform only local schema validation.

`--ci` : `bool`, *default: false*
:   Strict mode: fail on warnings (coercions, drift). Useful for CI/CD pipelines.

---

### `diff`

<div class="api-signature">
<code>strake-cli diff [file]</code>
<span class="type">command</span>
</div>

Preview the differences between your local configuration and the live metadata store.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to compare.

---

### `apply`

<div class="api-signature">
<code>strake-cli apply [file]</code>
<span class="type">command</span>
</div>

Deploy your local configuration to the metadata store. In JSON mode, returns an `ApplyReceipt`.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to deploy.

`--force` : `bool`, *default: false*
:   Required for potentially destructive actions (e.g., deleting a source or bypassing orphaned references).

`--dry-run` : `bool`, *default: false*
:   Runs validation and previews changes (via `diff`) without actually persisting them.

`--expected-version` : `int`, *optional*
:   Enforces optimistic locking. The operation will fail if the current version in the metadata store does not match this value.

`--notify-url` : `str`, *optional*
:   URL to notify after successful application (for cache invalidation).

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
<code>strake-cli add &lt;source&gt; [table]</code>
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
:   Connect to the configured AI provider to generate natural language descriptions for the table and its columns.

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
<code>strake-cli remove &lt;source&gt; [table]</code>
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

## Domain Management

### `domain list`

<div class="api-signature">
<code>strake-cli domain list</code>
<span class="type">command</span>
</div>

List all registered domains currently tracked in the metadata store.

---

### `domain history`

<div class="api-signature">
<code>strake-cli domain history [name]</code>
<span class="type">command</span>
</div>

Show the audit trail of deployment events for a domain.

**Options:**

`name` : `str`, *default: default*
:   The name of the domain to inspect.

---

### `domain rollback`

<div class="api-signature">
<code>strake-cli domain rollback [name] --to-version &lt;n&gt;</code>
<span class="type">command</span>
</div>

Revert a domain to a previous known-good version.

**Options:**

`name` : `str`, *default: default*
:   The name of the domain to rollback.

`--to-version` : `int`
:   **Required**. The specific target version to revert to.

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
