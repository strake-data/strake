# CLI Project Lifecycle Commands

These commands manage the lifecycle of your Strake project, validate schemas against live remote databases, and synchronize definitions.

## `init`

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

## `validate`

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

### Machine-Readable Output

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

### Exit Codes

The command returns distinct exit codes for CI integration:
- `0` (Success): Configuration is valid, with no errors or warnings.
- `1` (Error): Validation failed, or warnings were treated as errors via `--fail-on-warnings`.
- `2` (Warnings): Validation passed, but non-fatal warnings/drift were detected.
- `3` (Dry Run): Command was run with `--dry-run`; diff was printed, webhook skipped.

---

## `diff`

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

## `sync`

<div class="api-signature">
<code>strake-cli sync [file]</code>
<span class="type">command</span>
</div>

Introspect live remote database schemas and update the local `sources.yaml` schema definitions in-place, preserving hand-written table and column descriptions.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file (e.g. `sources.yaml`) to sync.

---

## `status`

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
