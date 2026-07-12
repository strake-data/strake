# CLI Security & Secrets Commands

These commands allow you to validate credentials, check environment configuration, and test network connectivity to remote sources.

## `secrets validate`

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

## `test-connection`

<div class="api-signature">
<code>strake-cli test-connection [file]</code>
<span class="type">command</span>
</div>

Read the configuration and attempt to connect to every defined source to verify credentials and network reachability.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to test.
