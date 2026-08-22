from __future__ import annotations

try:
    from ._strake import *
    from ._strake import __doc__, __all__
except ImportError:
    pass


def connect(
    dsn_or_config: str | None = None,
    sources_config: str | None = None,
    api_key: str | None = None,
    mode: str | None = None,
    trace_dir: str | None = None,
) -> StrakeConnection:
    """
    Convenience function to connect to Strake.

    :param dsn_or_config: Path to strake.yaml (embedded) or grpc:// URL (remote).
                          Defaults to ``"strake.yaml"`` in the current directory.
    :param sources_config: Optional path to sources.yaml.
    :param api_key: Optional API key for authenticating with remote servers.
    :param mode: If "embedded", forces local execution. If "remote", requires a grpc:// URL.
    :param trace_dir: Optional directory to store session traces. Defaults to
                      ``.strake/traces/`` relative to the running script.
    :raises ConfigError: If configuration parsing or validation fails.
    :raises ConnectionError: If connecting to the remote server fails.
    """
    if trace_dir:
        from .tracing import get_emitter

        get_emitter(trace_dir=trace_dir)

    if dsn_or_config is None:
        dsn_or_config = "strake.yaml"
    elif mode == "embedded" and not dsn_or_config:
        dsn_or_config = "strake.yaml"

    conn = StrakeConnection(dsn_or_config, sources_config, api_key)
    return conn
