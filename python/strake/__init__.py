from typing import Optional

try:
    from ._strake import *
    from ._strake import __doc__, __all__
except ImportError:
    pass


def connect(
    dsn_or_config: Optional[str] = None,
    sources_config: Optional[str] = None,
    api_key: Optional[str] = None,
    mode: Optional[str] = None,
) -> "StrakeConnection":
    """
    Convenience function to connect to Strake.

    :param dsn_or_config: Path to strake.yaml (embedded) or grpc:// URL (remote).
                          Defaults to ``"strake.yaml"`` in the current directory.
    :param sources_config: Optional path to sources.yaml.
    :param api_key: Optional API key for authenticating with remote servers.
    :param mode: If "embedded", forces local execution. If "remote", requires a grpc:// URL.
    """
    if dsn_or_config is None:
        dsn_or_config = "strake.yaml"
    elif mode == "embedded" and not dsn_or_config:
        dsn_or_config = "strake.yaml"

    conn = StrakeConnection(dsn_or_config, sources_config, api_key)
    return conn
