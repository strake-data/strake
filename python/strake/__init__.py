try:
    from ._strake import *
    from ._strake import __doc__, __all__
except ImportError:
    pass

def connect(dsn_or_config: str = "strake.yaml", sources_config: str = None, api_key: str = None, mode: str = None):
    """
    Convenience function to connect to Strake.
    
    :param dsn_or_config: Path to strake.yaml (embedded) or grpc:// URL (remote).
    :param mode: If "embedded", uses local execution. If "remote", requires a grpc:// URL.
    """
    # If mode is explicitly set to embedded and no dsn provided, use default
    if mode == "embedded" and not dsn_or_config:
        dsn_or_config = "strake.yaml"
    
    conn = StrakeConnection(dsn_or_config, sources_config, api_key)
    # Add an 'execute' alias for 'sql' to be more familiar to some users
    if not hasattr(conn, "execute"):
        conn.execute = conn.sql
    return conn

