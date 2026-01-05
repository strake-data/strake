import pytest
import strakepy
import pandas as pd
import os

# Assuming strake-server is running for remote tests
# If not, we can skip remote tests or mock them
REMOTE_DSN = "grpc://localhost:50053"
LOCAL_CONFIG = "../config/strake.yaml"

@pytest.fixture
def local_conn():
    if not os.path.exists(LOCAL_CONFIG):
        pytest.skip(f"Local config {LOCAL_CONFIG} not found")
    return strakepy.StrakeConnection(LOCAL_CONFIG)

@pytest.fixture
def remote_conn():
    # Attempt to connect to a running server
    try:
        conn = strakepy.StrakeConnection(REMOTE_DSN)
        return conn
    except Exception as e:
        pytest.skip(f"Remote server not available at {REMOTE_DSN}: {e}")

def test_local_connection(local_conn):
    assert local_conn is not None
    # Test a simple query
    df = local_conn.sql("SELECT 1 as val").to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert df["val"][0] == 1

def test_local_join(local_conn):
    pytest.skip("Requires full data environment setup")

def test_remote_connection(remote_conn):
    assert remote_conn is not None
    df = remote_conn.sql("SELECT 1 as val").to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert df["val"][0] == 1

def test_remote_join(remote_conn):
    pytest.skip("Requires full data environment setup")

def test_trace(local_conn):
    # Trace should not raise errors and return Explain plan
    out = local_conn.trace("SELECT 1")
    assert "LogicalPlan" in out or "Projection: Int64(1)" in out

def test_describe(local_conn):
    # Describe should return table list
    out = local_conn.describe()
    assert "+-" in out # formatting
    assert "information_schema" in out # default tables check

def test_context_manager():
    # Test context manager support
    if not os.path.exists(LOCAL_CONFIG):
         pytest.skip(f"Local config {LOCAL_CONFIG} not found")
    
    with strakepy.StrakeConnection(LOCAL_CONFIG) as conn:
        assert conn is not None
        df = conn.sql("SELECT 1 as val").to_pandas()
        assert df["val"][0] == 1

def test_params_not_implemented(local_conn):
    # Params should raise NotImplementedError (ValueError for now)
    with pytest.raises(ValueError, match="Not yet implemented"):
        local_conn.sql("SELECT 1", params={"id": 1})
