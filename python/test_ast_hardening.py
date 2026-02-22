"""
Tests for validate_ast — the fast-path import allowlist and code-size check.

Scope: These tests cover what validate_ast actually enforces:
  1. Code-size limit
  2. Import allowlist (both AST and the _safe_import_shim at runtime)
  3. Pattern matching block (niche, low-maintenance)
  4. Naive getattr introspection check (convenience, not a hard security guarantee)

Tests for name/attribute deny-lists (DANGEROUS_NAMES, dunder blocking) have been
removed because those checks were removed from validate_ast — they were high-
maintenance, trivially bypassed, and created false confidence. OS-level isolation
is the real security boundary, not Python AST inspection.
"""
import sys
import os
from strake.sandbox.core import validate_ast, ALLOWED_IMPORTS, MAX_CODE_SIZE


def test_import_allowlist_blocks_dangerous():
    """Dangerous imports (os, subprocess, etc.) are rejected."""
    cases = [
        ("import os", "os"),
        ("import subprocess", "subprocess"),
        ("import socket", "socket"),
        ("from requests import get", "requests"),
        ("import urllib.request", "urllib"),
    ]
    for code, module in cases:
        err = validate_ast(code)
        assert err is not None, f"Expected error for 'import {module}', got None"
        assert "not permitted" in err, f"Unexpected error message: {err}"
    print("✓ Import allowlist blocks dangerous modules")


def test_import_allowlist_permits_safe():
    """Safe data-science and stdlib modules are allowed through."""
    cases = [
        "import pandas",
        "import numpy as np",
        "from math import sqrt",
        "import json",
        "from typing import Optional",
        "from datetime import datetime",
    ]
    for code in cases:
        err = validate_ast(code)
        assert err is None, f"Unexpected rejection of '{code}': {err}"
    print("✓ Import allowlist permits safe modules")


def test_code_size_limit():
    """Code exceeding MAX_CODE_SIZE is rejected immediately."""
    big_code = "x = 1\n" * (MAX_CODE_SIZE // 6 + 1)
    err = validate_ast(big_code)
    assert err is not None
    assert "Code size exceeds" in err
    print(f"✓ Code size limit ({MAX_CODE_SIZE} bytes) enforced")


def test_pattern_matching_blocked():
    """Pattern matching syntax is blocked (Python 3.10+)."""
    if sys.version_info < (3, 10):
        print("⚠ Skipping pattern match test (Python < 3.10)")
        return
    code = """
match x:
    case 1:
        print("one")
"""
    err = validate_ast(code)
    assert err is not None
    assert "Pattern matching syntax not allowed" in err
    print("✓ Pattern matching blocked")


def test_normal_code_allowed():
    """Normal data-analysis code passes without error."""
    code = """
import math
import json

def compute(values):
    total = sum(values)
    mean = total / len(values)
    return {"total": total, "mean": mean}

result = compute([1, 2, 3, 4, 5])
print(json.dumps(result))
"""
    err = validate_ast(code)
    assert err is None, f"Valid code was rejected: {err}"
    print("✓ Normal data-analysis code passes")


def test_naive_getattr_blocked():
    """Direct getattr calls targeting dunder attrs are caught."""
    code = "getattr(obj, '__class__')"
    err = validate_ast(code)
    assert err is not None
    assert "Dangerous getattr target" in err
    print("✓ Naive getattr introspection blocked")


def test_syntax_error_reported():
    """Syntax errors are surfaced as Syntax Error messages."""
    code = "def foo(:\n    pass"
    err = validate_ast(code)
    assert err is not None
    assert "Syntax Error" in err
    print("✓ Syntax errors reported cleanly")


if __name__ == "__main__":
    print("Running validate_ast tests...\n")
    test_import_allowlist_blocks_dangerous()
    test_import_allowlist_permits_safe()
    test_code_size_limit()
    test_pattern_matching_blocked()
    test_normal_code_allowed()
    test_naive_getattr_blocked()
    test_syntax_error_reported()
    print("\nAll tests passed!")
