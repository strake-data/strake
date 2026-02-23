import sys
import io
import contextlib
import logging
import json
import ast
import types
import time
import threading
from typing import Any, Dict, List, Optional
import pyarrow as pa
from .base import SandboxResult

logger = logging.getLogger("strake.sandbox.core")

# Constants for security hardening
MAX_OUTPUT_SIZE = 10 * 1024 * 1024  # 10MB limit
MAX_CODE_SIZE = 1024 * 1024  # 1MB limit

# Allowed safe imports for data science and basic operations.
# This allowlist is low-maintenance (only changes when a new safe module is added)
# and is enforced at both the AST level (below) and at runtime via _safe_import_shim.
# It is a genuine fast-path guardrail: stopping `import subprocess` before a process
# is even spawned. It does NOT replace OS-level isolation.
ALLOWED_IMPORTS = frozenset(
    {
        # Data science
        "pandas",
        "numpy",
        "pyarrow",
        "sklearn",
        "scipy",
        "statsmodels",
        "matplotlib",
        # Safe stdlib
        "json",
        "math",
        "statistics",
        "datetime",
        "collections",
        "itertools",
        "functools",
        "decimal",
        "fractions",
        "re",
        "string",
        # Type system — no capabilities
        "typing",
        "typing_extensions",
        "abc",
        "dataclasses",
        "enum",
        "__future__",
        # Strake itself — always injected into the context as `strake`, but allow
        # explicit `import strake` too for consistency.
        "strake",
    }
)

# CPython sets __builtins__ to the builtins *module* in non-__main__ modules
# and to a *dict* in __main__. We normalize to the callable here.
if hasattr(__builtins__, "__import__"):
    _REAL_IMPORT = __builtins__.__import__  # type: ignore[union-attr]
else:
    _REAL_IMPORT = __builtins__["__import__"]  # type: ignore[index]


def _safe_import_shim(name, globals=None, locals=None, fromlist=(), level=0):
    """
    Runtime __import__ shim that enforces the ALLOWED_IMPORTS allowlist.

    This is the runtime counterpart to the AST import check in validate_ast.
    Together they provide a layered fast-path rejection with an audit log entry,
    but neither is a substitute for OS-level process isolation.
    """
    top = name.split(".")[0]
    if top not in ALLOWED_IMPORTS:
        raise ImportError(f"Import of '{name}' is not permitted in the sandbox.")
    return _REAL_IMPORT(name, globals, locals, fromlist, level)


class LimitedStringIO(io.StringIO):
    """StringIO subclass that enforces a maximum size to prevent memory exhaustion."""

    def __init__(self, max_size: int):
        super().__init__()
        self.max_size = max_size
        self._size = 0

    def write(self, s: str) -> int:
        if self._size + len(s) > self.max_size:
            raise MemoryError(f"Output exceeded maximum size of {self.max_size} bytes")
        self._size += len(s)
        return super().write(s)


def validate_ast(code: str) -> Optional[str]:
    """
    Fast-path import allowlist and code-size check.

    This function validates two things:
      1. Code does not exceed the size limit.
      2. All import statements reference modules in ALLOWED_IMPORTS.

    Security scope: This is a fast-path guardrail providing an audit log entry
    and early rejection *before* spawning a subprocess. It does NOT prevent a
    determined attacker from executing arbitrary code — Python cannot be sandboxed
    from within. The real security boundary is OS-level isolation (seccomp, Landlock,
    Firecracker VM). Do not add name/attribute deny-lists here; they are trivially
    bypassed and create high maintenance burden for negligible benefit.
    """
    if len(code) > MAX_CODE_SIZE:
        return f"Security Error: Code size exceeds limit of {MAX_CODE_SIZE} bytes"

    try:
        # Block pattern matching as a niche, low-maintenance additional check.
        _MATCH_TYPES = (
            (ast.Match, ast.MatchAs, ast.MatchStar)
            if sys.version_info >= (3, 10)
            else ()
        )

        tree = ast.parse(code)
        for node in ast.walk(tree):
            if _MATCH_TYPES and isinstance(node, _MATCH_TYPES):
                return "Security Error: Pattern matching syntax not allowed"

            # Check imports against the allowlist
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.Import):
                    names = [alias.name.split(".")[0] for alias in node.names]
                else:  # ImportFrom
                    names = [node.module.split(".")[0]] if node.module else []

                for name in names:
                    if name not in ALLOWED_IMPORTS:
                        return f"Security Error: Import of '{name}' is not permitted in the sandbox."

            # Check for direct getattr calls targeting dunder/dangerous attributes.
            # Catches the most naive introspection chains; does not block determined attackers.
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "getattr":
                    if len(node.args) >= 2:
                        arg = node.args[1]
                        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                            if arg.value.startswith("__") or arg.value in {
                                "mro",
                                "subclasses",
                                "bases",
                                "globals",
                                "locals",
                            }:
                                return f"Security Error: Dangerous getattr target '{arg.value}'."

    except SyntaxError as e:
        return f"Syntax Error: {e.msg} (line {e.lineno})"
    except Exception as e:
        return f"Validation Error: {str(e)}"

    return None


class SafeBatchProxy:
    __slots__ = ("_batch",)

    def __init__(self, batch: pa.RecordBatch):
        object.__setattr__(self, "_batch", batch)

    def __getattr__(self, name):
        raise AttributeError(
            f"Security Error: SafeBatchProxy has no attribute '{name}'"
        )

    def to_pandas(self):
        """WARNING: Materializes batch to pandas DataFrame (copy)."""
        return object.__getattribute__(self, "_batch").to_pandas()

    def to_arrow(self):
        """Zero-copy reference to this batch as RecordBatch."""
        return object.__getattribute__(self, "_batch")

    def to_pylist(self):
        return object.__getattribute__(self, "_batch").to_pylist()

    @property
    def num_rows(self):
        return object.__getattribute__(self, "_batch").num_rows


class SafeTableProxy:
    __slots__ = ("_table",)

    def __init__(self, table: pa.Table):
        object.__setattr__(self, "_table", table)

    def __del__(self):
        # Ensure underlying pyarrow table is cleared to hint for immediate GC
        try:
            object.__setattr__(self, "_table", None)
        except Exception:
            pass

    def __getattr__(self, name):
        raise AttributeError(
            f"Security Error: SafeTableProxy has no attribute '{name}'"
        )

    def __setattr__(self, name, value):
        raise AttributeError("Security Error: Cannot set attributes on table proxy")

    def __repr__(self):
        cols = object.__getattribute__(self, "_table").schema.names
        return f"Table({self.num_rows} rows × {len(cols)} cols: {', '.join(cols)})"

    def to_pandas(self):
        """Return a mutable pandas DataFrame of the table."""
        # pandas is a safe, allowed import within the sandbox environment
        import pandas as pd

        return object.__getattribute__(self, "_table").to_pandas()

    def to_arrow(self):
        """Return the immutable pyarrow Table."""
        return object.__getattribute__(self, "_table")

    def iter_batches(self, batch_size: int = 1024):
        """Yield SafeBatchProxy objects for memory-efficient iteration.

        Prefer this over ``column_values()`` for large result sets.
        """
        table = object.__getattribute__(self, "_table")
        for i in range(0, table.num_rows, batch_size):
            yield SafeBatchProxy(table.slice(i, batch_size))

    def to_pylist(self):
        return object.__getattribute__(self, "_table").to_pylist()

    @property
    def num_rows(self):
        return object.__getattribute__(self, "_table").num_rows

    def __len__(self):
        return object.__getattribute__(self, "_table").num_rows

    def column_values(self, name):
        """Return a column as a Python list.

        WARNING: Materializes the entire column from Arrow memory into a Python
        list (one Python object per value). For large tables, use ``iter_batches()``
        instead to keep data in Arrow format until consumed row-by-row.
        """
        col = object.__getattribute__(self, "_table").column(name)
        return col.to_pylist()


class StrakeShim:
    """
    The 'strake' object exposed to the sandbox.
    """

    __slots__ = ("_conn", "_sandbox_ref")

    def __init__(self, connection: Any, sandbox: Any):
        object.__setattr__(self, "_conn", connection)
        object.__setattr__(self, "_sandbox_ref", sandbox)

    def __getattr__(self, name):
        raise AttributeError(f"Security Error: 'strake' has no attribute '{name}'")

    def __setattr__(self, name, value):
        raise AttributeError("Security Error: Cannot set attributes on strake shim")

    def get_indexer(self) -> Any:
        """Get the schema indexer, initializing it lazily and thread-safely."""
        sandbox = object.__getattribute__(self, "_sandbox_ref")
        conn = object.__getattribute__(self, "_conn")
        
        if not hasattr(sandbox, "_indexer_thread_lock"):
            object.__setattr__(sandbox, "_indexer_thread_lock", threading.Lock())
            
        if not hasattr(sandbox, "_indexer") or sandbox._indexer is None:
            with sandbox._indexer_thread_lock:
                if not hasattr(sandbox, "_indexer") or sandbox._indexer is None:
                    # Note: Need explicit access to strake root search module
                    from strake.search import SchemaIndexer

                    object.__setattr__(sandbox, "_indexer", SchemaIndexer(conn))
        return sandbox._indexer

    def sql(self, query: str):
        """
        Execute SQL and return a SafeTableProxy natively for zero-copy memory access.
        """
        conn = object.__getattribute__(self, "_conn")
        logger.info(f"Sandbox SQL: {query}")
        start = time.monotonic_ns()
        status = "ok"
        result_rows = 0
        try:
            table = conn.sql(query)
            result_rows = table.num_rows
            return SafeTableProxy(table)
        except Exception as exc:
            status = f"error:{type(exc).__name__}"
            raise
        finally:
            elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
            try:
                from strake.tracing import get_emitter
                get_emitter().emit({
                    "event": "span",
                    "span_type": "sandbox_sql",
                    "name": "sql",
                    "query": query,
                    "result_rows": result_rows,
                    "latency_ms": round(elapsed_ms, 2),
                    "status": status,
                })
            except Exception:
                pass  # tracing must never break execution

    def sql_json(self, query: str, max_rows: int = 1000) -> str:
        """
        Execute SQL and return results as a JSON string.

        Truncates to max_rows to protect Agent context memory.

        NOTE: Serialization is row-by-row and GIL-bound. This method is suitable
        for small result sets (a few thousand rows). For large datasets, use
        ``sql()`` and access via ``iter_batches()`` instead.
        """
        conn = object.__getattribute__(self, "_conn")
        table = conn.sql(query)
        if table.num_rows > max_rows:
            logger.warning(
                f"SQL result truncated from {table.num_rows} to {max_rows} rows."
            )
            table = table.slice(0, max_rows)

        output = io.StringIO()
        output.write("[")
        first = True
        for batch in table.to_batches():
            for row in batch.to_pylist():
                if not first:
                    output.write(",")
                else:
                    first = False
                json.dump(row, output)
        output.write("]")
        return output.getvalue()

    def search(self, query: str) -> List[Dict[str, Any]]:
        """
        Semantic search for tables using LanceDB vector index.
        Runs inside asyncio.to_thread, so it must be thread-safe for indexer init.
        """
        logger.info(f"Sandbox Search: {query}")
        start = time.monotonic_ns()
        status = "ok"
        result_count = 0
        try:
            indexer = object.__getattribute__(self, "get_indexer")()
            results = indexer.search_tables(query)
            result_count = len(results)
            return results
        except Exception as exc:
            status = f"error:{type(exc).__name__}"
            raise
        finally:
            elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
            try:
                from strake.tracing import get_emitter
                get_emitter().emit({
                    "event": "span",
                    "span_type": "sandbox_search",
                    "name": "search",
                    "query": query,
                    "result_count": result_count,
                    "latency_ms": round(elapsed_ms, 2),
                    "status": status,
                })
            except Exception:
                pass  # tracing must never break execution


# TEST_INTERNAL: Exposed strictly for synchronous testing limits inside `test_mcp.py`
def _sandbox_worker_inner(code: str, queue: Any, connection: Any) -> None:
    """The function executed inside the sandboxed process."""

    # Capture stdout
    stdout_cap = LimitedStringIO(MAX_OUTPUT_SIZE)
    stderr_cap = io.StringIO()

    exec_start = time.monotonic_ns()
    exec_status = "ok"
    error_type = None

    try:

        class WorkerSandbox:
            def __init__(self):
                self._indexer = None
                self._indexer_thread_lock = threading.Lock()

        strake_shim = StrakeShim(connection, WorkerSandbox())

        # The sandbox worker uses the full __builtins__. The security boundary is
        # OS-level isolation (seccomp, Landlock, Firecracker), not Python-level
        # restrictions (which cannot be reliably enforced from within CPython).
        # The import allowlist is still enforced at runtime via _safe_import_shim
        # injected as __import__.
        _builtins_dict = (
            __builtins__
            if isinstance(__builtins__, dict)
            else vars(__builtins__)  # type: ignore[arg-type]
        )
        global_context: Dict[str, Any] = {
            "__builtins__": {**_builtins_dict, "__import__": _safe_import_shim},
            "__strake_shim__": strake_shim,
            "__name__": "__sandbox__",
        }

        try:
            with (
                contextlib.redirect_stdout(stdout_cap),
                contextlib.redirect_stderr(stderr_cap),
            ):
                # [Hardening] Use optimized bytecode and dedicated execution frame
                global_context["strake"] = strake_shim
                compiled = compile(code, "<sandbox>", "exec", optimize=2)

                # Execute in dedicated function to ensure frame cleanup
                def _exec():
                    exec(compiled, global_context)

                _exec()
        finally:
            # [Hardening] Clear context references to prevent retention/introspection
            global_context.clear()

        queue.put(
            SandboxResult(
                stdout=stdout_cap.getvalue(), stderr=stderr_cap.getvalue(), result=None
            )
        )

    except Exception as e:
        exec_status = "error"
        error_type = type(e).__name__
        logger.debug("Sandbox exception", exc_info=True)
        import traceback

        err_msg = ""
        if isinstance(e, MemoryError):
            err_msg = f"Resource Error: {str(e)}"
        elif isinstance(e, SyntaxError):
            err_msg = f"Syntax Error: {str(e)}"
        else:
            err_msg = f"Runtime Error: {type(e).__name__}: {str(e)}"

        queue.put(
            SandboxResult(stdout=stdout_cap.getvalue(), stderr=err_msg, result=None)
        )
    finally:
        # Emit sandbox_exec span — best effort, must not break execution
        elapsed_ms = (time.monotonic_ns() - exec_start) / 1_000_000
        try:
            from strake.tracing import get_emitter
            from strake.tracing.session import code_field
            get_emitter().emit({
                "event": "span",
                "span_type": "sandbox_exec",
                "name": "_sandbox_worker_inner",
                **code_field(code),
                "latency_ms": round(elapsed_ms, 2),
                "status": exec_status,
                "error_type": error_type,
            })
        except Exception:
            pass
