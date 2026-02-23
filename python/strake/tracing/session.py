"""
Core tracing primitives: sessions, spans, and pluggable emitters.

Design goals:
  - Zero mandatory dependencies beyond stdlib + typing.
  - JSON-lines file output by default (``~/.strake/traces/``).
  - All public entry-points are safe to call even when tracing is disabled
    (via ``NullEmitter``).
  - The ``@span`` decorator works on both sync and async functions.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import json
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

logger = logging.getLogger("strake.tracing")

F = TypeVar("F", bound=Callable[..., Any])

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------

_TRACE_ENABLED_DEFAULT = "true"


def _env_bool(key: str, default: str) -> bool:
    return os.environ.get(key, default).lower() in ("1", "true", "yes")


def _trace_enabled() -> bool:
    return _env_bool("STRAKE_TRACE_ENABLED", _TRACE_ENABLED_DEFAULT)


def _trace_code_capture() -> bool:
    """When true, full script text is stored instead of just a SHA-256 hash."""
    return _env_bool("STRAKE_TRACE_CODE", "false")


def _trace_dir() -> Path:
    return Path(os.environ.get("STRAKE_TRACE_DIR", os.path.expanduser("~/.strake/traces")))


# ---------------------------------------------------------------------------
# Emitters
# ---------------------------------------------------------------------------


class TraceEmitter(ABC):
    """Pluggable sink for trace records."""

    @abstractmethod
    def emit(self, record: Dict[str, Any]) -> None:
        """Persist a single trace record."""

    def flush(self) -> None:
        """Optional flush — called at session close."""


class NullEmitter(TraceEmitter):
    """Drop-all emitter used when tracing is disabled."""

    def emit(self, record: Dict[str, Any]) -> None:
        pass


class JsonLinesFileEmitter(TraceEmitter):
    """Append trace records as JSON-lines to a file in ``~/.strake/traces/``.

    File naming: ``<date>_<session_id>.jsonl`` — one file per session for
    easy correlation and cleanup.
    """

    def __init__(self, trace_dir: Optional[Path] = None):
        self._dir = trace_dir or _trace_dir()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = None
        self._path: Optional[Path] = None

    def _ensure_file(self, session_id: str) -> None:
        if self._file is None:
            today = datetime.now(timezone.utc).strftime("%Y%m%d")
            self._path = self._dir / f"{today}_{session_id}.jsonl"
            self._file = open(self._path, "a", encoding="utf-8")

    def emit(self, record: Dict[str, Any]) -> None:
        session_id = record.get("session_id", "unknown")
        self._ensure_file(session_id)
        assert self._file is not None
        self._file.write(json.dumps(record, default=str) + "\n")

    def flush(self) -> None:
        if self._file is not None:
            self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

# Module-level singleton — created lazily.
_emitter: Optional[TraceEmitter] = None


def get_emitter() -> TraceEmitter:
    """Return the module-level emitter, creating it on first call."""
    global _emitter
    if _emitter is None:
        if _trace_enabled():
            _emitter = JsonLinesFileEmitter()
            logger.info("Tracing enabled — writing to %s", _trace_dir())
        else:
            _emitter = NullEmitter()
            logger.debug("Tracing disabled (STRAKE_TRACE_ENABLED != true)")
    return _emitter


def set_emitter(emitter: TraceEmitter) -> None:
    """Override the module-level emitter (useful for testing)."""
    global _emitter
    _emitter = emitter


# ---------------------------------------------------------------------------
# AgentSession
# ---------------------------------------------------------------------------


class AgentSession:
    """Context manager representing one agent invocation.

    Usage::

        emitter = get_emitter()
        with AgentSession(emitter, metadata={"tool": "run_python"}) as sess:
            # ... do work ...
            sess.record_event("custom_event", {"key": "value"})

    On exit, a ``session_end`` record is emitted with timing and termination
    reason.
    """

    def __init__(
        self,
        emitter: Optional[TraceEmitter] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.session_id: str = uuid.uuid4().hex
        self.emitter: TraceEmitter = emitter or get_emitter()
        self.metadata: Dict[str, Any] = metadata or {}
        self._start_ns: int = 0
        self._terminated = False
        self.termination_reason: str = "natural"

    # -- context manager --

    def __enter__(self) -> "AgentSession":
        self._start_ns = time.monotonic_ns()
        self.emitter.emit(
            {
                "event": "session_start",
                "session_id": self.session_id,
                "timestamp": _utc_iso(),
                **self.metadata,
            }
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[override]
        if exc_type is not None:
            self.termination_reason = f"error:{exc_type.__name__}"
        elapsed_ms = (time.monotonic_ns() - self._start_ns) / 1_000_000
        self.emitter.emit(
            {
                "event": "session_end",
                "session_id": self.session_id,
                "timestamp": _utc_iso(),
                "termination_reason": self.termination_reason,
                "duration_ms": round(elapsed_ms, 2),
            }
        )
        self.emitter.flush()
        self._terminated = True

    # -- public helpers --

    def record_event(self, event_type: str, data: Optional[Dict[str, Any]] = None) -> None:
        """Emit an arbitrary event record under this session."""
        self.emitter.emit(
            {
                "event": event_type,
                "session_id": self.session_id,
                "timestamp": _utc_iso(),
                **(data or {}),
            }
        )


# ---------------------------------------------------------------------------
# @span decorator
# ---------------------------------------------------------------------------


def span(
    span_type: str = "tool_call",
    name: Optional[str] = None,
    capture_args: bool = False,
) -> Callable[[F], F]:
    """Decorator that emits a structured span record around a function.

    Parameters
    ----------
    span_type:
        Category string written to the record (e.g. ``"tool_call"``,
        ``"sandbox_exec"``).
    name:
        Human-readable label; defaults to the function name.
    capture_args:
        If *True*, include a JSON-serializable summary of positional args in
        the span record.  Defaults to *False* for privacy.
    """

    def decorator(fn: F) -> F:
        label = name or fn.__name__

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                emitter = get_emitter()
                start = time.monotonic_ns()
                status = "ok"
                error_detail = None
                result = None
                try:
                    result = await fn(*args, **kwargs)
                    return result
                except Exception as exc:
                    status = "error"
                    error_detail = f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
                    record: Dict[str, Any] = {
                        "event": "span",
                        "span_type": span_type,
                        "name": label,
                        "timestamp": _utc_iso(),
                        "latency_ms": round(elapsed_ms, 2),
                        "status": status,
                    }
                    if error_detail:
                        record["error"] = error_detail
                    if capture_args:
                        record["args"] = _summarise_args(args, kwargs)
                    _add_result_meta(record, result)
                    emitter.emit(record)

            return async_wrapper  # type: ignore[return-value]

        else:

            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                emitter = get_emitter()
                start = time.monotonic_ns()
                status = "ok"
                error_detail = None
                result = None
                try:
                    result = fn(*args, **kwargs)
                    return result
                except Exception as exc:
                    status = "error"
                    error_detail = f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
                    record: Dict[str, Any] = {
                        "event": "span",
                        "span_type": span_type,
                        "name": label,
                        "timestamp": _utc_iso(),
                        "latency_ms": round(elapsed_ms, 2),
                        "status": status,
                    }
                    if error_detail:
                        record["error"] = error_detail
                    if capture_args:
                        record["args"] = _summarise_args(args, kwargs)
                    _add_result_meta(record, result)
                    emitter.emit(record)

            return sync_wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------
# Code hashing helper (used by MCP instrumentation)
# ---------------------------------------------------------------------------


def hash_code(code: str) -> str:
    """Return a SHA-256 hex digest of *code*."""
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def code_field(code: str) -> Dict[str, Any]:
    """Return the appropriate code representation based on config.

    When ``STRAKE_TRACE_CODE=true`` the full text is included; otherwise only
    a SHA-256 hash and byte-length are recorded.
    """
    if _trace_code_capture():
        return {"code": code, "code_size_bytes": len(code.encode("utf-8"))}
    return {"code_hash": hash_code(code), "code_size_bytes": len(code.encode("utf-8"))}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summarise_args(args: tuple, kwargs: dict) -> Dict[str, Any]:
    """Best-effort JSON-safe summary of call arguments."""
    summary: Dict[str, Any] = {}
    if args:
        safe = []
        for a in args:
            try:
                json.dumps(a)
                safe.append(a)
            except (TypeError, ValueError):
                safe.append(repr(a)[:200])
        summary["positional"] = safe
    if kwargs:
        safe_kw = {}
        for k, v in kwargs.items():
            try:
                json.dumps(v)
                safe_kw[k] = v
            except (TypeError, ValueError):
                safe_kw[k] = repr(v)[:200]
        summary["keyword"] = safe_kw
    return summary


def _add_result_meta(record: Dict[str, Any], result: Any) -> None:
    """Add result metadata (size, type) to a span record without capturing content."""
    if result is None:
        return
    if isinstance(result, (str, bytes)):
        record["result_size_bytes"] = len(result) if isinstance(result, bytes) else len(result.encode("utf-8"))
    elif isinstance(result, (list, dict)):
        try:
            record["result_size_bytes"] = len(json.dumps(result))
        except (TypeError, ValueError):
            pass
    record["result_type"] = type(result).__name__
