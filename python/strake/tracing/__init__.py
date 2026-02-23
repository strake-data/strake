"""
Strake Agent Observability — structured tracing for sandbox interactions.

All tracing is additive (no existing behaviour changes) and gated behind
``STRAKE_TRACE_ENABLED`` (default: ``true``).

Quick-start::

    from strake.tracing import get_emitter, AgentSession

    emitter = get_emitter()
    with AgentSession(emitter, metadata={"user": "demo"}) as sess:
        ...  # traced work
"""

from .session import (
    AgentSession,
    TraceEmitter,
    JsonLinesFileEmitter,
    NullEmitter,
    get_emitter,
    span,
)

__all__ = [
    "AgentSession",
    "TraceEmitter",
    "JsonLinesFileEmitter",
    "NullEmitter",
    "get_emitter",
    "span",
]
