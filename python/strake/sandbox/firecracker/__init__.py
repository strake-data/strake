"""Firecracker-based sandbox manager package.

Provides safe microVM-based code execution, telemetry, lifecycle management, and manual
HTTP UDS client transports.
"""

# Standard Library
import asyncio
import logging
import os
import tempfile

# Local Application Imports
from strake.sandbox.firecracker.manager import (
    FirecrackerSandboxManager,
    METERING_INTERVAL_ENV,
)
from strake.sandbox.firecracker.telemetry import SandboxMetering
from strake.tracing import get_emitter

logger = logging.getLogger("strake.sandbox.firecracker")

__all__ = [
    "FirecrackerSandboxManager",
    "SandboxMetering",
    "METERING_INTERVAL_ENV",
    "get_emitter",
    "asyncio",
    "os",
    "tempfile",
]
