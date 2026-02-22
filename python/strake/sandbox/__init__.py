"""
Strake Sandbox Module
Provides safe remote and local execution environments for generated Python.
"""

from .base import SandboxManager, SandboxResult
from .firecracker import FirecrackerSandboxManager
from .native import (
    NativeOSSandboxManager,
    LinuxSandboxManager,
    MacOSSandboxManager,
    WindowsSandboxManager,
)

__all__ = [
    "SandboxManager",
    "FirecrackerSandboxManager",
    "NativeOSSandboxManager",
    "LinuxSandboxManager",
    "MacOSSandboxManager",
    "WindowsSandboxManager",
]
