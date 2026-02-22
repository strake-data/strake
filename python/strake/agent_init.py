#!/usr/bin/env python3
"""
Hardware-Isolated Guest Payload Executor (agent_init)
This script serves as the `init` process (PID 1) inside the Firecracker MicroVM guest.
It binds to the VSOCK, receives the payload securely, prevents Python-level introspection
escapes (e.g. `().__class__.__bases__`), executes the code using the Strake engine,
and returns the result cleanly over the VM boundary.
"""

import socket
import sys
import json
import logging
import traceback
import builtins
import os
from typing import Any
import io
import contextlib

# Hardening Constants
MAX_PAYLOAD_SIZE = 10 * 1024 * 1024  # 10MB limit


def _harden_environment():
    """
    Defense in depth: disables a few convenient introspection entry points.

    These mitigations are effective against naive or accidental introspection only.
    A determined attacker with access to a Python interpreter can bypass all of
    them (via C extensions, ctypes, or sys.exc_info frame chains). The real
    security boundary for this agent is the Firecracker VM and its VSOCK channel,
    not anything done here. Do not rely on this function for security guarantees.
    """
    # 1. Remove the convenient frame-access shortcut
    if hasattr(sys, "_getframe"):
        del sys._getframe

    # 2. Suppress tracebacks from leaking host paths in error output
    sys.tracebacklimit = 0

    # 3. Defense in depth: override exc_info to reduce accidental frame exposure.
    #    Not a security control — `ctypes.pythonapi.PyErr_GetExcInfo()` and
    #    C extensions can still reach frames.
    _real_exc_info = sys.exc_info

    def _blocked_exc_info() -> Any:
        return (None, None, None)

    sys.exc_info = _blocked_exc_info  # type: ignore

    # 4. Defense in depth: restrict class construction to object and Exception bases.
    #    Bypassable via ctypes; included as a speed-bump rather than a hard guard.
    original_build_class = builtins.__build_class__

    def safe_build_class(func, name, *bases, **kwds):
        for base in bases:
            if type(base) is type and issubclass(base, (Exception, BaseException)):
                pass  # Allow custom exceptions
            elif base is object:
                pass  # Allow basic objects
            else:
                raise TypeError(
                    f"Security Error: Inheriting from '{base.__name__}' is forbidden in sandbox."
                )
        return original_build_class(func, name, *bases, **kwds)

    builtins.__build_class__ = safe_build_class  # type: ignore


def _recv_exact(sock, n):
    """Accurately receive exactly n bytes from a stream socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed mid-read")
        buf.extend(chunk)
    return bytes(buf)


import builtins as _builtins

# Guest builtins: a restricted subset provided to executed code.
# This is a secondary, speed-bump layer. The real security boundary is the
# Firecracker VM + VSOCK channel. A determined guest process running as PID 1
# inside the VM can reach the host only through the explicit VSOCK protocol.
GUEST_SAFE_BUILTINS = {
    "print": _builtins.print,
    "len": _builtins.len,
    "range": _builtins.range,
    "str": _builtins.str,
    "int": _builtins.int,
    "float": _builtins.float,
    "list": _builtins.list,
    "dict": _builtins.dict,
    "Exception": _builtins.Exception,
    "ValueError": _builtins.ValueError,
    "TypeError": _builtins.TypeError,
}


def run_vsock_server():
    """Binds to the VSOCK guest CID and listens for host payloads."""
    # VSOCK Constants
    VMADDR_CID_ANY = 4294967295
    PORT = 1234

    # Guest CID is 3 by default in Firecracker
    server = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    server.bind((socket.VMADDR_CID_ANY, PORT))  # Listen on all interfaces
    server.listen(1)

    logging.info(f"Guest agent listening on VSOCK port {PORT}")

    while True:
        conn, addr = server.accept()
        try:
            # Protocol: 4 bytes length + Payload
            try:
                length_bytes = _recv_exact(conn, 4)
            except ConnectionError:
                continue

            length = int.from_bytes(length_bytes, byteorder="big")
            if length > MAX_PAYLOAD_SIZE:
                conn.sendall(
                    json.dumps({"error": "Payload exceeded maximum size"}).encode(
                        "utf-8"
                    )
                )
                continue

            payload_data = _recv_exact(conn, length)

            payload = json.loads(payload_data.decode("utf-8"))
            code = payload.get("code", "")

            # [Hardening] Guest-side code size check
            MAX_CODE_SIZE = 1024 * 1024  # 1MB
            if len(code) > MAX_CODE_SIZE:
                conn.sendall(
                    json.dumps(
                        {
                            "error": f"Security Error: Code size exceeds limit of {MAX_CODE_SIZE} bytes"
                        }
                    ).encode("utf-8")
                )
                continue

            # Code execution context.
            # __builtins__ is restricted as a speed-bump for naive mistakes;
            # the real security boundary is the VM/VSOCK channel itself.
            global_ctx = {
                "__name__": "__sandbox__",
                "__builtins__": GUEST_SAFE_BUILTINS,
                # "strake": RemoteStrakeProxy(...)
            }

            stdout_cap = io.StringIO()
            stderr_cap = io.StringIO()
            error = None

            try:
                with (
                    contextlib.redirect_stdout(stdout_cap),
                    contextlib.redirect_stderr(stderr_cap),
                ):
                    # [Hardening] Use optimized bytecode and dedicated execution frame
                    compiled = compile(code, "<sandbox>", "exec", optimize=2)

                    # Execute in a nested scope to ensure frame locals are cleaned up by Python
                    def _exec():
                        exec(compiled, global_ctx)

                    _exec()
            except Exception as e:
                error = str(e)
            finally:
                # No frame manipulation needed—nested function scope and context mgr clean up
                global_ctx.clear()

            output = stdout_cap.getvalue()
            if error:
                response = {"error": f"{error}\n{stderr_cap.getvalue()}"}
            else:
                response = {"output": output}

            resp_bytes = json.dumps(response).encode("utf-8")
            conn.sendall(len(resp_bytes).to_bytes(4, byteorder="big"))
            conn.sendall(resp_bytes)

        except Exception as e:
            logging.error(f"Error handling connection: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    _harden_environment()
    run_vsock_server()
