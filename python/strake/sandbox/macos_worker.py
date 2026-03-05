import sys
import os
import resource
import json
import logging
from typing import Optional

from strake.sandbox.core import _sandbox_worker_inner, validate_ast
from strake.sandbox.base import SandboxResult, SandboxErrorMessages
from strake.sandbox.native import SandboxConfig
from strake import StrakeConnection

# Configure secure logging to stderr (which is captured by the parent)
# Avoid world-writable /tmp files to prevent symlink attacks.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger("strake.sandbox.worker")

class QueueAdapter:
    """Mock queue that writes JSON results to stdout for subprocess communication."""

    def put(self, result: dict):
        # result is already a dict from _sandbox_worker_inner
        config = SandboxConfig()
        try:
            sys.stdout.write(config.marker_start + json.dumps(result) + config.marker_end + "\n")
            sys.stdout.flush()
        except Exception as e:
            logger.error(f"Worker IPC Error: {e}")

def main():
    config = SandboxConfig()

    def emit_error(msg: str):
        """Helper to emit a structured SandboxResult error and exit."""
        sys.stdout.write(
            config.marker_start
            + json.dumps({"stdout": "", "stderr": msg, "result": None})
            + config.marker_end
            + "\n"
        )
        sys.stdout.flush()
        sys.exit(1)

    try:
        # 1. Bounded stdin read to prevent memory exhaustion
        # The payload contains both token and code as JSON.
        MAX_PAYLOAD_SIZE = config.max_code_size + 4096
        payload_raw = sys.stdin.read(MAX_PAYLOAD_SIZE + 1)
        if len(payload_raw) > MAX_PAYLOAD_SIZE:
            logger.error(SandboxErrorMessages.CODE_SIZE_EXCEEDED)
            emit_error(SandboxErrorMessages.CODE_SIZE_EXCEEDED)

        try:
            payload = json.loads(payload_raw)
            token = payload.get("token")
            code = payload.get("code", "")
        except json.JSONDecodeError:
            logger.error("Malformed IPC payload from parent")
            emit_error(SandboxErrorMessages.INTERNAL_FAILURE)

        # 2. AST Validation (Defense in Depth)
        if error := validate_ast(code):
            logger.error(f"{error}")
            emit_error(error)

        # 3. Apply Resource Limits (rlimit)
        timeout_env = os.environ.get("SANDBOX_TIMEOUT_SECS", "30")
        try:
            timeout = float(timeout_env)
        except ValueError:
            timeout = 30.0
            
        # RLIMIT_CPU is seconds of CPU time.
        # Hard == soft: sandboxed process cannot raise its own CPU limit.
        cpu_hard = int(timeout) + 1
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_hard, cpu_hard))
        
        # RLIMIT_AS (Address Space)
        # Hard limit == soft limit intentionally: prevents the sandboxed process
        # from raising its own memory ceiling.
        memory_limit_env = os.environ.get("SANDBOX_MEMORY_LIMIT", str(512 * 1024 * 1024))
        try:
            memory_limit = int(memory_limit_env)
        except ValueError:
            memory_limit = 512 * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

        # 4. Connection Setup
        try:
            if os.environ.get("STRAKE_CONFIG"):
                conn = StrakeConnection(os.environ["STRAKE_CONFIG"])
            else:
                url = os.environ.get("STRAKE_URL", "grpc://127.0.0.1:50051")
                conn = StrakeConnection(url, api_key=token)
        except Exception as e:
            logger.error(f"Sandbox could not connect to data engine: {e}")
            emit_error(SandboxErrorMessages.CONNECTION_FAILED)
        finally:
            token = None  # Guarantee second-layer cleanup

        # 5. Execute the worker logic
        # conn is guaranteed to be a valid StrakeConnection if we reached here
        _sandbox_worker_inner(code, QueueAdapter(), conn)

    except Exception as e:
        # Sanitize output to avoid leaking internal tracebacks to the AI
        logger.debug(f"Unhandled worker exception", exc_info=True)
        emit_error(SandboxErrorMessages.INTERNAL_FAILURE)

if __name__ == "__main__":
    main()
