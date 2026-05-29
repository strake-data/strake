# Standard Library
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

# Local Application Imports
from strake.sandbox.firecracker.client import UnixSocketHttpClient

logger = logging.getLogger("strake.sandbox.firecracker.telemetry")


@dataclass
class SandboxMetering:
    """Dataclass holding resource metering statistics for a sandbox execution session.

    Attributes:
        session_id: Unique string identifying this sandbox execution session.
        start_cpu: Cumulative guest CPU time (user + system) at session baseline in seconds.
        start_wall: Monotonic start timestamp of the execution session.
        max_memory_mb: Peak guest memory consumed during execution in MiB.
        sampling_task: Reference to the background memory polling task.
    """

    session_id: str
    start_cpu: float = 0.0
    start_wall: float = 0.0
    max_memory_mb: float = 0.0
    sampling_task: Optional[asyncio.Task[None]] = None


class FirecrackerTelemetryCollector:
    """Manages UDS metrics collection, Prometheus parsing, and background memory polling."""

    def __init__(self, metrics_path: str, api_timeout: float = 5.0) -> None:
        """Initialize the telemetry collector.

        Args:
            metrics_path: Path to the metrics Unix socket.
            api_timeout: Socket request timeout.
        """
        self.client = UnixSocketHttpClient(metrics_path)
        self.api_timeout = api_timeout

    @staticmethod
    def parse_metrics(text: str) -> dict[str, float]:
        """Parse Prometheus or JSON metrics output from Firecracker.

        Supports standard Prometheus exposition and legacy JSON formats.

        Args:
            text: Raw HTTP response payload string.

        Returns:
            Dictionary containing 'cpu_seconds' and 'memory_mb'.
        """
        if not isinstance(text, str) or not text:
            return {"cpu_seconds": 0.0, "memory_mb": 0.0}

        # 1. JSON Format fallback (Firecracker v0.23 to v0.25 version support)
        try:
            import json

            data = json.loads(text)
            vcpu = data.get("vcpu", {})
            cpu_seconds = float(vcpu.get("utime_seconds", 0.0)) + float(
                vcpu.get("stime_seconds", 0.0)
            )

            if cpu_seconds == 0.0:
                cpu_seconds = float(data.get("cpu_seconds", 0.0))

            balloon = data.get("balloon", {})
            memory_mb = float(balloon.get("memory_used_mb", 0.0))
            if memory_mb == 0.0:
                memory_mb = float(data.get("memory_used_mb", 0.0))

            return {"cpu_seconds": cpu_seconds, "memory_mb": memory_mb}
        except json.JSONDecodeError:
            pass

        # 2. Prometheus exposition format (production standard)
        cpu_sum = 0.0
        memory_bytes = 0.0
        has_promo = False

        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.rsplit(None, 1)
            if len(parts) < 2:
                continue

            metric_name, value_str = parts[0], parts[1]
            try:
                val = float(value_str)
            except ValueError:
                continue

            if metric_name.startswith("firecracker_cpu_usage_seconds_total"):
                cpu_sum += val
                has_promo = True
            elif metric_name.startswith("firecracker_memory_used_bytes"):
                memory_bytes = val
                has_promo = True

        if has_promo:
            return {
                "cpu_seconds": cpu_sum,
                "memory_mb": memory_bytes / (1024 * 1024),
            }

        return {"cpu_seconds": 0.0, "memory_mb": 0.0}

    async def fetch_cpu_seconds(self) -> float:
        """Fetch total CPU time from the metrics socket.

        Returns:
            Total guest CPU seconds. Returns 0.0 on error.

        Raises:
            ConnectionError: If stream communication fails.
            OSError: If UDS socket connection fails.
            asyncio.TimeoutError: If read times out.
            RuntimeError: If response status >= 400 or unparseable.
        """
        try:
            resp = await self.client.get("/metrics", timeout=self.api_timeout)
            return self.parse_metrics(resp)["cpu_seconds"]
        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            logger.debug(f"Failed to fetch CPU seconds: {e}")
            return 0.0

    async def fetch_memory_mb(self) -> float:
        """Fetch current guest memory RSS usage in MiB.

        Returns:
            Current guest memory in MiB. Returns 0.0 on error.

        Raises:
            ConnectionError: If stream connection fails.
            OSError: If UDS socket read fails.
            asyncio.TimeoutError: If endpoint read times out.
            RuntimeError: If HTTP response is invalid or status >= 400.
        """
        try:
            resp = await self.client.get("/metrics", timeout=self.api_timeout)
            return self.parse_metrics(resp)["memory_mb"]
        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            logger.debug(f"Failed to fetch memory metrics: {e}")
            return 0.0

    async def sample_memory_loop(
        self, metering: SandboxMetering, interval: float
    ) -> None:
        """Background polling task tracking peak memory usage.

        Args:
            metering: Target metrics data class.
            interval: Poll polling interval in seconds.

        Raises:
            ConnectionError: If metric read connection fails.
            OSError: If socket transport fails.
            asyncio.TimeoutError: If metrics endpoint read times out.
            RuntimeError: If metrics request is status >= 400 or unparseable.
        """
        try:
            while True:
                try:
                    mem = await self.fetch_memory_mb()
                    if mem > metering.max_memory_mb:
                        metering.max_memory_mb = mem
                except (
                    ConnectionError,
                    OSError,
                    asyncio.TimeoutError,
                    RuntimeError,
                ) as e:
                    logger.debug(f"Memory sampling iteration error: {e}")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
