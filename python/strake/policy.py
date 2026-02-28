import logging
import platform
import uuid
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

logger = logging.getLogger("strake.sandbox.policy")

# Default paths the sandbox always grants read access to.
# These are the minimum required for CPython and data-science libraries to function.
_DEFAULT_READ_PATHS: list[str] = [
    "/usr",
    "/lib",
    "/lib64",
    "/etc",
    "/tmp",
    "/dev/null",
    "/dev/urandom",
    "/dev/zero",
    "/proc/self",
]


@dataclass
class ScopedToken:
    """A capability-scoped token with sandbox-bound OIDC claims."""

    token_str: str
    sandbox_id: str
    claims: Dict[str, Any] = field(default_factory=dict)

    def is_valid_for(self, sandbox_id: str) -> bool:
        return self.sandbox_id == sandbox_id


@dataclass
class SandboxAttestation:
    """Audit-grade proof of constraints applied.

    NOT CRYPTOGRAPHIC ATTESTATION. This produces a plain SHA-256 hash with no
    private key and no TPM/secure-enclave involvement.
    """

    sandbox_id: str
    constraints_applied: list[str]
    timestamp: float
    landlock_abi_version: Optional[int] = None
    seatbelt_profile_hash: Optional[str] = None

    def sign(self) -> str:
        """
        Return a deterministic hash of the attestation payload.

        NOT CRYPTOGRAPHIC ATTESTATION. This produces a plain SHA-256 hash with no
        private key and no TPM/secure-enclave involvement. It can be replicated by
        anyone with the same inputs. A production implementation would use a
        hardware-backed signing key or a remote attestation service.
        """
        payload = json.dumps(
            {
                "id": self.sandbox_id,
                "constraints": self.constraints_applied,
                "timestamp": self.timestamp,
                "landlock_abi": self.landlock_abi_version,
                "seatbelt_hash": self.seatbelt_profile_hash,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()


class SandboxPolicy:
    """
    Unified Sandbox Policy DSL.
    Defines the resource limits and access constraints for a sandbox.
    """

    def __init__(
        self,
        memory_limit_mb: int = 256,
        cpu_cores: int = 1,
        strict: bool = False,
        workspace_root: Optional[str] = None,
        allowed_read_paths: Optional[list[str]] = None,
    ):
        """
        Defines the resource limits and access constraints for a sandbox.

        :param memory_limit_mb: Maximum memory in MB.
        :param cpu_cores: CPU cores limit (enforced via rlimit).
        :param strict: If True, fail sandbox initialization if OS-level
                       isolation primitives are unavailable.
        :param workspace_root: The single directory the sandbox may write to.
                               Defaults to None (no write access granted).
        :param allowed_read_paths: Additional read-only paths beyond the
                                   built-in defaults (/usr, /lib, /etc, etc.).
        """
        self.memory_limit_mb = memory_limit_mb
        self.cpu_cores = cpu_cores
        self.strict = strict
        self.workspace_root = workspace_root
        self.allowed_read_paths = list(allowed_read_paths or [])
        # Set during to_macos() — path to the generated SBPL profile file.
        self._seatbelt_profile_path: Optional[str] = None
        # Set during _apply_landlock() — detected ABI version.
        self._landlock_abi_version: Optional[int] = None

    # ─── Linux: Seccomp-BPF ──────────────────────────────────────────────

    def _apply_seccomp(self) -> bool:
        """
        Applies a seccomp-bpf deny-list filter using ctypes.

        Strategy: DENY specific dangerous syscalls with SECCOMP_RET_ERRNO(EPERM),
        ALLOW everything else. This is safer than an allowlist for Python because
        CPython uses many internal syscalls (futex, mmap, mprotect, clone, etc.)
        and an allowlist would SIGKILL the process on any missed one.

        Denied syscalls (network + exec):
          socket, connect, accept, accept4, bind, listen,
          sendto, sendmsg, sendmmsg, execve, execveat

        This matches the Cursor/Codex deny-list approach.

        Trade-off: SECCOMP_RET_ERRNO allows an attacker to distinguish
        "blocked by seccomp" from "failed for other reasons" via the
        specific errno value. SECCOMP_RET_KILL_PROCESS would be more
        opaque but terminates the entire process on any blocked call.
        We choose ERRNO for the "Honest Sandbox" model: the sandbox
        worker can surface a clean PermissionError to the agent rather
        than an unexplained SIGKILL.
        """
        import ctypes
        import struct

        # Constants
        PR_SET_SECCOMP = 22
        SECCOMP_MODE_FILTER = 2

        # BPF OpCodes
        BPF_LD = 0x00
        BPF_W = 0x00
        BPF_ABS = 0x20
        BPF_JMP = 0x05
        BPF_RET = 0x06
        BPF_K = 0x00
        BPF_JEQ = 0x10

        # Seccomp Results
        SECCOMP_RET_ALLOW = 0x7FFF0000
        # SECCOMP_RET_ERRNO with EPERM (0x0005_0001 = ERRNO flag | errno 1)
        SECCOMP_RET_ERRNO_EPERM = 0x00050001

        # x86_64 syscall numbers for the deny-list
        # NOTE: Network syscalls (socket, connect, accept, bind, listen, sendto,
        # sendmsg, sendmmsg) are intentionally NOT blocked because the Strake
        # runtime's Tokio-based federation engine requires network access to reach
        # remote data sources (e.g., Postgres, gRPC). User code cannot make direct
        # network calls because the import allowlist blocks socket/http/urllib modules.
        DENIED_SYSCALLS: list[int] = [
            59,  # execve
            322,  # execveat
        ]

        def BPF_STMT(code: int, k: int) -> bytes:
            return struct.pack("HBBI", code, 0, 0, k)

        def BPF_JUMP(code: int, k: int, jt: int, jf: int) -> bytes:
            return struct.pack("HBBI", code, jt, jf, k)

        # Build filter:
        #   0: Load syscall number from seccomp_data.nr (offset 0 on x86_64)
        #   1..N: For each denied syscall, jump to DENY if match
        #   N+1: Default ALLOW
        #   N+2: DENY (ERRNO EPERM)
        n_denied = len(DENIED_SYSCALLS)
        filters: list[bytes] = []

        # Load the syscall number. seccomp_data.nr is at offset 0.
        filters.append(BPF_STMT(BPF_LD | BPF_W | BPF_ABS, 0))

        # For each denied syscall: if match, jump to the DENY instruction.
        # The DENY instruction is at position (1 + n_denied + 1) = after all
        # jump instructions and the default ALLOW.
        for i, nr in enumerate(DENIED_SYSCALLS):
            remaining = n_denied - i - 1
            # jt = jump over remaining checks + ALLOW to reach DENY
            # jf = 0 (fall through to next check)
            filters.append(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, nr, remaining + 1, 0))

        # Default: ALLOW
        filters.append(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW))

        # DENY: return EPERM
        filters.append(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM))

        filter_data = b"".join(filters)

        class SockFilter(ctypes.Structure):
            _fields_ = [
                ("code", ctypes.c_uint16),
                ("jt", ctypes.c_uint8),
                ("jf", ctypes.c_uint8),
                ("k", ctypes.c_uint32),
            ]

        class SockFProg(ctypes.Structure):
            _fields_ = [
                ("len", ctypes.c_uint16),
                ("filter", ctypes.POINTER(SockFilter)),
            ]

        try:
            libc = ctypes.CDLL("libc.so.6")
        except OSError:
            return False

        prog = SockFProg()
        prog.len = len(filters)
        prog.filter = ctypes.cast(
            ctypes.create_string_buffer(filter_data), ctypes.POINTER(SockFilter)
        )

        try:
            # Must set NO_NEW_PRIVS before applying seccomp if not root
            PR_SET_NO_NEW_PRIVS = 38
            libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)

            res = libc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, ctypes.byref(prog))
            if res != 0:
                return False
            return True
        except Exception:
            return False

    # ─── Linux: Landlock LSM ─────────────────────────────────────────────

    def _detect_landlock_abi(self) -> int:
        """
        Detect the highest supported Landlock ABI version.
        Returns -1 if Landlock is not available.
        """
        import ctypes

        try:
            libc = ctypes.CDLL("libc.so.6", use_errno=True)
        except OSError:
            return -1

        LANDLOCK_CREATE_RULESET_VERSION = 1 << 0
        # x86_64 syscall number for landlock_create_ruleset
        SYS_landlock_create_ruleset = 444

        try:
            abi = libc.syscall(
                SYS_landlock_create_ruleset, None, 0, LANDLOCK_CREATE_RULESET_VERSION
            )
            return abi if abi > 0 else -1
        except Exception:
            return -1

    def _apply_landlock(self) -> bool:
        """
        Applies Landlock filesystem isolation using the kernel syscall API.

        Creates a ruleset that:
        - Handles all known filesystem access rights (adjusted for ABI version)
        - Grants read-only access to system paths (/usr, /lib, /etc, etc.)
        - Grants read+write access to workspace_root (if configured)
        - Denies everything else by default

        Requires Linux kernel >= 5.13 with CONFIG_SECURITY_LANDLOCK=y.
        Returns True on success, False on failure (graceful degradation).
        """
        import ctypes
        import ctypes.util

        abi = self._detect_landlock_abi()
        if abi < 0:
            logger.warning("Landlock not available on this kernel.")
            return False

        # Validate ABI is within a range we understand. Future kernel
        # versions may introduce new semantics we haven't tested against.
        _MAX_SUPPORTED_ABI = 7
        if abi > _MAX_SUPPORTED_ABI:
            logger.warning(
                f"Landlock ABI version {abi} is newer than supported ({_MAX_SUPPORTED_ABI}). "
                "Proceeding with best-effort compatibility."
            )

        self._landlock_abi_version = abi
        logger.info(f"Landlock ABI version {abi} detected.")

        try:
            libc = ctypes.CDLL("libc.so.6", use_errno=True)
        except OSError:
            return False

        # ── Syscall numbers (x86_64) ──
        SYS_landlock_create_ruleset = 444
        SYS_landlock_add_rule = 445
        SYS_landlock_restrict_self = 446

        # ── Landlock filesystem access rights (bitmask) ──
        LANDLOCK_ACCESS_FS_EXECUTE = 1 << 0
        LANDLOCK_ACCESS_FS_WRITE_FILE = 1 << 1
        LANDLOCK_ACCESS_FS_READ_FILE = 1 << 2
        LANDLOCK_ACCESS_FS_READ_DIR = 1 << 3
        LANDLOCK_ACCESS_FS_REMOVE_DIR = 1 << 4
        LANDLOCK_ACCESS_FS_REMOVE_FILE = 1 << 5
        LANDLOCK_ACCESS_FS_MAKE_CHAR = 1 << 6
        LANDLOCK_ACCESS_FS_MAKE_DIR = 1 << 7
        LANDLOCK_ACCESS_FS_MAKE_REG = 1 << 8
        LANDLOCK_ACCESS_FS_MAKE_SOCK = 1 << 9
        LANDLOCK_ACCESS_FS_MAKE_FIFO = 1 << 10
        LANDLOCK_ACCESS_FS_MAKE_BLOCK = 1 << 11
        LANDLOCK_ACCESS_FS_MAKE_SYM = 1 << 12
        LANDLOCK_ACCESS_FS_REFER = 1 << 13  # ABI 2+
        LANDLOCK_ACCESS_FS_TRUNCATE = 1 << 14  # ABI 3+
        LANDLOCK_ACCESS_FS_IOCTL_DEV = 1 << 15  # ABI 5+

        LANDLOCK_RULE_PATH_BENEATH = 1

        # ── Build handled_access_fs with ABI-aware degradation ──
        handled_access_fs = (
            LANDLOCK_ACCESS_FS_EXECUTE
            | LANDLOCK_ACCESS_FS_WRITE_FILE
            | LANDLOCK_ACCESS_FS_READ_FILE
            | LANDLOCK_ACCESS_FS_READ_DIR
            | LANDLOCK_ACCESS_FS_REMOVE_DIR
            | LANDLOCK_ACCESS_FS_REMOVE_FILE
            | LANDLOCK_ACCESS_FS_MAKE_CHAR
            | LANDLOCK_ACCESS_FS_MAKE_DIR
            | LANDLOCK_ACCESS_FS_MAKE_REG
            | LANDLOCK_ACCESS_FS_MAKE_SOCK
            | LANDLOCK_ACCESS_FS_MAKE_FIFO
            | LANDLOCK_ACCESS_FS_MAKE_BLOCK
            | LANDLOCK_ACCESS_FS_MAKE_SYM
            | LANDLOCK_ACCESS_FS_REFER
            | LANDLOCK_ACCESS_FS_TRUNCATE
            | LANDLOCK_ACCESS_FS_IOCTL_DEV
        )

        # Strip newer flags for older ABI versions (fallthrough pattern)
        if abi < 5:
            handled_access_fs &= ~LANDLOCK_ACCESS_FS_IOCTL_DEV
        if abi < 3:
            handled_access_fs &= ~LANDLOCK_ACCESS_FS_TRUNCATE
        if abi < 2:
            handled_access_fs &= ~LANDLOCK_ACCESS_FS_REFER

        # ── Define ctypes structs matching kernel ABI ──
        class LandlockRulesetAttr(ctypes.Structure):
            _fields_ = [
                ("handled_access_fs", ctypes.c_uint64),
                ("handled_access_net", ctypes.c_uint64),
                ("scoped", ctypes.c_uint64),
            ]

        class LandlockPathBeneathAttr(ctypes.Structure):
            _fields_ = [
                ("allowed_access", ctypes.c_uint64),
                ("parent_fd", ctypes.c_int32),
            ]

        # ── Create ruleset ──
        attr = LandlockRulesetAttr()
        attr.handled_access_fs = handled_access_fs
        attr.handled_access_net = 0  # Network is handled by seccomp
        attr.scoped = 0

        ruleset_fd = libc.syscall(
            SYS_landlock_create_ruleset,
            ctypes.byref(attr),
            ctypes.sizeof(attr),
            0,  # flags = 0
        )
        if ruleset_fd < 0:
            errno = ctypes.get_errno()
            logger.warning(f"landlock_create_ruleset failed: errno={errno}")
            return False

        # ── Helper: add a path rule to the ruleset ──
        read_access = (
            LANDLOCK_ACCESS_FS_EXECUTE
            | LANDLOCK_ACCESS_FS_READ_FILE
            | LANDLOCK_ACCESS_FS_READ_DIR
        )
        write_access = (
            read_access
            | LANDLOCK_ACCESS_FS_WRITE_FILE
            | LANDLOCK_ACCESS_FS_REMOVE_DIR
            | LANDLOCK_ACCESS_FS_REMOVE_FILE
            | LANDLOCK_ACCESS_FS_MAKE_CHAR
            | LANDLOCK_ACCESS_FS_MAKE_DIR
            | LANDLOCK_ACCESS_FS_MAKE_REG
            | LANDLOCK_ACCESS_FS_MAKE_SOCK
            | LANDLOCK_ACCESS_FS_MAKE_FIFO
            | LANDLOCK_ACCESS_FS_MAKE_BLOCK
            | LANDLOCK_ACCESS_FS_MAKE_SYM
        )
        if abi >= 2:
            write_access |= LANDLOCK_ACCESS_FS_REFER
        if abi >= 3:
            write_access |= LANDLOCK_ACCESS_FS_TRUNCATE

        # Mask to only include bits we declared in handled_access_fs
        read_access &= handled_access_fs
        write_access &= handled_access_fs

        # O_PATH: open fd for path operations only (no read/write).
        # O_CLOEXEC: atomic close-on-exec. Requires Linux >= 2.6.23.
        # On modern kernels (>= 3.0, which is a prerequisite for Landlock
        # anyway), this is atomic and safe in multi-threaded processes.
        O_PATH = 0o10000000
        O_CLOEXEC = 0o2000000

        def _add_path_rule(path: str, access: int) -> bool:
            """Add a single path rule. Closes the FD immediately after use."""
            fd: int = -1
            try:
                if not os.path.exists(path):
                    logger.debug(f"Landlock: skipping non-existent path: {path}")
                    return True  # Not an error — path just doesn't exist on this system

                fd = os.open(path, O_PATH | O_CLOEXEC)

                path_attr = LandlockPathBeneathAttr()
                path_attr.allowed_access = access
                path_attr.parent_fd = fd

                ret = libc.syscall(
                    SYS_landlock_add_rule,
                    ruleset_fd,
                    LANDLOCK_RULE_PATH_BENEATH,
                    ctypes.byref(path_attr),
                    0,
                )
                if ret < 0:
                    errno = ctypes.get_errno()
                    logger.warning(
                        f"landlock_add_rule failed for {path}: errno={errno}"
                    )
                    return False
                return True
            except OSError as e:
                logger.warning(f"Landlock: could not open {path}: {e}")
                return False
            finally:
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        pass

        try:
            # ── Add read-only rules for system paths ──
            all_read_paths = list(_DEFAULT_READ_PATHS) + self.allowed_read_paths
            for path in all_read_paths:
                _add_path_rule(path, read_access)

            # ── Add read+write rule for workspace root ──
            if self.workspace_root:
                if not _add_path_rule(self.workspace_root, write_access):
                    logger.warning(
                        f"Landlock: could not add write rule for workspace: {self.workspace_root}"
                    )

            # ── Add read+write rule for LanceDB cache ──
            cache_dir = os.path.expanduser("~/.strake")
            if os.path.exists(cache_dir):
                if not _add_path_rule(cache_dir, write_access):
                    logger.warning(
                        f"Landlock: could not add write rule for cache: {cache_dir}"
                    )

            # ── Enforce the ruleset ──
            PR_SET_NO_NEW_PRIVS = 38
            libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)

            ret = libc.syscall(SYS_landlock_restrict_self, ruleset_fd, 0)
            if ret < 0:
                errno = ctypes.get_errno()
                logger.error(
                    f"CRITICAL: landlock_restrict_self failed (errno={errno}). "
                    "Continuing WITHOUT filesystem sandbox!"
                )
                return False

            logger.info(
                f"Landlock: enforced (ABI v{abi}, "
                f"read={len(all_read_paths)} paths, "
                f"write={'yes' if self.workspace_root else 'no'})"
            )
            return True
        finally:
            # Close the ruleset fd (path fds are already closed in _add_path_rule)
            try:
                os.close(ruleset_fd)
            except OSError:
                pass

    # ─── Linux: Combined policy ──────────────────────────────────────────

    def to_linux(self) -> list[str]:
        """Applies Linux sandbox constraints: Namespaces, setrlimit, Seccomp, Landlock"""
        import resource
        import ctypes

        applied: list[str] = []

        # 1. Resource Limits (setrlimit)
        try:
            resource.setrlimit(
                resource.RLIMIT_CPU, (self.cpu_cores * 5, self.cpu_cores * 5)
            )

            # Note: RLIMIT_AS (virtual memory limit) can break heavily multi-threaded frameworks
            # like Tokio and PyArrow because thread stacks and memory allocators reserve huge
            # virtual address spaces without actually using physical RAM.
            # We enforce memory via cgroups v2 instead where possible.

            applied.append("rlimit")
        except Exception as e:
            msg = f"Could not apply strict rlimits: {e}"
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.debug(msg)

        # 2. Cgroups v2 Delegation Check
        from pathlib import Path

        cgroup_controllers = Path("/sys/fs/cgroup/cgroup.controllers")
        if cgroup_controllers.exists():
            try:
                controllers = cgroup_controllers.read_text().split()
                if "memory" in controllers and "cpu" in controllers:
                    applied.append("cgroups_v2")
                else:
                    logger.warning(
                        "cgroups v2 mounted, but memory/cpu controllers not delegated to this user."
                    )
            except Exception as e:
                logger.warning(f"Failed to read cgroup.controllers: {e}")

        # 3. Namespaces
        try:
            libc = ctypes.CDLL("libc.so.6")
            CLONE_NEWUSER = 0x10000000
            CLONE_NEWNET = 0x40000000
            ret = libc.unshare(CLONE_NEWUSER | CLONE_NEWNET)
            if ret != 0:
                errno = ctypes.get_errno()
                raise OSError(f"unshare failed: errno {errno}")
            applied.append("namespaces")
        except Exception as e:
            msg = f"Network and User namespace isolation NOT applied: {e}"
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.warning(msg)

        # 4. Seccomp-BPF (deny-list with ERRNO)
        if self._apply_seccomp():
            applied.append("seccomp-bpf")
            logger.info("Sandbox Initialization: seccomp-bpf deny-list is ACTIVE.")
        else:
            msg = "Sandbox Initialization: seccomp-bpf filtering NOT active."
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.warning(msg)

        # 5. Landlock LSM (real enforcement)
        if self._apply_landlock():
            applied.append("landlock")
            logger.info(
                f"Sandbox Initialization: Landlock LSM (ABI v{self._landlock_abi_version}) is ACTIVE."
            )
        else:
            msg = (
                "Sandbox Initialization: Landlock LSM filesystem isolation NOT active."
            )
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.warning(msg)

        return applied

    # ─── macOS: Seatbelt ─────────────────────────────────────────────────

    def _generate_seatbelt_profile(self) -> str:
        """
        Generate a Seatbelt Profile Language (SBPL) profile string.

        Uses a deny-default policy with explicit allowances for:
        - Process execution and forking (required for Python)
        - Read access to system directories and configured paths
        - Write access to workspace_root only
        - Network access is denied by default

        Reference: Cursor/Codex seatbelt.rs implementation.
        """
        lines: list[str] = [
            "(version 1)",
            "(deny default)",
            "",
            ";; Allow process execution (required for Python interpreter)",
            "(allow process-exec)",
            "(allow process-fork)",
            "(allow signal (target self))",
            "",
            ";; Allow basic system operations",
            "(allow sysctl-read)",
            "(allow mach-lookup)",
            "(allow mach-register)",
            "(allow ipc-posix-shm-read-data)",
            "(allow ipc-posix-shm-write-data)",
            "",
            ";; Allow read access to system directories",
        ]

        # System read-only paths (macOS equivalents)
        macos_read_paths: list[str] = [
            "/usr",
            "/Library",
            "/System",
            "/private/var/db",
            "/private/etc",
            "/dev/null",
            "/dev/urandom",
            "/dev/random",
            "/dev/zero",
            "/dev/fd",
        ]

        # Add Python framework paths
        python_paths: list[str] = [
            sys.prefix,
            sys.exec_prefix,
        ]
        if hasattr(sys, "base_prefix"):
            python_paths.append(sys.base_prefix)

        all_read_paths = macos_read_paths + python_paths + self.allowed_read_paths

        for path in all_read_paths:
            canonical = self._canonicalize_macos_path(path)
            lines.append(f'(allow file-read* (subpath "{canonical}"))')

        lines.append("")

        # Workspace write access
        if self.workspace_root:
            canonical_ws = self._canonicalize_macos_path(self.workspace_root)
            lines.append(f";; Workspace write access: {canonical_ws}")
            lines.append(f'(allow file-read* (subpath "{canonical_ws}"))')
            lines.append(f'(allow file-write* (subpath "{canonical_ws}"))')
        else:
            lines.append(";; No workspace write access configured")

        lines.append("")

        # Temporary files (Python needs these)
        lines.append(";; Allow temporary file access")
        lines.append('(allow file-read* (subpath "/private/tmp"))')
        lines.append('(allow file-write* (subpath "/private/tmp"))')
        lines.append('(allow file-read* (subpath "/private/var/folders"))')
        lines.append('(allow file-write* (subpath "/private/var/folders"))')

        lines.append("")
        lines.append(";; Network access: DENIED by default (deny default covers this)")
        lines.append("")

        return "\n".join(lines) + "\n"

    @staticmethod
    def _canonicalize_macos_path(path: str) -> str:
        """
        Canonicalize a macOS path, resolving symlinks like /var -> /private/var.

        On macOS, /var, /tmp, and /etc are symlinks to /private/var, /private/tmp,
        and /private/etc respectively. Seatbelt requires canonical paths.
        """
        try:
            return os.path.realpath(path)
        except OSError:
            return path

    def to_macos(self) -> list[str]:
        """Applies macOS Seatbelt and rlimit constraints.

        Requires /usr/bin/sandbox-exec to be present on the system.
        This binary ships with macOS but may be absent in stripped CI images.
        """
        import resource
        import tempfile

        applied: list[str] = []

        # Pre-check: verify sandbox-exec is available
        _SEATBELT_BIN = "/usr/bin/sandbox-exec"
        if not os.path.isfile(_SEATBELT_BIN):
            msg = (
                f"sandbox-exec not found at {_SEATBELT_BIN}. "
                "Seatbelt enforcement unavailable on this system."
            )
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.warning(msg)

        # 1. Resource Limits
        try:
            resource.setrlimit(
                resource.RLIMIT_CPU, (self.cpu_cores * 5, self.cpu_cores * 5)
            )
            mem_bytes = self.memory_limit_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (mem_bytes, mem_bytes))
            applied.append("rlimit")
        except Exception as e:
            msg = f"Could not apply strict rlimits: {e}"
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.debug(msg)

        # 2. Seatbelt Profile Generation
        try:
            profile = self._generate_seatbelt_profile()

            # Write profile to a temporary file for sandbox-exec to consume
            profile_fd, profile_path = tempfile.mkstemp(
                prefix="strake_seatbelt_", suffix=".sb"
            )
            try:
                os.write(profile_fd, profile.encode("utf-8"))
            finally:
                os.close(profile_fd)

            self._seatbelt_profile_path = profile_path
            applied.append("seatbelt")
            logger.info(
                f"Sandbox Initialization: Seatbelt profile generated at {profile_path}"
            )
        except Exception as e:
            msg = f"Could not generate Seatbelt profile: {e}"
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.warning(msg)

        return applied

    # ─── Windows ─────────────────────────────────────────────────────────

    def to_windows(self) -> list[str]:
        """Applies Windows Job Objects constraints."""
        import ctypes

        applied: list[str] = []
        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            job = kernel32.CreateJobObjectW(None, None)
            current_process = kernel32.GetCurrentProcess()
            kernel32.AssignProcessToJobObject(job, current_process)
            applied.append("job_object")
        except Exception as e:
            msg = f"Could not apply Windows Job Object sandbox: {e}"
            if self.strict:
                raise RuntimeError(f"Sandbox Hardening Error: {msg}")
            logger.debug(msg)

        logger.error(
            "Sandbox Initialization: AppContainer LPAC and SID_PRIVATE_NETWORK_CLIENT_SERVER "
            "restriction is NOT active. Egress network access is possible."
        )

        if self.strict:
            raise RuntimeError(
                "Sandbox Hardening Error: Windows AppContainer isolation not available."
            )

        return applied

    def to_wasm(self) -> list[str]:
        """Future path for WASM execution."""
        return ["wasm-sandbox-mock"]

    def apply_for_os(self, os_type: str) -> list[str]:
        if os_type == "linux":
            return self.to_linux()
        elif os_type == "macos":
            return self.to_macos()
        elif os_type == "windows":
            return self.to_windows()
        elif os_type == "wasm":
            return self.to_wasm()
        elif os_type == "none":
            return ["none"]
        return []
