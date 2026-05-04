"""
OSELOT SCP image transport.

Pushes a decoded GOES image to a known host using the system `scp`
binary. We deliberately avoid paramiko - the Yocto image is minimal
and adding a Python crypto stack just to copy a file is wasteful.

The function is best-effort. A failed transfer is logged and reported
in the return value; it never raises into the watcher.
"""

from __future__ import annotations

import logging
import os
import shlex
import shutil
import subprocess
from typing import Optional, Tuple

log = logging.getLogger("oselot.scp")

# Absolute path is required by the spec ("AArch64 target ... explicitly
# PATH-safe"). Resolve once at import; the systemd unit may have a
# minimal PATH.
_SCP_BIN = shutil.which("scp") or "/usr/bin/scp"


def push(
    local_path: str,
    remote_host: str,
    remote_user: str,
    remote_path: str,
    identity_file: Optional[str] = None,
    timeout_sec: int = 60,
) -> Tuple[bool, str]:
    """Copy `local_path` to <remote_user>@<remote_host>:<remote_path>/.

    Returns (success, full_remote_target). On failure, the second element
    still contains the intended target so callers can log it.
    """
    basename = os.path.basename(local_path)
    # remote_path may or may not include a trailing slash; normalise so the
    # IRC FILE field is predictable.
    remote_dir = remote_path.rstrip("/")
    full_remote = f"{remote_host}:{remote_dir}/{basename}"
    target = f"{remote_user}@{full_remote}"

    if not os.path.isfile(local_path):
        log.error("scp: local file missing: %s", local_path)
        return False, full_remote

    cmd = [_SCP_BIN]
    if identity_file:
        cmd += ["-i", identity_file]
    # BatchMode prevents scp from prompting for a password if key auth fails -
    # without it the daemon would block forever on stdin.
    cmd += [
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"ConnectTimeout={max(5, timeout_sec // 4)}",
        local_path,
        target,
    ]

    log.info("scp push: %s", " ".join(shlex.quote(c) for c in cmd))
    try:
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired:
        log.error("scp timed out after %ds: %s", timeout_sec, local_path)
        return False, full_remote
    except FileNotFoundError:
        log.error("scp binary not found at %s", _SCP_BIN)
        return False, full_remote
    except Exception as exc:
        log.error("scp invocation failed: %s", exc)
        return False, full_remote

    if result.returncode != 0:
        log.error(
            "scp failed (rc=%d) stderr=%s",
            result.returncode,
            result.stderr.decode("utf-8", "replace").strip(),
        )
        return False, full_remote

    log.info("scp ok: %s -> %s", local_path, full_remote)
    return True, full_remote
