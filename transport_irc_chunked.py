"""
OSELOT [OSELOT-XFER] chunked file transport over IRC.

Wire format (formal spec, also documented in README):

    [OSELOT-XFER] BEGIN filename=<name> size=<bytes> chunks=<N> sha256=<hex>
    [OSELOT-XFER] CHUNK NNNN/MMMM <base64-chunk>
    ...
    [OSELOT-XFER] END   filename=<name> sha256=<hex> status=ok

Sequence numbers are 4-digit zero-padded decimal so a single sort by
string puts them in order. Total chunk count is repeated in every CHUNK
line for resilience: if a receiver joins mid-stream it can still allocate
correctly on first sight.

Each CHUNK line is sized so the whole IRC PRIVMSG payload stays under
IRC_LINE_LIMIT (450 chars) including the `[OSELOT-XFER] CHUNK NNNN/MMMM `
prefix and the base64 expansion ratio (~4/3).
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import time
from typing import Tuple

from irc_publisher import IRC_LINE_LIMIT, IRCPublisher

log = logging.getLogger("oselot.xfer")

XFER_TAG = "[OSELOT-XFER]"

# Prefix on every CHUNK line: "[OSELOT-XFER] CHUNK NNNN/MMMM "
# That's 28 chars at MMMM=9999. Reserve 32 to be safe.
_CHUNK_PREFIX_RESERVE = 32


def _max_safe_raw_chunk_size() -> int:
    """Largest raw byte count whose base64 encoding fits in a single line."""
    available = IRC_LINE_LIMIT - _CHUNK_PREFIX_RESERVE
    # base64 expands by 4/3 (rounded up to multiple of 4). Round down to
    # multiple of 3 so the encoding has no padding except possibly on the
    # very last chunk.
    return (available // 4) * 3


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def send_file(
    publisher: IRCPublisher,
    file_path: str,
    chunk_size_bytes: int = 300,
    inter_chunk_delay_ms: int = 100,
    channel: str = None,
) -> Tuple[bool, int]:
    """Stream `file_path` to the IRC channel as a sequence of base64
    chunks. Returns (success, chunk_count). Failures are logged; never
    raises into the caller."""

    if not os.path.isfile(file_path):
        log.error("xfer: file not found: %s", file_path)
        return False, 0

    safe_max = _max_safe_raw_chunk_size()
    if chunk_size_bytes > safe_max:
        log.warning(
            "chunk_size_bytes=%d exceeds IRC-safe maximum %d; clamping",
            chunk_size_bytes, safe_max,
        )
        chunk_size_bytes = safe_max
    if chunk_size_bytes < 1:
        log.error("invalid chunk_size_bytes=%d", chunk_size_bytes)
        return False, 0

    size = os.path.getsize(file_path)
    chunk_count = (size + chunk_size_bytes - 1) // chunk_size_bytes
    if chunk_count > 9999:
        # 4-digit sequence numbers cap at 9999. Either bump chunk size or
        # fail loudly - silently producing malformed output would be worse.
        log.error(
            "file %s requires %d chunks > 9999 cap; raise chunk_size_bytes",
            file_path, chunk_count,
        )
        return False, chunk_count

    digest = _sha256(file_path)
    filename = os.path.basename(file_path)
    delay = max(0.0, inter_chunk_delay_ms / 1000.0)

    begin = (
        f"{XFER_TAG} BEGIN filename={filename} size={size} "
        f"chunks={chunk_count} sha256={digest}"
    )
    end_ok = (
        f"{XFER_TAG} END filename={filename} sha256={digest} status=ok"
    )

    if not publisher.send_raw(begin, channel=channel):
        log.error("xfer: failed to send BEGIN for %s", filename)
        return False, 0

    sent = 0
    try:
        with open(file_path, "rb") as f:
            for index in range(1, chunk_count + 1):
                raw = f.read(chunk_size_bytes)
                if not raw:
                    break
                encoded = base64.b64encode(raw).decode("ascii")
                line = f"{XFER_TAG} CHUNK {index:04d}/{chunk_count:04d} {encoded}"
                if not publisher.send_raw(line, channel=channel):
                    log.error("xfer: send failed at chunk %d/%d", index, chunk_count)
                    return False, sent
                sent += 1
                if delay and index < chunk_count:
                    time.sleep(delay)
    except OSError as exc:
        log.error("xfer: read error on %s: %s", file_path, exc)
        return False, sent

    if not publisher.send_raw(end_ok, channel=channel):
        log.error("xfer: failed to send END for %s", filename)
        return False, sent

    log.info("xfer: %s sent in %d chunks (%d bytes)", filename, sent, size)
    return True, sent
