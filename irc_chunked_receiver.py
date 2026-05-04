#!/usr/bin/env python3
"""
OSELOT [OSELOT-XFER] receiver / reassembler.

Reads IRC channel lines from stdin (or a file given with --input) and
reassembles any complete [OSELOT-XFER] transfers into files in the
output directory, verifying SHA-256 on completion.

Usage:
    # live: tail an IRC client log into the receiver
    tail -F ~/.weechat/logs/irc.network.#oselot-xfer.weechatlog \\
        | irc_chunked_receiver.py --output-dir ./recv

    # offline: replay a recorded log
    irc_chunked_receiver.py --input recorded.log --output-dir ./recv

Lines may contain arbitrary IRC framing before the [OSELOT-XFER] tag
(timestamps, nick prefixes, etc). The parser anchors on the tag and
ignores anything before it.

Multiple concurrent transfers are supported, keyed by filename in the
BEGIN line.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, Optional

log = logging.getLogger("oselot.recv")

XFER_TAG = "[OSELOT-XFER]"

# Anchor on the tag; allow arbitrary IRC framing before it.
_TAG_RE = re.compile(r"\[OSELOT-XFER\]\s+(BEGIN|CHUNK|END)\s+(.*)$")
_KV_RE = re.compile(r"(\w+)=(\S+)")
_CHUNK_HEAD_RE = re.compile(r"^(\d+)/(\d+)\s+(\S+)\s*$")


@dataclass
class _Transfer:
    filename: str
    size: int
    chunks: int
    sha256: str
    received: Dict[int, bytes] = field(default_factory=dict)
    completed: bool = False


class Receiver:
    def __init__(self, output_dir: str) -> None:
        self._output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._transfers: Dict[str, _Transfer] = {}

    def feed_line(self, line: str) -> None:
        line = line.rstrip("\r\n")
        match = _TAG_RE.search(line)
        if not match:
            return
        kind, rest = match.group(1), match.group(2)
        try:
            if kind == "BEGIN":
                self._handle_begin(rest)
            elif kind == "CHUNK":
                self._handle_chunk(rest)
            elif kind == "END":
                self._handle_end(rest)
        except Exception as exc:
            # A malformed line should never kill the receiver - log and move on.
            log.warning("ignored malformed %s line: %s (%s)", kind, line, exc)

    # ---- handlers ---------------------------------------------------------

    def _handle_begin(self, rest: str) -> None:
        kv = dict(_KV_RE.findall(rest))
        filename = kv.get("filename")
        size = int(kv.get("size", "0"))
        chunks = int(kv.get("chunks", "0"))
        digest = kv.get("sha256", "")
        if not filename or chunks <= 0 or not digest:
            log.warning("BEGIN missing required fields: %r", kv)
            return
        if filename in self._transfers and not self._transfers[filename].completed:
            log.info("BEGIN replaces in-flight transfer for %s", filename)
        self._transfers[filename] = _Transfer(
            filename=filename, size=size, chunks=chunks, sha256=digest,
        )
        log.info("BEGIN %s size=%d chunks=%d", filename, size, chunks)

    def _handle_chunk(self, rest: str) -> None:
        # Format: NNNN/MMMM <base64>
        m = _CHUNK_HEAD_RE.match(rest)
        if not m:
            log.warning("malformed CHUNK header: %r", rest)
            return
        index = int(m.group(1))
        total = int(m.group(2))
        encoded = m.group(3)
        # Without a BEGIN we cannot know which transfer this chunk belongs
        # to. The protocol doesn't carry filename per-chunk; we accept only
        # chunks that match the most recent in-flight transfer with the
        # matching total. This is a best-effort heuristic for the case where
        # a receiver joins mid-stream - the recorded-log path is the
        # primary use case and there a BEGIN always precedes.
        target = self._find_active_transfer(total)
        if target is None:
            log.debug("CHUNK %d/%d with no matching BEGIN; skipping", index, total)
            return
        try:
            raw = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            log.warning("CHUNK %d/%d base64 decode failed: %s", index, total, exc)
            return
        target.received[index] = raw

    def _handle_end(self, rest: str) -> None:
        kv = dict(_KV_RE.findall(rest))
        filename = kv.get("filename")
        digest = kv.get("sha256", "")
        status = kv.get("status", "")
        if not filename or filename not in self._transfers:
            log.warning("END for unknown filename %r", filename)
            return
        t = self._transfers[filename]
        if status != "ok":
            log.warning("END %s status=%s; not writing", filename, status)
            t.completed = True
            return
        if digest != t.sha256:
            log.warning(
                "END %s sha256 mismatch with BEGIN: %s vs %s",
                filename, digest, t.sha256,
            )
        missing = [i for i in range(1, t.chunks + 1) if i not in t.received]
        if missing:
            log.error(
                "END %s missing %d chunks (e.g. %s); cannot reassemble",
                filename, len(missing), missing[:5],
            )
            t.completed = True
            return

        # Concatenate in sequence order.
        buf = bytearray()
        for i in range(1, t.chunks + 1):
            buf.extend(t.received[i])
        actual = hashlib.sha256(buf).hexdigest()
        if actual != t.sha256:
            log.error(
                "END %s sha256 verify failed: got %s expected %s",
                filename, actual, t.sha256,
            )
            t.completed = True
            return

        # Write atomically: write to .part, fsync, rename.
        out_path = os.path.join(self._output_dir, filename)
        part_path = out_path + ".part"
        with open(part_path, "wb") as f:
            f.write(buf)
            f.flush()
            os.fsync(f.fileno())
        os.replace(part_path, out_path)
        t.completed = True
        log.info("END %s reassembled OK -> %s", filename, out_path)

    def _find_active_transfer(self, total: int) -> Optional[_Transfer]:
        for t in self._transfers.values():
            if not t.completed and t.chunks == total:
                return t
        return None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reassemble [OSELOT-XFER] chunked files.")
    p.add_argument("--input", default="-",
                   help="Path to a log file, or '-' for stdin (default).")
    p.add_argument("--output-dir", required=True,
                   help="Directory to write reassembled files into.")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    receiver = Receiver(args.output_dir)

    if args.input == "-":
        stream = sys.stdin
    else:
        stream = open(args.input, "r", encoding="utf-8", errors="replace")

    try:
        for line in stream:
            receiver.feed_line(line)
    finally:
        if stream is not sys.stdin:
            stream.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
