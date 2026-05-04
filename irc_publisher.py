"""
OSELOT IRC Publisher
====================

Wraps the existing OSELOT IRC Python client and exposes two operations
used by the rest of the pipeline:

    publish_metadata(meta_dict)   - format and send a single [OSELOT] line
    send_raw(text)                - push an arbitrary line to the channel
                                    (used by the chunked transport)

Required interface of the underlying IRC client
-----------------------------------------------
The existing client is treated as an opaque dependency. This wrapper
expects it to expose the following methods. Adapt the small bridge in
`_DefaultClientAdapter` if the real client's signatures differ.

    client.connect(server: str, port: int, nick: str) -> None
    client.join(channel: str)                         -> None
    client.send_message(channel: str, text: str)      -> None
    client.disconnect()                               -> None

The wrapper keeps a single persistent connection and reconnects lazily
when send_message raises. Persistent is cleaner than stateless because
the chunked transport sends hundreds of consecutive lines.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

log = logging.getLogger("oselot.irc")


# Field order is part of the on-wire spec - subscribers parse positionally
# when they want to be cheap, and by key otherwise. Keep it stable.
METADATA_FIELD_ORDER = ("SAT", "REGION", "TS", "CH", "TRANSPORT")
METADATA_TAG = "[OSELOT]"

# Conservative IRC line cap. RFC 2812 says 512 bytes including CR/LF and
# the server-prepended ":nick!user@host PRIVMSG #chan :" prefix, which
# can easily eat 60-80 bytes. 450 leaves room.
IRC_LINE_LIMIT = 450


class IRCPublisherError(Exception):
    pass


class IRCPublisher:
    def __init__(
        self,
        client: Any,
        server: str,
        port: int,
        channel: str,
        nick: str,
        reconnect_delay_sec: int = 10,
    ) -> None:
        self._client = client
        self._server = server
        self._port = int(port)
        self._channel = channel
        self._nick = nick
        self._reconnect_delay = int(reconnect_delay_sec)
        self._connected = False
        # IRC clients are typically not thread-safe. The watcher is single
        # threaded today, but the chunked transport may be moved to a worker
        # thread later, so guard sends.
        self._lock = threading.Lock()

    # ---- connection management --------------------------------------------

    def connect(self) -> None:
        with self._lock:
            self._connect_locked()

    def _connect_locked(self) -> None:
        if self._connected:
            return
        log.info("connecting to IRC %s:%d as %s", self._server, self._port, self._nick)
        self._client.connect(self._server, self._port, self._nick)
        self._client.join(self._channel)
        self._connected = True
        log.info("joined %s", self._channel)

    def disconnect(self) -> None:
        with self._lock:
            if not self._connected:
                return
            try:
                self._client.disconnect()
            except Exception as exc:
                log.warning("disconnect raised: %s", exc)
            self._connected = False

    # ---- public API -------------------------------------------------------

    def publish_metadata(self, meta: dict) -> bool:
        """Format `meta` as an [OSELOT] line and send it. Returns True on
        success, False on failure (caller decides whether to retry)."""
        line = self._format_metadata(meta)
        ok = self._send_to(self._channel, line)
        if ok:
            log.info("published: %s", line)
        return ok

    def send_raw(self, text: str, channel: Optional[str] = None) -> bool:
        """Send an arbitrary line to `channel` (default: the metadata
        channel). Used by the chunked transport which may target a
        different channel."""
        target = channel or self._channel
        return self._send_to(target, text)

    # ---- internals --------------------------------------------------------

    @staticmethod
    def _format_metadata(meta: dict) -> str:
        parts = [METADATA_TAG]
        for key in METADATA_FIELD_ORDER:
            if key in meta:
                parts.append(f"{key}:{meta[key]}")
        # Trailing free-form fields (FILE, CHUNKS, SIZE, ...) - keep input
        # order, skip None values.
        for key, value in meta.items():
            if key in METADATA_FIELD_ORDER:
                continue
            if value is None:
                continue
            parts.append(f"{key}:{value}")
        line = " ".join(parts)
        if len(line) > IRC_LINE_LIMIT:
            # Truncate the FILE field rather than dropping it entirely so
            # subscribers still see the rest of the metadata.
            log.warning("metadata line %d > %d, truncating", len(line), IRC_LINE_LIMIT)
            line = line[: IRC_LINE_LIMIT - 3] + "..."
        return line

    def _send_to(self, channel: str, text: str) -> bool:
        # Enforce the line limit at the boundary - the chunked transport
        # sizes itself to fit but a misconfigured chunk_size could overflow.
        if len(text) > IRC_LINE_LIMIT:
            log.error("refusing to send %d-char line (limit %d)", len(text), IRC_LINE_LIMIT)
            return False
        with self._lock:
            try:
                self._connect_locked()
                self._client.send_message(channel, text)
                return True
            except Exception as exc:
                log.warning("send failed (%s); will reconnect on next send", exc)
                self._connected = False
                return False


# ---------------------------------------------------------------------------
# Default adapter: a no-op client useful for tests and dry-runs. The
# integrator replaces this with the real OSELOT IRC client.
# ---------------------------------------------------------------------------

class _DefaultClientAdapter:
    """Stub client that prints messages instead of sending them. Replace
    in production by importing the real client and passing it to
    IRCPublisher(client=...)."""

    def connect(self, server: str, port: int, nick: str) -> None:
        log.info("[stub-irc] connect %s:%d nick=%s", server, port, nick)

    def join(self, channel: str) -> None:
        log.info("[stub-irc] join %s", channel)

    def send_message(self, channel: str, text: str) -> None:
        log.info("[stub-irc] %s | %s", channel, text)

    def disconnect(self) -> None:
        log.info("[stub-irc] disconnect")


def make_default_publisher(server: str, port: int, channel: str, nick: str,
                           reconnect_delay_sec: int = 10) -> IRCPublisher:
    """Convenience for tests - builds an IRCPublisher backed by the stub
    adapter so the watcher runs end-to-end without a real IRC server."""
    return IRCPublisher(
        client=_DefaultClientAdapter(),
        server=server,
        port=port,
        channel=channel,
        nick=nick,
        reconnect_delay_sec=reconnect_delay_sec,
    )
