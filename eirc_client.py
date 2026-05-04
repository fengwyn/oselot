"""
eIRC client adapter for the OSELOT publisher.

Implements the four-method interface IRCPublisher requires
(connect / join / send_message / disconnect) on top of the eIRC TCP +
structured-packet protocol.

eIRC has no channel concept inside a Node, so `join` and the `channel`
arg of `send_message` are no-ops here — every send goes to the single
Node we're connected to.

Protocol (mirrors src/client/client.py in the eIRC repo):

    1. TCP connect to <host>:<port>.
    2. Server sends b"USER" raw ASCII; we reply with the username
       as raw ASCII bytes.
    3. Server broadcasts "<user> joined!" to all clients (raw ASCII)
       and sends "Connected to server!" to us (raw ASCII).
    4. From here on, every message is a packet:
         packet = build_packet(self.username, body)
       The Node enforces an "oselot*" nick prefix on lines starting
       with [OSELOT] or [OSELOT-XFER] — anything else gets bounced
       with an ERROR packet, so the username MUST start with "oselot".

A background reader drains the socket so the kernel buffer never fills
up (the Node broadcasts every send back to us plus other clients'
chatter). We only act on `ERROR` packets — they go to the log so a
misconfigured nick doesn't fail silently.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Optional

from packet import build_packet, unpack_packet

log = logging.getLogger("oselot.eirc")

# Time to wait for the server's USER prompt after connect.
_HANDSHAKE_TIMEOUT_SEC = 5.0
# How long the reader thread blocks per recv before checking the stop flag.
_READ_TIMEOUT_SEC = 1.0


class EIRCClient:
    """Concrete eIRC adapter. One instance == one Node connection."""

    def __init__(self) -> None:
        self._sock: Optional[socket.socket] = None
        self._username: str = ""
        self._reader: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._send_lock = threading.Lock()

    # ---- IRCPublisher contract --------------------------------------------

    def connect(self, server: str, port: int, nick: str) -> None:
        if self._sock is not None:
            return
        if not nick.startswith("oselot"):
            # The server would silently bounce every [OSELOT] line we sent.
            # Fail fast at connect so a misconfigured nick is obvious.
            raise ValueError(
                f"eIRC nick {nick!r} must start with 'oselot' to emit "
                f"[OSELOT] lines (server policy in node.cpp)"
            )
        self._username = nick
        self._stop.clear()

        log.info("eIRC connect %s:%d as %s", server, port, nick)
        sock = socket.create_connection((server, int(port)), timeout=_HANDSHAKE_TIMEOUT_SEC)

        # ---- USER handshake (raw ASCII, not a structured packet) ----------
        prompt = sock.recv(1024)
        if not prompt:
            sock.close()
            raise ConnectionError("eIRC: server closed before USER prompt")
        if prompt.strip() != b"USER":
            # Old/new server versions might prepend something; warn and
            # continue rather than refusing.
            log.warning("eIRC: unexpected handshake prompt %r", prompt)
        sock.sendall(nick.encode("ascii"))

        # The server then sends a "<user> joined!" broadcast and a
        # "Connected to server!" welcome (both raw ASCII, possibly
        # coalesced). We don't need to parse them - the reader thread
        # below will drain whatever shows up.

        sock.settimeout(_READ_TIMEOUT_SEC)
        self._sock = sock

        self._reader = threading.Thread(
            target=self._read_loop, name="eirc-reader", daemon=True)
        self._reader.start()

    def join(self, channel: str) -> None:
        # eIRC has no channel concept inside a Node. The Node IS the
        # room. Accept the call for interface compatibility.
        log.debug("eIRC join %s (no-op; eIRC has no channels)", channel)

    def send_message(self, channel: str, text: str) -> None:
        # `channel` ignored - see join() comment.
        if self._sock is None:
            raise ConnectionError("eIRC: not connected")
        packet = build_packet(self._username, text)
        with self._send_lock:
            self._sock.sendall(packet)

    def disconnect(self) -> None:
        self._stop.set()
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        if self._reader is not None:
            self._reader.join(timeout=2.0)
            self._reader = None
        log.info("eIRC disconnected")

    # ---- internals --------------------------------------------------------

    def _read_loop(self) -> None:
        """Drain the socket. The publisher doesn't need incoming traffic,
        but we have to read it or the kernel buffer fills and our sends
        stall. We do log ERROR packets - they're how the server tells us
        a send was rejected (e.g. nick policy)."""
        sock = self._sock
        if sock is None:
            return
        while not self._stop.is_set():
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                log.warning("eIRC: server closed the connection")
                break

            # Best-effort parse: try as a packet, fall back to raw ASCII.
            # eIRC concatenates messages on the wire so multiple packets
            # may arrive in one recv. We only care about ERROR; any
            # parse failure just gets discarded.
            try:
                p = unpack_packet(data)
                if p["header"] == "ERROR":
                    body = p["body"].decode("utf-8", "replace")
                    log.error("eIRC server reported: %s", body)
            except Exception:
                # Raw ASCII broadcast (joined / left / welcome) or
                # mid-stream coalescence. Drop silently.
                pass

        log.debug("eIRC reader thread exiting")
