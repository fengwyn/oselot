#!/usr/bin/env python3
"""
OSELOT goesproc output watcher.

Watches the directory `goesproc` writes decoded LRIT imagery into. For
every new (.jpg, .json) pair, this daemon:

    1. waits for both files to be fully written
    2. parses the JSON sidecar for metadata
    3. delivers the image via the configured transport (scp / irc_chunked)
    4. publishes a structured [OSELOT] line on IRC describing the event

inotify is used when available (via the optional `inotify_simple`
package); otherwise it falls back to directory polling. Both code paths
funnel into the same processing pipeline so behavior is identical.

Run:
    python3 watcher.py --config /etc/oselot/oselot.conf
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from typing import Iterable, Optional, Set, Tuple

import irc_publisher
import transport_irc_chunked
import transport_scp

log = logging.getLogger("oselot.watcher")

# How long to wait for the .json sidecar to appear after the .jpg shows
# up (or vice versa). goesproc writes them sequentially with a small gap.
_PAIR_TIMEOUT_SEC = 30
# How long a file size must remain unchanged before we consider it
# "done writing". Two consecutive equal stat results separated by this
# delay is enough on a quiet embedded box.
_QUIESCE_INTERVAL_SEC = 0.5
_QUIESCE_MAX_WAIT_SEC = 60


# ---------------------------------------------------------------------------
# Optional inotify backend
# ---------------------------------------------------------------------------

def _try_import_inotify():
    try:
        from inotify_simple import INotify, flags  # type: ignore
        return INotify, flags
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class Config:
    def __init__(self, path: str) -> None:
        cp = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
        if not cp.read(path):
            raise FileNotFoundError(f"config not found: {path}")
        self.cp = cp

    def get(self, section: str, key: str, default=None, cast=str):
        if cp_has := self.cp.has_option(section, key):
            return cast(self.cp.get(section, key).strip())
        if default is None:
            raise KeyError(f"missing config key [{section}].{key}")
        return default


# ---------------------------------------------------------------------------
# File pairing helpers
# ---------------------------------------------------------------------------

def _is_quiescent(path: str) -> bool:
    """Return True once `path` exists and its size has stopped changing."""
    deadline = time.monotonic() + _QUIESCE_MAX_WAIT_SEC
    last_size = -1
    last_change = time.monotonic()
    while time.monotonic() < deadline:
        try:
            size = os.path.getsize(path)
        except FileNotFoundError:
            time.sleep(_QUIESCE_INTERVAL_SEC)
            continue
        now = time.monotonic()
        if size == last_size and (now - last_change) >= _QUIESCE_INTERVAL_SEC:
            return True
        if size != last_size:
            last_size = size
            last_change = now
        time.sleep(_QUIESCE_INTERVAL_SEC)
    log.warning("quiesce timeout on %s", path)
    return False


def _wait_for_pair(jpg: str, json_path: str) -> bool:
    """Block until both jpg and json exist and have stopped changing, or
    the timeout fires. Returns True iff the pair is ready."""
    deadline = time.monotonic() + _PAIR_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if os.path.exists(jpg) and os.path.exists(json_path):
            if _is_quiescent(jpg) and _is_quiescent(json_path):
                return True
        time.sleep(0.25)
    log.warning("pair timeout: %s / %s", jpg, json_path)
    return False


def _sibling_json(jpg_path: str) -> str:
    base, _ext = os.path.splitext(jpg_path)
    return base + ".json"


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

class Pipeline:
    def __init__(self, config: Config, publisher: irc_publisher.IRCPublisher) -> None:
        self.cfg = config
        self.publisher = publisher
        self.method = config.get("transport", "image_method", default="scp").lower()
        if self.method not in ("scp", "irc_chunked"):
            log.warning("unknown image_method=%r; defaulting to scp", self.method)
            self.method = "scp"

    def process(self, jpg_path: str) -> None:
        json_path = _sibling_json(jpg_path)
        if not _wait_for_pair(jpg_path, json_path):
            log.error("skipping %s; pair never settled", jpg_path)
            return

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                meta_in = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            log.error("cannot read sidecar %s: %s", json_path, exc)
            return

        meta_out = self._extract_metadata(meta_in)
        log.info("processing %s ts=%s", os.path.basename(jpg_path), meta_out.get("TS"))

        if self.method == "scp":
            ok, remote = self._do_scp(jpg_path)
            meta_out["TRANSPORT"] = "scp"
            meta_out["FILE"] = remote
            meta_out["STATUS"] = "ok" if ok else "fail"
        else:
            ok, count = self._do_chunked(jpg_path)
            meta_out["TRANSPORT"] = "irc_chunked"
            meta_out["CHUNKS"] = count
            meta_out["SIZE"] = os.path.getsize(jpg_path) if os.path.exists(jpg_path) else 0
            meta_out["STATUS"] = "ok" if ok else "fail"

        if not self.publisher.publish_metadata(meta_out):
            log.error("failed to publish metadata for %s", jpg_path)

    def _extract_metadata(self, raw: dict) -> dict:
        # The exact field names in goesproc's JSON vary across versions and
        # the user's own pipeline. Pull from a few likely keys; fall back
        # to filename parsing if the JSON is sparse.
        sat = (raw.get("satellite_id") or raw.get("satellite")
               or self.cfg.get("oselot", "satellite_id", default="UNKNOWN"))
        region = raw.get("region") or raw.get("region_code") or "UNK"
        ts = raw.get("timestamp") or raw.get("time") or ""
        channel = raw.get("channel") or raw.get("ch") or ""
        return {
            "SAT": sat,
            "REGION": region,
            "TS": ts,
            "CH": channel,
        }

    def _do_scp(self, local_path: str) -> Tuple[bool, str]:
        try:
            return transport_scp.push(
                local_path=local_path,
                remote_host=self.cfg.get("scp", "remote_host"),
                remote_user=self.cfg.get("scp", "remote_user"),
                remote_path=self.cfg.get("scp", "remote_path"),
                identity_file=self.cfg.get("scp", "identity_file", default=""),
            )
        except Exception as exc:
            # transport_scp.push doesn't raise, but config lookups can.
            log.error("scp config error: %s", exc)
            return False, ""

    def _do_chunked(self, local_path: str) -> Tuple[bool, int]:
        try:
            return transport_irc_chunked.send_file(
                publisher=self.publisher,
                file_path=local_path,
                chunk_size_bytes=self.cfg.get(
                    "irc_chunked", "chunk_size_bytes", default=300, cast=int),
                inter_chunk_delay_ms=self.cfg.get(
                    "irc_chunked", "inter_chunk_delay_ms", default=100, cast=int),
                channel=self.cfg.get(
                    "irc_chunked", "channel",
                    default=self.cfg.get("irc", "channel")),
            )
        except Exception as exc:
            log.error("chunked config error: %s", exc)
            return False, 0


# ---------------------------------------------------------------------------
# Watch loops
# ---------------------------------------------------------------------------

def _scan_existing(directory: str) -> Set[str]:
    """Index files already present at startup so we don't reprocess
    everything on a watcher restart."""
    seen: Set[str] = set()
    try:
        for name in os.listdir(directory):
            if name.endswith(".jpg"):
                seen.add(os.path.join(directory, name))
    except FileNotFoundError:
        pass
    return seen


def _watch_inotify(directory: str, on_jpg) -> None:
    INotify, flags = _try_import_inotify()
    if INotify is None:
        raise RuntimeError("inotify_simple not available")

    inotify = INotify()
    # CLOSE_WRITE fires when goesproc closes the fd it wrote the jpg through;
    # MOVED_TO catches atomic write-then-rename. Together they trigger
    # exactly once per finalized file.
    watch_flags = (flags.CLOSE_WRITE | flags.MOVED_TO)
    inotify.add_watch(directory, watch_flags)
    log.info("inotify watching %s", directory)

    while True:
        for event in inotify.read(timeout=2000):
            if not event.name.endswith(".jpg"):
                continue
            path = os.path.join(directory, event.name)
            on_jpg(path)


def _watch_polling(directory: str, interval: float, on_jpg) -> None:
    log.info("polling %s every %.1fs", directory, interval)
    seen = _scan_existing(directory)
    log.info("ignoring %d pre-existing jpg files at startup", len(seen))
    while True:
        try:
            current = {
                os.path.join(directory, n)
                for n in os.listdir(directory)
                if n.endswith(".jpg")
            }
        except FileNotFoundError:
            log.warning("watch dir vanished: %s", directory)
            time.sleep(interval)
            continue
        new = current - seen
        for path in sorted(new):
            on_jpg(path)
        seen = current
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _setup_logging(level: str, log_file: Optional[str]) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            handlers.append(
                logging.handlers.RotatingFileHandler(
                    log_file, maxBytes=2_000_000, backupCount=3)
            )
        except OSError as exc:
            print(f"warning: cannot open log file {log_file}: {exc}", file=sys.stderr)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=handlers,
        force=True,
    )


def _build_publisher(cfg: Config) -> irc_publisher.IRCPublisher:
    """Build the IRC publisher.

    Two modes:
      - eirc (default): real eIRC TCP+packet client; publishes to a Node
        room, no manual setup beyond the address in oselot.conf.
      - stub: discards messages to the journal; useful for end-to-end
        tests without a running Node.
    """
    mode = cfg.get("irc", "mode", default="eirc").lower()
    if mode == "stub":
        return irc_publisher.make_default_publisher(
            server=cfg.get("irc", "server"),
            port=cfg.get("irc", "port", cast=int),
            channel=cfg.get("irc", "channel", default="(unused)"),
            nick=cfg.get("irc", "nick"),
        )

    import eirc_client
    return irc_publisher.IRCPublisher(
        client=eirc_client.EIRCClient(),
        server=cfg.get("irc", "server"),
        port=cfg.get("irc", "port", cast=int),
        # eIRC has no channels; pass a placeholder so IRCPublisher can
        # still address sends. The eIRC adapter ignores the channel arg.
        channel=cfg.get("irc", "channel", default="(node)"),
        nick=cfg.get("irc", "nick"),
        reconnect_delay_sec=cfg.get(
            "irc", "reconnect_delay_sec", default=10, cast=int),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="OSELOT goesproc watcher")
    parser.add_argument("--config", required=True, help="path to oselot.conf")
    args = parser.parse_args()

    cfg = Config(args.config)
    _setup_logging(
        level=cfg.get("oselot", "log_level", default="INFO"),
        log_file=cfg.get("oselot", "log_file", default=""),
    )

    output_dir = cfg.get("goesproc", "output_dir")
    poll_interval = cfg.get("goesproc", "poll_interval_sec", default=2, cast=int)
    if not os.path.isdir(output_dir):
        log.warning("watch dir %s does not exist; creating", output_dir)
        os.makedirs(output_dir, exist_ok=True)

    publisher = _build_publisher(cfg)
    try:
        publisher.connect()
    except Exception as exc:
        log.warning("initial IRC connect failed (%s); will retry on first send", exc)

    pipeline = Pipeline(cfg, publisher)

    def on_jpg(path: str) -> None:
        try:
            pipeline.process(path)
        except Exception as exc:
            # Final safety net - the watcher must never die from one bad file.
            log.exception("pipeline crashed on %s: %s", path, exc)

    stop = {"flag": False}

    def _on_signal(signum, _frame):
        log.info("signal %d received, shutting down", signum)
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    use_inotify = _try_import_inotify()[0] is not None
    while not stop["flag"]:
        try:
            if use_inotify:
                _watch_inotify(output_dir, on_jpg)
            else:
                _watch_polling(output_dir, poll_interval, on_jpg)
        except KeyboardInterrupt:
            break
        except Exception as exc:
            log.exception("watch loop crashed: %s; restarting in 5s", exc)
            time.sleep(5)

    publisher.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
