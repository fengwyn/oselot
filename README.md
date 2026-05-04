# OSELOT Ground Station — IRC Telemetry Publisher

Software bridge between `goesproc` (GOES LRIT image decoder) and the
existing OSELOT IRC infrastructure. Runs on a BeaglePlay under a minimal
Yocto Linux image

```
goesproc output dir  ->  watcher.py  -+->  scp transport         -> archive host
                                      |
                                      +->  irc_chunked transport -+
                                      |                           |
                                      +->  irc_publisher  --------+--> eIRC Node @ 192.168.0.241:6667
                                                                       (nick: oselot-goes)
```

Both the `[OSELOT]` metadata lines and the `[OSELOT-XFER]` chunked
stream go through a single eIRC Node connection. eIRC Nodes don't have
channels — the Node IS the room — so there's no separate xfer channel
to configure.

## Files

| File | Purpose |
|---|---|
| [watcher.py](watcher.py) | Main daemon. Watches the goesproc output dir (inotify with polling fallback), pairs `.jpg` + `.json`, dispatches transport, publishes metadata |
| [irc_publisher.py](irc_publisher.py) | Wrapper around the eIRC client. Formats `[OSELOT]` metadata lines, exposes `send_raw` for the chunked transport |
| [eirc_client.py](eirc_client.py) | Concrete eIRC TCP+packet adapter — does the USER handshake, drains the socket, sends `build_packet(nick, body)` per message |
| [packet.py](packet.py) | Vendored eIRC structured-packet codec (`build_packet` / `unpack_packet`) |
| [transport_scp.py](transport_scp.py) | Pushes images via the system `scp` binary. Subprocess-based, no paramiko |
| [transport_irc_chunked.py](transport_irc_chunked.py) | Streams images over eIRC using the `[OSELOT-XFER]` protocol |
| [irc_chunked_receiver.py](irc_chunked_receiver.py) | Standalone subscriber-side reassembler. Reads channel logs from stdin or a file and verifies SHA-256 |
| [oselot.conf](oselot.conf) | Documented example INI config |
| [oselot-watcher.service](oselot-watcher.service) | systemd unit |

## Setup

```sh
# On the BeaglePlay
install -d /usr/local/bin/oselot /etc/oselot /var/log
install -m 755 watcher.py irc_publisher.py eirc_client.py packet.py \
        transport_scp.py transport_irc_chunked.py irc_chunked_receiver.py \
        /usr/local/bin/oselot/
install -m 644 oselot.conf /etc/oselot/oselot.conf

install -m 644 oselot-watcher.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now oselot-watcher.service
journalctl -u oselot-watcher -f
```

Edit [/etc/oselot/oselot.conf](oselot.conf) to fix server addresses, the
SCP archive host, and the SSH identity file.

## Yocto integration

Append to `local.conf` `IMAGE_INSTALL`:

```
IMAGE_INSTALL:append = " \
    python3 \
    python3-core \
    python3-json \
    python3-logging \
    python3-configparser \
    openssh-scp \
    openssh-sftp \
    "
```

`inotify_simple` is **optional**. The watcher falls back to polling if
the import fails. To get inotify events on the target add it via either:

- `meta-python` Pip recipe (write a small `python3-inotify-simple_1.3.5.bb`),
  *or*
- pre-install the wheel into the rootfs at build time, *or*
- accept the polling fallback (1–2s detection latency on a quiet dir is
  fine for GOES image cadence).

No other third-party Python package is required by the runtime

## eIRC integration

The watcher connects directly to a single eIRC **Node** server (Tracker
is bypassed). [eirc_client.py](eirc_client.py) implements the four-method
interface IRCPublisher requires:

```python
client.connect(server: str, port: int, nick: str) -> None
client.join(channel: str)                         -> None  # no-op, eIRC has no channels
client.send_message(channel: str, text: str)      -> None  # channel arg ignored
client.disconnect()                               -> None
```

### Wire protocol

1. TCP connect to the Node.
2. Node sends `b"USER"` (raw ASCII). Client replies with the nick (raw ASCII).
3. Node broadcasts `<nick> joined!` and sends `Connected to server!`
   (both raw ASCII — drained by the reader thread, ignored).
4. Every subsequent message is `build_packet(nick, body)` where `body`
   is the `[OSELOT]` or `[OSELOT-XFER]` line.

### Nick policy

The Node enforces an `oselot*` prefix on lines starting with `[OSELOT]`
or `[OSELOT-XFER]` (see `node.cpp` "OSELOT feed: nick-restricted
ingestion"). Any other nick gets the line silently dropped and an
`ERROR` packet bounced back. [eirc_client.py](eirc_client.py) refuses
to connect with a non-`oselot*` nick to make misconfiguration obvious
at startup rather than at first publish.

Default nick: `oselot-goes`.

### Why one Node, both streams

eIRC Nodes have no channel concept — every connected client sees every
message. Splitting metadata and bulk xfer onto two Nodes would just
require two simultaneous TCP connections for no benefit; the Node
already handles `[OSELOT-XFER]` lines as pass-through (see
`node_commands.cpp` `OselotState::record` — only `[OSELOT]` metadata
lines are stored in the per-room ring buffer; chunked-transfer lines
broadcast without being recorded).

### Stub mode for testing

Set `[irc] mode = stub` in [oselot.conf](oselot.conf) to swap the eIRC
client for an in-process logger. Every send goes to the journal
prefixed with `[stub-irc]`; useful for verifying the watcher and
transports without a running Node.

## `[OSELOT]` metadata line format

Each successfully processed image produces one packet whose body
carries this line. The packet header is the bot nick (`oselot-goes`),
which is what the Node uses for its `oselot*` policy check.

```
[OSELOT] SAT:GOES19 REGION:FD TS:2024-04-30T12:30:45Z CH:13 TRANSPORT:scp FILE:archive.lan:/data/goes/incoming/20240430_123045_GOES19_FD_CH13.jpg STATUS:ok
[OSELOT] SAT:GOES19 REGION:FD TS:2024-04-30T12:30:45Z CH:13 TRANSPORT:irc_chunked CHUNKS:142 SIZE:58234 STATUS:ok
```

- Tag: `[OSELOT]` (literal).
- Fixed leading order: `SAT REGION TS CH TRANSPORT`.
- Trailing fields depend on `TRANSPORT`:
  - `scp` → `FILE:<host>:<absolute_path>`
  - `irc_chunked` → `CHUNKS:<n> SIZE:<bytes>`
- Final field: `STATUS:ok` or `STATUS:fail`
- Hard cap 450 chars (conservative below RFC 2812 512-byte line limit).
- Fields are space-separated; values must not contain spaces. The `FILE`
  field is the only field where path encoding matters — keep
  `[scp].remote_path` shell-safe

## `[OSELOT-XFER]` chunked transfer protocol

Formal spec — third parties implementing a compatible receiver should
follow this exactly

### Framing

All control and data lines start with the literal tag `[OSELOT-XFER]`
followed by a single space. The line is the **body** of an eIRC packet
whose header is the sender's nick. Receivers MUST anchor on the tag
and ignore any framing that precedes it (packet wrapping, timestamps,
log prefixes, etc.) — the receiver in this repo runs against an IRC
client log so it tolerates whatever framing the logger added.

Every line stays under 450 characters total. The eIRC Node uses a
1024-byte recv buffer and the packet header overhead is small (~30
bytes), so 450-char bodies fit with margin.

### BEGIN

```
[OSELOT-XFER] BEGIN filename=<basename> size=<bytes> chunks=<N> sha256=<hex64>
```

- `<basename>` — file basename only, no directory components, no
  whitespace. Receivers SHOULD reject paths containing `/`
- `<bytes>` — decimal, total raw byte length of the file
- `<N>` — decimal, total chunk count, `1 <= N <= 9999`
- `<hex64>` — 64-character lowercase hexadecimal SHA-256 of the raw file

### CHUNK

```
[OSELOT-XFER] CHUNK NNNN/MMMM <base64>
```

- `NNNN` — 4-digit zero-padded sequence number, `0001..MMMM`
- `MMMM` — 4-digit zero-padded total chunk count (matches BEGIN's `N`)
- `<base64>` — base64-encoded raw chunk bytes (RFC 4648 standard alphabet,
  no line breaks). All chunks except possibly the last decode to exactly
  `chunk_size_bytes` bytes; the last is `<= chunk_size_bytes`

Chunks MUST be sent in ascending sequence order. Receivers MUST tolerate
gaps (network drop, log truncation) and MAY drop the transfer at END if
chunks are missing

### END

```
[OSELOT-XFER] END filename=<basename> sha256=<hex64> status=<ok|fail>
```

- `<basename>` — must equal the BEGIN filename
- `<hex64>` — must equal the BEGIN sha256 on `status=ok`
- `status=ok` is the only value that produces a written file. Anything
  else aborts reassembly

Receivers MUST recompute SHA-256 over the concatenated chunk payloads
and compare against the END digest before delivering the file

## Testing without live satellite data

### 1. Mock file injection

The watcher is agnostic to whether files come from `goesproc` or your
hand. Drop a paired jpg + json into `[goesproc].output_dir`:

```sh
mkdir -p /data/goes/images
cat > /data/goes/images/20240430_123045_GOES19_FD_CH13.json <<'EOF'
{
  "timestamp": "2024-04-30T12:30:45Z",
  "satellite_id": "GOES19",
  "region": "FD",
  "channel": "13",
  "snr_db": 12.4
}
EOF
# Use any small jpg as a stand-in
cp /usr/share/icons/example.jpg /data/goes/images/20240430_123045_GOES19_FD_CH13.jpg
```

The watcher logs the pair, fires the configured transport, and publishes
the metadata line on the eIRC Node. With `[irc] mode = stub` the line
shows up in the journal instead:

```sh
journalctl -u oselot-watcher -f | grep stub-irc
```

### 2. Chunked receiver against a recorded log

Record any IRC client log that captured `[OSELOT-XFER]` traffic, then
replay it offline:

```sh
python3 irc_chunked_receiver.py \
    --input recorded.log \
    --output-dir ./recv \
    --log-level DEBUG
ls ./recv     # reassembled file appears here on END+sha256 match
```

For a closed-loop test without a Node, run the publisher in stub mode
and replay the journal:

```sh
# capture: set [irc] mode = stub and image_method = irc_chunked in
# oselot.conf, run the watcher, then drop a (jpg, json) pair into the
# watch dir. The stub logs every send to the journal:
journalctl -u oselot-watcher -f -o cat \
    | grep -F '[OSELOT-XFER]' > /tmp/xfer.log

# replay:
python3 irc_chunked_receiver.py --input /tmp/xfer.log --output-dir /tmp/recv
sha256sum /data/goes/images/<file>.jpg /tmp/recv/<file>.jpg  # must match
```

### 3. Watcher correctness without IRC

Set `image_method=scp` and point `remote_host` at a host where the user
has key-based ssh. Drop a pair into the watch dir and confirm the file
arrives at the remote and a metadata line is logged

## Troubleshooting

- **Watcher doesn't see new files:** check `inotify_simple` import; if
  it fails the daemon falls back to polling at `[goesproc].poll_interval_sec`.
  `journalctl -u oselot-watcher | head -20` shows which backend it picked.
- **eIRC connect fails immediately:** the Node sends `b"USER"` first; if
  the handshake times out, the Node likely isn't running on
  `[irc].server:port` or a firewall is in the way.
- **`ValueError: nick must start with 'oselot'`:** rename `[irc].nick`.
  The Node would silently bounce every send otherwise.
- **`eIRC server reported: OSELOT lines may only be emitted by oselot* nicks`:**
  same root cause as above, but caught after the fact via an `ERROR`
  packet from the Node — confirms the Node is reachable.
- **scp hangs:** the wrapper sets `BatchMode=yes` so it never prompts;
  any "permission denied (publickey)" means the identity file is wrong
  or not authorized on the remote.
- **Chunked transfer truncated:** check `chunk_size_bytes` — values
  above ~310 will be auto-clamped because the base64-expanded line
  would exceed the 450-char limit.
- **Chunks dropped on the wire:** lower `chunk_size_bytes` *or* raise
  `inter_chunk_delay_ms`. The Node's `recv(1024)` only unpacks one
  packet per call, so two packets sent within a few ms of each other
  can coalesce on the wire and the second is silently lost.
- **Receiver writes nothing:** verify the END line matches a BEGIN
  filename and `status=ok`; missing chunks are reported as
  `END ... missing N chunks`.
