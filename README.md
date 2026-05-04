# OSELOT Ground Station — IRC Telemetry Publisher

Software bridge between `goesproc` (GOES LRIT image decoder) and the
existing OSELOT IRC infrastructure. Runs on a BeaglePlay under a minimal
Yocto Linux image

```
goesproc output dir  ->  watcher.py  -+->  scp transport         -> archive host
                                      |
                                      +->  irc_chunked transport -> #oselot-xfer
                                      |
                                      +->  irc_publisher         -> #oselot
```

## Files

| File | Purpose |
|---|---|
| [watcher.py](watcher.py) | Main daemon. Watches the goesproc output dir (inotify with polling fallback), pairs `.jpg` + `.json`, dispatches transport, publishes metadata |
| [irc_publisher.py](irc_publisher.py) | Wrapper around the existing OSELOT Python IRC client. Formats `[OSELOT]` metadata lines, exposes `send_raw` for the chunked transport |
| [transport_scp.py](transport_scp.py) | Pushes images via the system `scp` binary. Subprocess-based, no paramiko |
| [transport_irc_chunked.py](transport_irc_chunked.py) | Streams images directly over IRC using the `[OSELOT-XFER]` protocol. |
| [irc_chunked_receiver.py](irc_chunked_receiver.py) | Standalone subscriber-side reassembler. Reads channel logs from stdin or a file and verifies SHA-256 |
| [oselot.conf](oselot.conf) | Documented example INI config |
| [oselot-watcher.service](oselot-watcher.service) | systemd unit |

## Setup

```sh
# On the BeaglePlay
install -d /usr/local/bin/oselot /etc/oselot /var/log
install -m 755 watcher.py irc_publisher.py transport_scp.py \
        transport_irc_chunked.py irc_chunked_receiver.py \
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

## IRC client integration

`irc_publisher.IRCPublisher` accepts an injected `client` object. The
wrapper assumes the existing OSELOT Python IRC client exposes:

```python
client.connect(server: str, port: int, nick: str) -> None
client.join(channel: str)                         -> None
client.send_message(channel: str, text: str)      -> None
client.disconnect()                               -> None
```

If signatures differ, edit `_build_publisher` in
[watcher.py](watcher.py) and either:

1. Pass the real client directly:
   ```python
   from oselot_irc_client import Client
   return irc_publisher.IRCPublisher(client=Client(...), ...)
   ```
2. Or write a tiny adapter class that translates the four method calls

Until that bridge is wired, the watcher uses
`_DefaultClientAdapter` from [irc_publisher.py](irc_publisher.py), which
just logs every message — useful for end-to-end smoke tests

### Future: C client

The C++ IRC server speaks a custom packet format. A minimal C client on
the BeaglePlay would replace `irc_publisher.IRCPublisher` only — the
watcher and transports stay in Python. The natural integration point is
to define a stable JSON-over-stdin/stdout contract with a small C
helper, or to bind via `ctypes` to a shared library exposing the four
methods above. Out of scope for this session

## Metadata IRC line format

Each successfully processed image produces one line on the metadata
channel (default `#oselot`):

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
followed by a single space. The line is sent as the trailing parameter
of an IRC `PRIVMSG` to the configured chunked-transfer channel
(default `#oselot-xfer`). Receivers MUST anchor on the tag and ignore
any IRC framing that precedes it (timestamps, `nick!user@host PRIVMSG`,
log prefixes, etc.)

Every line stays under 450 characters total

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

The watcher should log the pair, fire the configured transport, and
publish the metadata line. With the default stub IRC client the line
shows up in the systemd journal:

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

For a closed-loop test without IRC, run the publisher into a file and
the receiver against it:

```sh
# capture: edit oselot.conf to set image_method=irc_chunked, then run
# the watcher with the stub IRC client. The stub logs every send_message
# to the journal; pipe the journal through grep to extract the
# [OSELOT-XFER] lines:
journalctl -u oselot-watcher -f -o cat \
    | grep -F '[OSELOT-XFER]' > /tmp/xfer.log

# in another shell, replay:
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
  `journalctl -u oselot-watcher | head -20` shows which backend it picked
- **scp hangs:** the wrapper sets `BatchMode=yes` so it never prompts;
  any "permission denied (publickey)" means the identity file is wrong
  or not authorized on the remote
- **Chunked transfer truncated:** check `chunk_size_bytes` — values
  above ~310 will be auto-clamped because the base64-expanded line
  would exceed the 450-char IRC limit
- **Receiver writes nothing:** verify the END line matches a BEGIN
  filename and `status=ok`; missing chunks are reported as
  `END ... missing N chunks`.
