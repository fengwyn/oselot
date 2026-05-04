"""
eIRC structured packet codec — vendored from eIRC `src/utils/packet.py`.

Wire layout (little-endian):

    uint16 header_len | header bytes (utf-8)
    uint16 body_len   | body   bytes (utf-8)
    uint16 date_len   | date   bytes (utf-8, "Mon DD YYYY HH:MM:SS")

Vendored verbatim except for removing the file-side-effect logging.basicConfig
that the upstream module runs at import time (it points at log/packet.log
which does not exist on the BeaglePlay rootfs).

Keep this file in sync with the upstream eIRC repo whenever the wire
format changes. The eIRC C version (utils/packet.c) is the authoritative
spec.
"""

import struct
from datetime import datetime


def build_packet(header: str, body: str) -> bytes:
    header_bytes = header.encode("utf-8")
    body_bytes = body.encode("utf-8")

    header_len = len(header_bytes)
    body_len = len(body_bytes)

    cur_date = "{:%B %d %Y %H:%M:%S}".format(datetime.now())
    cur_date_bytes = cur_date.encode("utf-8")
    cur_date_len = len(cur_date_bytes)

    packet_format = f"<H{header_len}sH{body_len}sH{cur_date_len}s"
    packet = struct.pack(
        packet_format,
        header_len, header_bytes,
        body_len, body_bytes,
        cur_date_len, cur_date_bytes,
    )
    return packet


def unpack_packet(packet: bytes) -> dict:
    offset = 0

    header_len = struct.unpack_from("<H", packet, offset)[0]
    offset += 2
    header = struct.unpack_from(f"<{header_len}s", packet, offset)[0].decode("utf-8")
    offset += header_len

    body_len = struct.unpack_from("<H", packet, offset)[0]
    offset += 2
    body = struct.unpack_from(f"<{body_len}s", packet, offset)[0]
    offset += body_len

    date_len = struct.unpack_from("<H", packet, offset)[0]
    offset += 2
    date_str = struct.unpack_from(f"<{date_len}s", packet, offset)[0].decode("utf-8")

    return {"header": header, "body": body, "date": date_str}
