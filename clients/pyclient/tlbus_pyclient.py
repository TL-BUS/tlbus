#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import struct
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

FRAME_LIMIT = 8 * 1024 * 1024
DEFAULT_BUS_SOCKET = "/run/tlb.sock"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_PROTOCOL = "standard"
DEFAULT_TRANSPORT = "tl-bus"
DEFAULT_CONTENT_TYPE = "application/json"
REPLY_TO_HEADER = "reply_to"
TXN_ID_HEADER = "txn_id"

_TXN_CONTEXT_LOCAL = threading.local()


class TlbusPyClientError(RuntimeError):
    pass


def create_client_logger(name: str) -> logging.Logger:
    level_name = os.environ.get("TLB_LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def envelope_trace(envelope: dict[str, Any]) -> str:
    headers = envelope.get("headers")
    if not isinstance(headers, dict):
        headers = {}
    txn_id = str(headers.get(TXN_ID_HEADER, "-"))
    reply_to = str(headers.get(REPLY_TO_HEADER, "-"))
    return (
        f"from={envelope.get('from', '?')} "
        f"to={envelope.get('to', '?')} "
        f"txn_id={txn_id} "
        f"reply_to={reply_to}"
    )


def _txn_stack() -> list[str]:
    stack = getattr(_TXN_CONTEXT_LOCAL, "stack", None)
    if stack is None:
        stack = []
        _TXN_CONTEXT_LOCAL.stack = stack
    return stack


def current_txn_id() -> str | None:
    stack = _txn_stack()
    return stack[-1] if stack else None


@contextmanager
def txn_context(txn_id: str | None = None) -> Any:
    active_txn_id = (txn_id or "").strip() or uuid.uuid4().hex
    stack = _txn_stack()
    stack.append(active_txn_id)
    try:
        yield active_txn_id
    finally:
        if stack:
            stack.pop()


class MsgpackReader:
    def __init__(self, payload: bytes) -> None:
        self.payload = memoryview(payload)
        self.offset = 0

    def read(self, size: int) -> bytes:
        if self.offset + size > len(self.payload):
            raise TlbusPyClientError("unexpected end of msgpack payload")
        chunk = self.payload[self.offset : self.offset + size].tobytes()
        self.offset += size
        return chunk


def msgpack_pack(value: Any) -> bytes:
    payload = bytearray()
    _msgpack_pack_into(payload, value)
    return bytes(payload)


def _msgpack_pack_into(payload: bytearray, value: Any) -> None:
    if value is None:
        payload.append(0xC0)
        return
    if value is False:
        payload.append(0xC2)
        return
    if value is True:
        payload.append(0xC3)
        return

    if isinstance(value, int):
        _msgpack_pack_int(payload, value)
        return

    if isinstance(value, str):
        encoded = value.encode("utf-8")
        _msgpack_pack_raw(payload, encoded, 0xA0, 0xD9, 0xDA, 0xDB)
        return

    if isinstance(value, (bytes, bytearray, memoryview)):
        encoded = bytes(value)
        _msgpack_pack_raw(payload, encoded, None, 0xC4, 0xC5, 0xC6)
        return

    if isinstance(value, list):
        size = len(value)
        if size <= 0x0F:
            payload.append(0x90 | size)
        elif size <= 0xFFFF:
            payload.extend((0xDC, *struct.pack(">H", size)))
        else:
            payload.extend((0xDD, *struct.pack(">I", size)))
        for item in value:
            _msgpack_pack_into(payload, item)
        return

    if isinstance(value, dict):
        size = len(value)
        if size <= 0x0F:
            payload.append(0x80 | size)
        elif size <= 0xFFFF:
            payload.extend((0xDE, *struct.pack(">H", size)))
        else:
            payload.extend((0xDF, *struct.pack(">I", size)))
        for key, item in value.items():
            if not isinstance(key, str):
                raise TlbusPyClientError("msgpack map keys must be strings")
            _msgpack_pack_into(payload, key)
            _msgpack_pack_into(payload, item)
        return

    raise TlbusPyClientError(f"unsupported msgpack value type: {type(value)!r}")


def _msgpack_pack_int(payload: bytearray, value: int) -> None:
    if 0 <= value <= 0x7F:
        payload.append(value)
    elif -32 <= value < 0:
        payload.append(0x100 + value)
    elif 0 <= value <= 0xFF:
        payload.extend((0xCC, value))
    elif 0 <= value <= 0xFFFF:
        payload.extend((0xCD, *struct.pack(">H", value)))
    elif 0 <= value <= 0xFFFF_FFFF:
        payload.extend((0xCE, *struct.pack(">I", value)))
    elif 0 <= value <= 0xFFFF_FFFF_FFFF_FFFF:
        payload.extend((0xCF, *struct.pack(">Q", value)))
    elif -0x80 <= value < 0:
        payload.extend((0xD0, *struct.pack(">b", value)))
    elif -0x8000 <= value < -0x80:
        payload.extend((0xD1, *struct.pack(">h", value)))
    elif -0x8000_0000 <= value < -0x8000:
        payload.extend((0xD2, *struct.pack(">i", value)))
    elif -0x8000_0000_0000_0000 <= value < -0x8000_0000:
        payload.extend((0xD3, *struct.pack(">q", value)))
    else:
        raise TlbusPyClientError(f"integer out of supported msgpack range: {value}")


def _msgpack_pack_raw(
    payload: bytearray,
    encoded: bytes,
    fix_tag: int | None,
    tag8: int,
    tag16: int,
    tag32: int,
) -> None:
    size = len(encoded)
    if fix_tag is not None and size <= 0x1F:
        payload.append(fix_tag | size)
    elif size <= 0xFF:
        payload.extend((tag8, size))
    elif size <= 0xFFFF:
        payload.extend((tag16, *struct.pack(">H", size)))
    else:
        payload.extend((tag32, *struct.pack(">I", size)))
    payload.extend(encoded)


def msgpack_unpack(payload: bytes) -> Any:
    reader = MsgpackReader(payload)
    value = _msgpack_unpack(reader)
    if reader.offset != len(reader.payload):
        raise TlbusPyClientError("trailing bytes after msgpack payload")
    return value


def _msgpack_unpack(reader: MsgpackReader) -> Any:
    tag = reader.read(1)[0]

    if tag <= 0x7F:
        return tag
    if tag >= 0xE0:
        return tag - 0x100
    if 0xA0 <= tag <= 0xBF:
        return reader.read(tag & 0x1F).decode("utf-8")
    if 0x90 <= tag <= 0x9F:
        return [_msgpack_unpack(reader) for _ in range(tag & 0x0F)]
    if 0x80 <= tag <= 0x8F:
        return {
            _expect_map_key(_msgpack_unpack(reader)): _msgpack_unpack(reader)
            for _ in range(tag & 0x0F)
        }

    if tag == 0xC0:
        return None
    if tag == 0xC2:
        return False
    if tag == 0xC3:
        return True
    if tag == 0xC4:
        return reader.read(struct.unpack(">B", reader.read(1))[0])
    if tag == 0xC5:
        return reader.read(struct.unpack(">H", reader.read(2))[0])
    if tag == 0xC6:
        return reader.read(struct.unpack(">I", reader.read(4))[0])
    if tag == 0xCC:
        return struct.unpack(">B", reader.read(1))[0]
    if tag == 0xCD:
        return struct.unpack(">H", reader.read(2))[0]
    if tag == 0xCE:
        return struct.unpack(">I", reader.read(4))[0]
    if tag == 0xCF:
        return struct.unpack(">Q", reader.read(8))[0]
    if tag == 0xD0:
        return struct.unpack(">b", reader.read(1))[0]
    if tag == 0xD1:
        return struct.unpack(">h", reader.read(2))[0]
    if tag == 0xD2:
        return struct.unpack(">i", reader.read(4))[0]
    if tag == 0xD3:
        return struct.unpack(">q", reader.read(8))[0]
    if tag == 0xD9:
        return reader.read(struct.unpack(">B", reader.read(1))[0]).decode("utf-8")
    if tag == 0xDA:
        return reader.read(struct.unpack(">H", reader.read(2))[0]).decode("utf-8")
    if tag == 0xDB:
        return reader.read(struct.unpack(">I", reader.read(4))[0]).decode("utf-8")
    if tag == 0xDC:
        return [_msgpack_unpack(reader) for _ in range(struct.unpack(">H", reader.read(2))[0])]
    if tag == 0xDD:
        return [_msgpack_unpack(reader) for _ in range(struct.unpack(">I", reader.read(4))[0])]
    if tag == 0xDE:
        return {
            _expect_map_key(_msgpack_unpack(reader)): _msgpack_unpack(reader)
            for _ in range(struct.unpack(">H", reader.read(2))[0])
        }
    if tag == 0xDF:
        return {
            _expect_map_key(_msgpack_unpack(reader)): _msgpack_unpack(reader)
            for _ in range(struct.unpack(">I", reader.read(4))[0])
        }

    raise TlbusPyClientError(f"unsupported msgpack tag 0x{tag:02x}")


def _expect_map_key(value: Any) -> str:
    if not isinstance(value, str):
        raise TlbusPyClientError("tl-bus frames expect string keys in msgpack maps")
    return value


def write_frame(sock: socket.socket, frame: dict[str, Any]) -> None:
    payload = msgpack_pack(frame)
    if len(payload) > 0xFFFF_FFFF:
        raise TlbusPyClientError("frame exceeds 4GiB frame limit")
    sock.sendall(struct.pack("<I", len(payload)))
    sock.sendall(payload)


def read_frame(sock: socket.socket) -> dict[str, Any]:
    frame_len = struct.unpack("<I", read_exact(sock, 4))[0]
    if frame_len > FRAME_LIMIT:
        raise TlbusPyClientError(
            f"incoming frame size {frame_len} exceeds the {FRAME_LIMIT} byte limit"
        )
    payload = read_exact(sock, frame_len)
    frame = msgpack_unpack(payload)
    if not isinstance(frame, dict):
        raise TlbusPyClientError("expected msgpack map as tl-bus frame")
    return frame


def read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise TlbusPyClientError("socket closed while reading tl-bus frame")
        chunks.extend(chunk)
    return bytes(chunks)


def register_service(
    bus_socket: Path,
    service_name: str,
    service_secret: str,
    *,
    is_client: bool,
    features: dict[str, str] | None = None,
    capabilities: list[dict[str, Any]] | None = None,
    modes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    frame = {
        "kind": "service_registration_request",
        "body": {
            "manifest": {
                "name": service_name,
                "secret": service_secret,
                "is_client": is_client,
                "features": features or {},
                "capabilities": capabilities or [],
                "modes": modes or [],
            }
        },
    }

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(str(bus_socket))
        write_frame(client, frame)
        response = read_frame(client)

    if response.get("kind") != "service_registration_response":
        raise TlbusPyClientError(
            f"expected registration response frame, got {response.get('kind')!r}"
        )

    body = response.get("body")
    if not isinstance(body, dict):
        raise TlbusPyClientError("registration response body must be a map")

    if not body.get("allowed") or not body.get("active"):
        raise TlbusPyClientError(
            f"service registration rejected: {body.get('reason') or 'unknown reason'}"
        )

    return body


def send_envelope(
    bus_socket: Path,
    *,
    from_service: str,
    target: str,
    headers: dict[str, str],
    payload: bytes,
) -> None:
    outbound_headers = dict(headers)
    txn_id = outbound_headers.get(TXN_ID_HEADER)
    if not isinstance(txn_id, str) or not txn_id.strip():
        outbound_headers[TXN_ID_HEADER] = current_txn_id() or uuid.uuid4().hex

    outbound_headers.setdefault("protocol", DEFAULT_PROTOCOL)
    outbound_headers.setdefault("transport", DEFAULT_TRANSPORT)
    outbound_headers.setdefault("content_type", DEFAULT_CONTENT_TYPE)

    frame = {
        "kind": "envelope",
        "body": {
            "bus_id": uuid.uuid4().bytes,
            "from": from_service,
            "to": target,
            "ts": int(time.time()),
            "ttl_ms": 1_000,
            "headers": outbound_headers,
            "payload": payload,
        },
    }

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(str(bus_socket))
        write_frame(client, frame)


def bind_listener(local_socket_path: Path) -> socket.socket:
    local_socket_path.parent.mkdir(parents=True, exist_ok=True)
    cleanup_socket_path(local_socket_path)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(local_socket_path))
    listener.listen(1)
    return listener


def cleanup_socket_path(socket_path: Path) -> None:
    try:
        socket_path.unlink()
    except FileNotFoundError:
        pass


@contextmanager
def managed_listener(local_socket_path: Path) -> Any:
    listener = bind_listener(local_socket_path)
    try:
        yield listener
    finally:
        listener.close()
        cleanup_socket_path(local_socket_path)


def receive_envelope(listener: socket.socket, timeout_seconds: float) -> dict[str, Any]:
    listener.settimeout(timeout_seconds)
    connection, _ = listener.accept()
    with connection:
        frame = read_frame(connection)
    if frame.get("kind") != "envelope":
        raise TlbusPyClientError(f"expected envelope frame, got {frame.get('kind')!r}")
    body = frame.get("body")
    if not isinstance(body, dict):
        raise TlbusPyClientError("envelope frame body must be a map")
    return body


def map_remote_socket_path(local_bus_socket: Path, remote_bus_socket: str, remote_service_socket: str) -> Path:
    remote_bus_dir = Path(remote_bus_socket).parent
    if remote_bus_dir is None:
        return local_bus_socket.parent / Path(remote_service_socket).name

    remote_service = Path(remote_service_socket)
    try:
        relative_service = remote_service.relative_to(remote_bus_dir)
    except ValueError:
        relative_service = Path(remote_service.name)
    return local_bus_socket.parent / relative_service


def parse_pairs(entries: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for entry in entries:
        if "=" not in entry:
            raise TlbusPyClientError(f"expected key=value entry, got {entry!r}")
        key, value = entry.split("=", 1)
        key = key.strip()
        if not key:
            raise TlbusPyClientError("header/feature keys must not be empty")
        parsed[key] = value
    return parsed


def parse_json_or_text(value: str) -> bytes:
    try:
        parsed = json.loads(value)
        return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except json.JSONDecodeError:
        return value.encode("utf-8")


def decode_payload(payload: bytes | bytearray | memoryview) -> Any:
    raw = bytes(payload)
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", errors="replace")


def run_register(args: argparse.Namespace) -> int:
    logger = create_client_logger(args.service_name)
    body = register_service(
        Path(args.bus_socket),
        args.service_name,
        args.service_secret,
        is_client=not args.is_worker,
        features=parse_pairs(args.feature),
        capabilities=[],
        modes=[],
    )
    logger.info(
        "event=register service=%s is_client=%s service_socket=%s",
        args.service_name,
        str(not args.is_worker).lower(),
        body.get("service_socket", "-"),
    )
    print(json.dumps(body, indent=2, ensure_ascii=False))
    return 0


def run_send(args: argparse.Namespace) -> int:
    logger = create_client_logger(args.service_name)
    bus_socket = Path(args.bus_socket)
    registration = register_service(
        bus_socket,
        args.service_name,
        args.service_secret,
        is_client=True,
        features={"role": "pyclient"},
        capabilities=[
            {
                "name": "inbox",
                "address": f"{args.service_name}.inbox",
                "description": "Receives asynchronous replies",
            }
        ],
        modes=[
            {
                "transport": DEFAULT_TRANSPORT,
                "protocol": DEFAULT_PROTOCOL,
                "content_type": DEFAULT_CONTENT_TYPE,
            }
        ],
    )

    headers = parse_pairs(args.header)
    if TXN_ID_HEADER not in headers or not headers[TXN_ID_HEADER].strip():
        headers[TXN_ID_HEADER] = uuid.uuid4().hex
    headers.setdefault(REPLY_TO_HEADER, args.reply_to or f"{args.service_name}.inbox")

    with txn_context():
        send_envelope(
            bus_socket,
            from_service=args.service_name,
            target=args.target,
            headers=headers,
            payload=parse_json_or_text(args.payload),
        )

    logger.info(
        "event=send service=%s to=%s txn_id=%s reply_to=%s",
        args.service_name,
        args.target,
        headers.get(TXN_ID_HEADER, "-"),
        headers.get(REPLY_TO_HEADER, "-"),
    )
    print(
        json.dumps(
            {
                "status": "sent",
                "service": args.service_name,
                "target": args.target,
                "service_socket": registration.get("service_socket"),
            },
            ensure_ascii=False,
        )
    )
    return 0


def run_recv(args: argparse.Namespace) -> int:
    logger = create_client_logger(args.service_name)
    bus_socket = Path(args.bus_socket)
    registration = register_service(
        bus_socket,
        args.service_name,
        args.service_secret,
        is_client=True,
        features={"role": "pyclient"},
        capabilities=[
            {
                "name": "inbox",
                "address": f"{args.service_name}.inbox",
                "description": "Receives asynchronous replies",
            }
        ],
        modes=[
            {
                "transport": DEFAULT_TRANSPORT,
                "protocol": DEFAULT_PROTOCOL,
                "content_type": DEFAULT_CONTENT_TYPE,
            }
        ],
    )

    local_service_socket = map_remote_socket_path(
        bus_socket,
        str(registration["bus_socket"]),
        str(registration["service_socket"]),
    )

    with managed_listener(local_service_socket) as listener:
        envelope = receive_envelope(listener, args.timeout_seconds)

    logger.info("event=recv service=%s %s", args.service_name, envelope_trace(envelope))
    print(
        json.dumps(
            {
                "from": envelope.get("from"),
                "to": envelope.get("to"),
                "headers": envelope.get("headers"),
                "payload": decode_payload(envelope.get("payload", b"")),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Base Python client for TL-Bus AF_UNIX transport"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    register_cmd = subcommands.add_parser("register", help="register a service in TL-Bus")
    register_cmd.add_argument("--bus-socket", default=DEFAULT_BUS_SOCKET)
    register_cmd.add_argument("--service-name", required=True)
    register_cmd.add_argument("--service-secret", required=True)
    register_cmd.add_argument("--feature", action="append", default=[])
    register_cmd.add_argument("--is-worker", action="store_true")
    register_cmd.set_defaults(handler=run_register)

    send_cmd = subcommands.add_parser("send", help="register and send one envelope")
    send_cmd.add_argument("--bus-socket", default=DEFAULT_BUS_SOCKET)
    send_cmd.add_argument("--service-name", required=True)
    send_cmd.add_argument("--service-secret", required=True)
    send_cmd.add_argument("--target", required=True)
    send_cmd.add_argument("--payload", required=True)
    send_cmd.add_argument("--header", action="append", default=[])
    send_cmd.add_argument("--reply-to")
    send_cmd.set_defaults(handler=run_send)

    recv_cmd = subcommands.add_parser("recv", help="register and wait for one inbound envelope")
    recv_cmd.add_argument("--bus-socket", default=DEFAULT_BUS_SOCKET)
    recv_cmd.add_argument("--service-name", required=True)
    recv_cmd.add_argument("--service-secret", required=True)
    recv_cmd.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    recv_cmd.set_defaults(handler=run_recv)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except TlbusPyClientError as error:
        print(f"tlbus-pyclient: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
