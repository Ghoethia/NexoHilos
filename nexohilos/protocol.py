"""Mensajes JSON delimitados por salto de línea sobre un socket TCP."""

from __future__ import annotations

import json
import socket

MAX_BYTES = 1_000_000


def encode_message(message: dict) -> bytes:
    return (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")


def send_message(sock: socket.socket, message: dict) -> None:
    sock.sendall(encode_message(message))


def recv_message(sock: socket.socket) -> dict:
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("la conexión se cerró antes de completar el mensaje")
        buf += chunk
        if len(buf) > MAX_BYTES:
            raise ValueError("mensaje demasiado grande")
    line, _, _rest = buf.partition(b"\n")
    return json.loads(line.decode("utf-8"))
