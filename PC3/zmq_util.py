"""Utilidades ZMQ REQ con timeout y recuperación de socket."""
from __future__ import annotations

import json
import logging

import zmq

log = logging.getLogger(__name__)


def crear_req(ctx: zmq.Context, endpoint: str, timeout_ms: int) -> zmq.Socket:
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    sock.connect(endpoint)
    return sock


def req_json(
    ctx: zmq.Context,
    sock: zmq.Socket | None,
    endpoint: str,
    mensaje: dict,
    timeout_ms: int,
) -> tuple[zmq.Socket, dict | None]:
    """
    Envía JSON por REQ/REP. Devuelve (socket_actualizado, respuesta o None si falla).
  Si el socket queda en estado inválido tras timeout, se recrea.
    """
    if sock is None:
        sock = crear_req(ctx, endpoint, timeout_ms)

    try:
        sock.send_json(mensaje)
        return sock, sock.recv_json()
    except zmq.Again:
        log.debug("Timeout REQ -> %s", endpoint)
    except zmq.ZMQError as exc:
        log.debug("Error ZMQ REQ -> %s: %s", endpoint, exc)

    try:
        sock.close()
    except Exception:
        pass
    return crear_req(ctx, endpoint, timeout_ms), None
