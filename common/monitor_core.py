"""Lógica compartida del servicio de monitoreo REQ/REP."""
from __future__ import annotations
import json
import logging
import time

import zmq

from common.protocol import (
    BACKEND_PRINCIPAL,
    BACKEND_REPLICA,
    OP_ANALITICA_PRIORIDAD,
    OP_COMANDO_PRIORIDAD,
    OP_ESTADO_BD,
    OP_PING,
    OP_QUERY_HISTORICO,
    OP_QUERY_INTERSECCION,
    OP_QUERY_PRIORIZACIONES,
    error_response,
    ok_response,
)
from common.db_access import DatabaseStore

log = logging.getLogger("MonitorCore")


def enviar_prioridad_analitica(cfg: dict, interseccion: str, duracion: int, token: str) -> dict:
    red = cfg["red"]
    ctx = zmq.Context.instance()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, cfg.get("health_check", {}).get("timeout_ms", 3000))
    req.setsockopt(zmq.SNDTIMEO, cfg.get("health_check", {}).get("timeout_ms", 3000))
    endpoint = f"tcp://{red['pc2_ip']}:{red['analytics_cmd_port']}"
    req.connect(endpoint)
    payload = {
        "operacion": OP_ANALITICA_PRIORIDAD,
        "interseccion": interseccion,
        "duracion_seg": duracion,
        "token": token,
        "timestamp_envio": time.time(),
    }
    try:
        req.send_json(payload)
        resp = req.recv_json()
        return resp
    except zmq.Again:
        return {"ok": False, "error": "timeout analitica"}
    finally:
        req.close()


def procesar_solicitud(
    req: dict,
    store: DatabaseStore,
    cfg: dict,
    backend: str,
) -> dict:
    op = req.get("operacion", "")
    token_req = req.get("token", "")
    secret = cfg.get("seguridad", {}).get("secret_key", "")

    if op == OP_PING:
        return ok_response({"mensaje": "pong"}, backend=backend)

    if op == OP_ESTADO_BD:
        return ok_response({"tablas": store.resumen_tablas()}, backend=backend)

    if op == OP_QUERY_HISTORICO:
        t_ini = req.get("t_ini", "")
        t_fin = req.get("t_fin", "")
        if not t_ini or not t_fin:
            return error_response("t_ini y t_fin requeridos", backend=backend)
        datos = store.query_historico(t_ini, t_fin)
        return ok_response({"eventos": datos, "total": len(datos)}, backend=backend)

    if op == OP_QUERY_INTERSECCION:
        inter = req.get("interseccion", "")
        if not inter:
            return error_response("interseccion requerida", backend=backend)
        estado = store.query_estado_interseccion(inter)
        if estado is None:
            return error_response(f"sin datos para {inter}", backend=backend)
        return ok_response({"estado": estado}, backend=backend)

    if op == OP_QUERY_PRIORIZACIONES:
        datos = store.query_priorizaciones()
        return ok_response({"priorizaciones": datos, "total": len(datos)}, backend=backend)

    if op == OP_COMANDO_PRIORIDAD:
        if token_req != secret:
            return error_response("token inválido", backend=backend)
        inter = req.get("interseccion", "")
        duracion = int(req.get("duracion_seg", 60))
        if not inter:
            return error_response("interseccion requerida", backend=backend)
        log.info("[MONITOR] Comando prioridad %s %ss (backend=%s)", inter, duracion, backend)
        resp_an = enviar_prioridad_analitica(cfg, inter, duracion, secret)
        return ok_response(
            {"analitica": resp_an, "interseccion": inter, "duracion_seg": duracion},
            backend=backend,
        )

    return error_response(f"operacion desconocida: {op}", backend=backend)
