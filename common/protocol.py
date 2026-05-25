"""Protocolo REQ/REP del servicio de monitoreo y comandos a analítica."""
from __future__ import annotations
import time

# Operaciones del monitor (cliente REQ -> monitor REP)
OP_PING = "ping"
OP_QUERY_HISTORICO = "query_historico"
OP_QUERY_INTERSECCION = "query_interseccion"
OP_QUERY_PRIORIZACIONES = "query_priorizaciones"
OP_COMANDO_PRIORIDAD = "comando_prioridad"
OP_ESTADO_BD = "estado_bd"

# Comando hacia analítica (monitor REQ -> analítica REP)
OP_ANALITICA_PRIORIDAD = "prioridad_manual"

BACKEND_PRINCIPAL = "principal"
BACKEND_REPLICA = "replica"


def ok_response(data: dict | None = None, backend: str = BACKEND_PRINCIPAL) -> dict:
    out = {"ok": True, "backend": backend, "ts": time.time()}
    if data:
        out.update(data)
    return out


def error_response(mensaje: str, backend: str = BACKEND_PRINCIPAL) -> dict:
    return {"ok": False, "error": mensaje, "backend": backend, "ts": time.time()}
