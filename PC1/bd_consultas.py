"""Protocolo de consultas REQ/REP compartido entre BD principal y BD réplica."""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any


def manejar_consulta(
    conn: sqlite3.Connection,
    lock: threading.Lock,
    req: dict,
    rol: str = "principal",
) -> dict:
    """Ejecuta una consulta sobre SQLite y devuelve respuesta JSON-serializable."""
    accion = req.get("accion", "")

    if accion == "ping":
        return {"ok": True, "tipo": "ping", "rol": rol}

    if accion == "estado_sistema":
        return _estado_sistema(conn, lock, rol)

    if accion == "consulta_historica":
        desde, hasta = req.get("desde"), req.get("hasta")
        if not desde or not hasta:
            return {"ok": False, "error": "Faltan 'desde' o 'hasta'."}
        return _consulta_historica(conn, lock, desde, hasta, rol)

    if accion == "consulta_interseccion":
        inter = req.get("interseccion")
        if not inter:
            return {"ok": False, "error": "Falta 'interseccion'."}
        return _consulta_interseccion(conn, lock, inter, rol)

    return {"ok": False, "error": f"Accion desconocida: {accion!r}"}


def _fetch_all(
    conn: sqlite3.Connection,
    lock: threading.Lock,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict]:
    conn.row_factory = sqlite3.Row
    with lock:
        cur = conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def _estado_sistema(conn: sqlite3.Connection, lock: threading.Lock, rol: str) -> dict:
    totales: dict[str, dict] = {}
    with lock:
        for tabla in ("eventos_sensores", "estados_semaforos", "alertas_congestion"):
            fila = conn.execute(
                f"SELECT COUNT(*) AS n, MAX(timestamp) AS ult FROM {tabla}"
            ).fetchone()
            totales[tabla] = {"filas": fila[0], "ultimo": fila[1]}
    return {
        "ok": True,
        "tipo": "estado_sistema",
        "rol_bd": rol,
        "resumen": totales,
    }


def _consulta_historica(
    conn: sqlite3.Connection,
    lock: threading.Lock,
    desde: str,
    hasta: str,
    rol: str,
) -> dict:
    rows = _fetch_all(
        conn,
        lock,
        """
        SELECT datos_json FROM eventos_sensores
        WHERE timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
        """,
        (desde, hasta),
    )
    eventos = [json.loads(r["datos_json"]) for r in rows]
    return {
        "ok": True,
        "tipo": "consulta_historica",
        "rol_bd": rol,
        "cantidad": len(eventos),
        "eventos": eventos,
    }


def _consulta_interseccion(
    conn: sqlite3.Connection,
    lock: threading.Lock,
    interseccion: str,
    rol: str,
) -> dict:
    with lock:
        row = conn.execute(
            """
            SELECT interseccion, estado, duracion_seg, motivo, timestamp
            FROM estados_semaforos
            WHERE interseccion = ?
            ORDER BY recibido_en DESC
            LIMIT 1
            """,
            (interseccion,),
        ).fetchone()
    alertas_rows = _fetch_all(
        conn,
        lock,
        """
        SELECT datos_json FROM alertas_congestion
        WHERE interseccion = ?
        ORDER BY timestamp DESC
        LIMIT 10
        """,
        (interseccion,),
    )
    ultimo = None
    if row:
        ultimo = {
            "interseccion": row[0],
            "estado": row[1],
            "duracion_seg": row[2],
            "motivo": row[3],
            "timestamp": row[4],
        }
    return {
        "ok": True,
        "tipo": "consulta_interseccion",
        "rol_bd": rol,
        "interseccion": interseccion,
        "ultimo_estado": ultimo,
        "alertas": [json.loads(r["datos_json"]) for r in alertas_rows],
    }
