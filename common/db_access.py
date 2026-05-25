"""Acceso SQLite compartido para BD principal y réplica."""
from __future__ import annotations
import json
import sqlite3
import threading
import time


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS eventos_sensores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT NOT NULL,
    tipo TEXT NOT NULL,
    interseccion TEXT NOT NULL,
    datos_json TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    recibido_en REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS estados_semaforos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    interseccion TEXT NOT NULL,
    estado TEXT NOT NULL,
    duracion_seg INTEGER NOT NULL,
    motivo TEXT,
    timestamp TEXT NOT NULL,
    recibido_en REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS alertas_congestion (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    interseccion TEXT NOT NULL,
    nivel TEXT NOT NULL,
    accion_tomada TEXT,
    datos_json TEXT,
    timestamp TEXT NOT NULL,
    recibido_en REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ev_timestamp ON eventos_sensores(timestamp);
CREATE INDEX IF NOT EXISTS idx_ev_interseccion ON eventos_sensores(interseccion);
CREATE INDEX IF NOT EXISTS idx_semaf_interseccion ON estados_semaforos(interseccion);
"""


class DatabaseStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        with self.conn:
            self.conn.executescript(SCHEMA_SQL)

    def insertar_evento(self, evento: dict) -> None:
        ts = (
            evento.get("timestamp")
            or evento.get("timestamp_fin")
            or evento.get("timestamp_inicio", "")
        )
        sql = """
            INSERT INTO eventos_sensores
            (sensor_id, tipo, interseccion, datos_json, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(
                sql,
                (
                    evento.get("sensor_id", ""),
                    evento.get("tipo_sensor", ""),
                    evento.get("interseccion", ""),
                    json.dumps(evento, ensure_ascii=False),
                    ts,
                    time.time(),
                ),
            )
            self.conn.commit()

    def insertar_estado_semaforo(self, cmd: dict) -> None:
        sql = """
            INSERT INTO estados_semaforos
            (interseccion, estado, duracion_seg, motivo, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(
                sql,
                (
                    cmd.get("interseccion", ""),
                    cmd.get("estado", ""),
                    cmd.get("duracion_seg", 0),
                    cmd.get("motivo", ""),
                    cmd.get("timestamp", ""),
                    time.time(),
                ),
            )
            self.conn.commit()

    def insertar_alerta(self, alerta: dict) -> None:
        sql = """
            INSERT INTO alertas_congestion
            (interseccion, nivel, accion_tomada, datos_json, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(
                sql,
                (
                    alerta.get("interseccion", ""),
                    alerta.get("nivel", ""),
                    alerta.get("accion_tomada", ""),
                    json.dumps(alerta, ensure_ascii=False),
                    alerta.get("timestamp", ""),
                    time.time(),
                ),
            )
            self.conn.commit()

    def despachar(self, mensaje: dict) -> None:
        tipo = mensaje.get("tipo_msg", "evento")
        if tipo == "evento":
            self.insertar_evento(mensaje)
        elif tipo == "semaforo":
            self.insertar_estado_semaforo(mensaje)
        elif tipo == "alerta":
            self.insertar_alerta(mensaje)

    def query_historico(self, t_ini: str, t_fin: str) -> list:
        sql = """
            SELECT datos_json FROM eventos_sensores
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
        """
        with self._lock:
            rows = self.conn.execute(sql, (t_ini, t_fin)).fetchall()
        return [json.loads(r[0]) for r in rows]

    def query_estado_interseccion(self, interseccion: str) -> dict | None:
        sql = """
            SELECT interseccion, estado, duracion_seg, motivo, timestamp
            FROM estados_semaforos
            WHERE interseccion = ?
            ORDER BY recibido_en DESC
            LIMIT 1
        """
        with self._lock:
            row = self.conn.execute(sql, (interseccion,)).fetchone()
        if not row:
            return None
        return {
            "interseccion": row[0],
            "estado": row[1],
            "duracion_seg": row[2],
            "motivo": row[3],
            "timestamp": row[4],
        }

    def query_priorizaciones(self) -> list:
        sql = """
            SELECT datos_json FROM alertas_congestion
            WHERE nivel = 'PRIORIZACION'
            ORDER BY timestamp ASC
        """
        with self._lock:
            rows = self.conn.execute(sql).fetchall()
        return [json.loads(r[0]) for r in rows]

    def contar_eventos(self) -> int:
        with self._lock:
            return self.conn.execute(
                "SELECT COUNT(*) FROM eventos_sensores"
            ).fetchone()[0]

    def resumen_tablas(self) -> dict:
        tablas = {
            "eventos_sensores": "SELECT COUNT(*), MAX(timestamp) FROM eventos_sensores",
            "estados_semaforos": "SELECT COUNT(*), MAX(timestamp) FROM estados_semaforos",
            "alertas_congestion": "SELECT COUNT(*), MAX(timestamp) FROM alertas_congestion",
        }
        out = {}
        with self._lock:
            for nombre, sql in tablas.items():
                fila = self.conn.execute(sql).fetchone()
                out[nombre] = {"filas": fila[0] or 0, "ultimo": fila[1] or "sin registros"}
        return out

    def close(self) -> None:
        self.conn.close()
