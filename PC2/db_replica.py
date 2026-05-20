from __future__ import annotations
import zmq
import json
import sqlite3
import logging
import threading
import time
import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'PC1'))
from sensor_base import cargar_config
from bd_consultas import manejar_consulta


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("BDReplica")

DB_PATH_DEFAULT = "replica.db"


class BDServicio:

    def __init__(self, cfg: dict, pull_port: int, db_path: str,
                 rol: str = "replica"):
        self.cfg      = cfg
        self.pull_port= pull_port
        self.db_path  = db_path
        self.rol      = rol
        self._activo  = True
        self._lock    = threading.Lock()

        # Inicializar BD
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._crear_tablas()
        log.info(f"[BD-{self.rol.upper()}] BD inicializada en {db_path}")

        # Socket PULL
        self.ctx         = zmq.Context()
        self.pull_socket = self.ctx.socket(zmq.PULL)
        self.pull_socket.setsockopt(zmq.RCVHWM, 5000)
        endpoint = f"tcp://0.0.0.0:{pull_port}"
        self.pull_socket.bind(endpoint)
        log.info(f"[BD-{self.rol.upper()}] PULL escuchando en {endpoint}")

        red = cfg["red"]
        self.rep_consultas = self.ctx.socket(zmq.REP)
        self.rep_consultas.setsockopt(zmq.RCVHWM, 1000)
        self.rep_consultas.setsockopt(zmq.LINGER, 2000)
        ep_q = f"tcp://0.0.0.0:{red['bd_replica_query_port']}"
        self.rep_consultas.bind(ep_q)
        log.info(f"[BD-{self.rol.upper()}] REP consultas Monitoreo -> {ep_q}")

        self.log_estado_inicial()

    def _crear_tablas(self) -> None:
        with self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS eventos_sensores (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    sensor_id   TEXT    NOT NULL,
                    tipo        TEXT    NOT NULL,
                    interseccion TEXT   NOT NULL,
                    datos_json  TEXT    NOT NULL,
                    timestamp   TEXT    NOT NULL,
                    recibido_en REAL    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS estados_semaforos (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    interseccion TEXT    NOT NULL,
                    estado       TEXT    NOT NULL,
                    duracion_seg INTEGER NOT NULL,
                    motivo       TEXT,
                    timestamp    TEXT    NOT NULL,
                    recibido_en  REAL    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alertas_congestion (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    interseccion TEXT    NOT NULL,
                    nivel        TEXT    NOT NULL,
                    accion_tomada TEXT,
                    datos_json   TEXT,
                    timestamp    TEXT    NOT NULL,
                    recibido_en  REAL    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_ev_timestamp
                    ON eventos_sensores(timestamp);
                CREATE INDEX IF NOT EXISTS idx_ev_interseccion
                    ON eventos_sensores(interseccion);
                CREATE INDEX IF NOT EXISTS idx_semaf_interseccion
                    ON estados_semaforos(interseccion);
            """)

    def log_estado_inicial(self) -> None:
        """Imprime el estado actual de la BD al arrancar el servicio."""
        try:
            tablas = {
                "eventos_sensores":   "SELECT COUNT(*), MAX(timestamp) FROM eventos_sensores",
                "estados_semaforos":  "SELECT COUNT(*), MAX(timestamp) FROM estados_semaforos",
                "alertas_congestion": "SELECT COUNT(*), MAX(timestamp) FROM alertas_congestion",
            }
            log.info(f"[BD-{self.rol.upper()}] ══════════════════════════════════════")
            log.info(f"[BD-{self.rol.upper()}] Estado inicial de '{self.db_path}'")
            log.info(f"[BD-{self.rol.upper()}] ══════════════════════════════════════")

            total_registros = 0
            for tabla, sql in tablas.items():
                with self._lock:
                    fila = self.conn.execute(sql).fetchone()
                n   = fila[0] if fila[0] else 0
                ult = fila[1] if fila[1] else "sin registros"
                total_registros += n
                log.info(
                    f"[BD-{self.rol.upper()}]   {tabla:<28} filas={n:<6} último={ult}"
                )

            log.info(f"[BD-{self.rol.upper()}] ──────────────────────────────────────")
            log.info(f"[BD-{self.rol.upper()}]   Total registros: {total_registros}")
            log.info(f"[BD-{self.rol.upper()}] ══════════════════════════════════════")
        except Exception as exc:
            log.warning(f"[BD-{self.rol.upper()}] No se pudo leer estado inicial: {exc}")

    def insertar_evento(self, evento: dict) -> None:
        """Persiste un evento de sensor."""
        ts = (evento.get("timestamp") or
              evento.get("timestamp_fin") or
              evento.get("timestamp_inicio", ""))
        sql = """
            INSERT INTO eventos_sensores
                (sensor_id, tipo, interseccion, datos_json, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(sql, (
                evento.get("sensor_id", ""),
                evento.get("tipo_sensor", ""),
                evento.get("interseccion", ""),
                json.dumps(evento),
                ts,
                time.time()
            ))
            self.conn.commit()

    def insertar_estado_semaforo(self, cmd: dict) -> None:
        """Persiste un cambio de semáforo."""
        sql = """
            INSERT INTO estados_semaforos
                (interseccion, estado, duracion_seg, motivo, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(sql, (
                cmd.get("interseccion", ""),
                cmd.get("estado", ""),
                cmd.get("duracion_seg", 0),
                cmd.get("motivo", ""),
                cmd.get("timestamp", ""),
                time.time()
            ))
            self.conn.commit()

    def insertar_alerta(self, alerta: dict) -> None:
        """Persiste una alerta de congestión."""
        sql = """
            INSERT INTO alertas_congestion
                (interseccion, nivel, accion_tomada, datos_json, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(sql, (
                alerta.get("interseccion", ""),
                alerta.get("nivel", ""),
                alerta.get("accion_tomada", ""),
                json.dumps(alerta),
                alerta.get("timestamp", ""),
                time.time()
            ))
            self.conn.commit()

    # ------------------------------------------------------------------
    # Queries (usadas por Monitoreo cuando esta BD es la activa)
    # ------------------------------------------------------------------

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
            "estado":       row[1],
            "duracion_seg": row[2],
            "motivo":       row[3],
            "timestamp":    row[4],
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

    # ------------------------------------------------------------------
    # Loop principal
    # ------------------------------------------------------------------

    def _despachar(self, mensaje: dict) -> None:
        """Enruta el mensaje al método de inserción correcto según 'tipo_msg'."""
        tipo = mensaje.get("tipo_msg", "evento")
        if tipo == "evento":
            self.insertar_evento(mensaje)
        elif tipo == "semaforo":
            self.insertar_estado_semaforo(mensaje)
        elif tipo == "alerta":
            self.insertar_alerta(mensaje)
        else:
            log.warning(f"[BD-{self.rol.upper()}] tipo_msg desconocido: {tipo!r}")

    def _hilo_escritura(self) -> None:
        while self._activo:
            try:
                raw = self.pull_socket.recv(flags=zmq.NOBLOCK)
                try:
                    msg = json.loads(raw.decode())
                    self._despachar(msg)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.warning(f"[BD-{self.rol.upper()}] JSON inválido: {e}")
            except zmq.Again:
                time.sleep(0.05)

    def _hilo_consultas(self) -> None:
        while self._activo:
            try:
                raw = self.rep_consultas.recv(flags=zmq.NOBLOCK)
                try:
                    req = json.loads(raw.decode())
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    self.rep_consultas.send_json({"ok": False, "error": f"JSON invalido: {e}"})
                    continue
                resp = manejar_consulta(self.conn, self._lock, req, rol="replica")
                self.rep_consultas.send_json(resp)
            except zmq.Again:
                time.sleep(0.02)

    def ejecutar(self) -> None:
        log.info(f"[BD-{self.rol.upper()}] Servicio iniciado (PULL + REP consultas).")
        threading.Thread(
            target=self._hilo_escritura, daemon=True, name="bd-rep-escritura"
        ).start()
        threading.Thread(
            target=self._hilo_consultas, daemon=True, name="bd-rep-consultas"
        ).start()
        try:
            while self._activo:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info(f"[BD-{self.rol.upper()}] Detenido por usuario.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        total = self.contar_eventos()
        log.info(f"[BD-{self.rol.upper()}] Total eventos_sensores: {total}")
        self.pull_socket.close()
        self.rep_consultas.close()
        self.ctx.term()
        self.conn.close()
        log.info(f"[BD-{self.rol.upper()}] Recursos liberados.")


# ===========================================================================
# Entry point — lanza la BD réplica de PC2
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="BD Réplica — PC2")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--db",     default=DB_PATH_DEFAULT)
    args = parser.parse_args()

    cfg  = cargar_config(args.config)
    red  = cfg["red"]

    servicio = BDServicio(
        cfg=cfg,
        pull_port=red["bd_rep_port"],
        db_path=args.db,
        rol="replica"
    )
    servicio.ejecutar()


if __name__ == "__main__":
    main()