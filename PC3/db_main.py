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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sensor_base import cargar_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("BDPrincipal")

DB_PATH_DEFAULT = "principal.db"


class BDPrincipal:

    def __init__(self, cfg: dict, db_path: str = DB_PATH_DEFAULT):
        self.cfg     = cfg
        self.db_path = db_path
        self._activo = True
        self._lock   = threading.Lock()

        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._crear_tablas()

        red = cfg["red"]
        self.ctx = zmq.Context()

        # recibe datos de analítica
        self.pull_escritura = self.ctx.socket(zmq.PULL)
        self.pull_escritura.setsockopt(zmq.RCVHWM, 5000)
        self.pull_escritura.bind(f"tcp://0.0.0.0:{red['bd_princ_port']}")

        # health check
        self.pull_ping = self.ctx.socket(zmq.PULL)
        self.pull_ping.bind(f"tcp://0.0.0.0:{red['health_ping_port']}")

    def _crear_tablas(self) -> None:
        with self.conn:
            self.conn.executescript("""
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
            """)

    def _despachar(self, mensaje: dict) -> None:
        tipo = mensaje.get("tipo_msg", "evento")

        if tipo == "evento":
            self._insertar_evento(mensaje)
        elif tipo == "semaforo":
            self._insertar_estado_semaforo(mensaje)
        elif tipo == "alerta":
            self._insertar_alerta(mensaje)

    def _insertar_evento(self, ev: dict) -> None:
        ts = (ev.get("timestamp") or
              ev.get("timestamp_fin") or
              ev.get("timestamp_inicio", ""))

        sql = """
            INSERT INTO eventos_sensores
            (sensor_id, tipo, interseccion, datos_json, timestamp, recibido_en)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._lock:
            self.conn.execute(sql, (
                ev.get("sensor_id", ""),
                ev.get("tipo_sensor", ""),
                ev.get("interseccion", ""),
                json.dumps(ev),
                ts,
                time.time()
            ))
            self.conn.commit()

    def _insertar_estado_semaforo(self, cmd: dict) -> None:
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

    def _insertar_alerta(self, alerta: dict) -> None:
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

    def _hilo_escritura(self) -> None:
        while self._activo:
            try:
                raw = self.pull_escritura.recv(flags=zmq.NOBLOCK)
                try:
                    msg = json.loads(raw.decode())
                    self._despachar(msg)
                except:
                    pass
            except zmq.Again:
                time.sleep(0.05)

    def _hilo_ping(self) -> None:
        while self._activo:
            try:
                self.pull_ping.recv_json(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.1)

    def ejecutar(self) -> None:
        threading.Thread(target=self._hilo_escritura, daemon=True).start()
        threading.Thread(target=self._hilo_ping, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        self.pull_escritura.close()
        self.pull_ping.close()
        self.ctx.term()
        self.conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--db", default=DB_PATH_DEFAULT)
    args = parser.parse_args()

    cfg = cargar_config(args.config)
    BDPrincipal(cfg, db_path=args.db).ejecutar()


if __name__ == "__main__":
    main()