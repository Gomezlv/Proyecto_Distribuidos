from __future__ import annotations
import zmq
import json
import logging
import time
import argparse

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import cargar_config
from common.db_access import DatabaseStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("BDReplica")

DB_PATH_DEFAULT = "replica.db"


class BDServicio:
    def __init__(self, cfg: dict, pull_port: int, db_path: str, rol: str = "replica"):
        self.cfg = cfg
        self.rol = rol
        self._activo = True
        self.store = DatabaseStore(db_path)
        log.info(f"[BD-{rol.upper()}] BD inicializada en {db_path}")

        self.ctx = zmq.Context()
        self.pull_socket = self.ctx.socket(zmq.PULL)
        self.pull_socket.setsockopt(zmq.RCVHWM, 5000)
        endpoint = f"tcp://0.0.0.0:{pull_port}"
        self.pull_socket.bind(endpoint)
        log.info(f"[BD-{rol.upper()}] PULL escuchando en {endpoint}")
        self._log_estado_inicial()

    def _log_estado_inicial(self) -> None:
        resumen = self.store.resumen_tablas()
        log.info(f"[BD-{self.rol.upper()}] Estado inicial de '{self.store.db_path}'")
        total = 0
        for tabla, info in resumen.items():
            total += info["filas"]
            log.info(f"[BD-{self.rol.upper()}]   {tabla:<28} filas={info['filas']:<6} último={info['ultimo']}")
        log.info(f"[BD-{self.rol.upper()}]   Total registros: {total}")

    def query_historico(self, t_ini: str, t_fin: str) -> list:
        return self.store.query_historico(t_ini, t_fin)

    def query_estado_interseccion(self, interseccion: str) -> dict | None:
        return self.store.query_estado_interseccion(interseccion)

    def query_priorizaciones(self) -> list:
        return self.store.query_priorizaciones()

    def contar_eventos(self) -> int:
        return self.store.contar_eventos()

    def ejecutar(self) -> None:
        log.info(f"[BD-{self.rol.upper()}] Servicio iniciado. Esperando eventos...")
        try:
            while self._activo:
                try:
                    raw = self.pull_socket.recv(flags=zmq.NOBLOCK)
                    try:
                        msg = json.loads(raw.decode())
                        self.store.despachar(msg)
                        log.info(
                            f"[BD-{self.rol.upper()}] Persistido "
                            f"{msg.get('tipo_msg', '?')} | "
                            f"{msg.get('sensor_id', msg.get('interseccion', '?'))}"
                        )
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        log.warning(f"[BD-{self.rol.upper()}] JSON inválido: {e}")
                except zmq.Again:
                    time.sleep(0.05)
        except KeyboardInterrupt:
            log.info(f"[BD-{self.rol.upper()}] Detenido por usuario.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        log.info(f"[BD-{self.rol.upper()}] Total eventos: {self.store.contar_eventos()}")
        self.pull_socket.close()
        self.ctx.term()
        self.store.close()
        log.info(f"[BD-{self.rol.upper()}] Recursos liberados.")


def main():
    parser = argparse.ArgumentParser(description="BD Réplica — PC2")
    parser.add_argument("--config", default="../PC1/config.json")
    parser.add_argument("--db", default=DB_PATH_DEFAULT)
    args = parser.parse_args()
    cfg = cargar_config(args.config)
    servicio = BDServicio(
        cfg=cfg,
        pull_port=cfg["red"]["bd_rep_port"],
        db_path=args.db,
        rol="replica",
    )
    servicio.ejecutar()


if __name__ == "__main__":
    main()
