"""Monitor REQ/REP en PC2 cuando PC3 completo no está disponible."""
from __future__ import annotations
import argparse
import logging

import zmq

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import cargar_config
from common.db_access import DatabaseStore
from common.monitor_core import procesar_solicitud
from common.protocol import BACKEND_REPLICA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("MonitorFO")

DB_DEFAULT = "replica.db"


class MonitorFailover:
    def __init__(self, cfg: dict, db_path: str = DB_DEFAULT):
        self.cfg = cfg
        self._activo = True
        self.store = DatabaseStore(db_path)
        port = cfg["red"].get("monitor_failover_port", 5601)

        self.ctx = zmq.Context()
        self.rep = self.ctx.socket(zmq.REP)
        self.rep.bind(f"tcp://0.0.0.0:{port}")
        log.info("[MONITOR-FO] Modo degradado — REP en tcp://0.0.0.0:%s", port)

    def ejecutar(self) -> None:
        log.info("[MONITOR-FO] Servicio failover activo (réplica).")
        try:
            while self._activo:
                req = self.rep.recv_json()
                op = req.get("operacion", "?")
                log.info("[MONITOR-FO] Operación: %s", op)
                resp = procesar_solicitud(req, self.store, self.cfg, BACKEND_REPLICA)
                self.rep.send_json(resp)
        except KeyboardInterrupt:
            log.info("[MONITOR-FO] Detenido.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        self.rep.close()
        self.ctx.term()
        self.store.close()


def main():
    parser = argparse.ArgumentParser(description="Monitor Failover — PC2")
    parser.add_argument("--config", default="../PC1/config.json")
    parser.add_argument("--db", default=DB_DEFAULT)
    args = parser.parse_args()
    cfg = cargar_config(args.config)
    MonitorFailover(cfg, db_path=args.db).ejecutar()


if __name__ == "__main__":
    main()
