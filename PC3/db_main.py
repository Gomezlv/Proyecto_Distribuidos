from __future__ import annotations
import zmq
import json
import logging
import threading
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
log = logging.getLogger("BDPrincipal")

DB_PATH_DEFAULT = "principal.db"


class BDPrincipal:
    def __init__(self, cfg: dict, db_path: str = DB_PATH_DEFAULT):
        self.cfg = cfg
        self._activo = True
        self.store = DatabaseStore(db_path)
        red = cfg["red"]

        self.ctx = zmq.Context()
        self.pull_escritura = self.ctx.socket(zmq.PULL)
        self.pull_escritura.setsockopt(zmq.RCVHWM, 5000)
        self.pull_escritura.bind(f"tcp://0.0.0.0:{red['bd_princ_port']}")

        self.rep_health = self.ctx.socket(zmq.REP)
        self.rep_health.bind(f"tcp://0.0.0.0:{red['health_ack_port']}")
        log.info(f"[BD-PRINCIPAL] REP health en tcp://0.0.0.0:{red['health_ack_port']}")

        self._log_estado_inicial()

    def _log_estado_inicial(self) -> None:
        resumen = self.store.resumen_tablas()
        log.info("[BD-PRINCIPAL] Estado inicial de '%s'", self.store.db_path)
        for tabla, info in resumen.items():
            log.info(
                "[BD-PRINCIPAL]   %s filas=%s último=%s",
                tabla, info["filas"], info["ultimo"],
            )

    def _hilo_escritura(self) -> None:
        while self._activo:
            try:
                raw = self.pull_escritura.recv(flags=zmq.NOBLOCK)
                try:
                    msg = json.loads(raw.decode())
                    self.store.despachar(msg)
                    log.info(
                        "[BD-PRINCIPAL] Persistido %s | %s",
                        msg.get("tipo_msg", "?"),
                        msg.get("sensor_id", msg.get("interseccion", "?")),
                    )
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.warning("[BD-PRINCIPAL] JSON inválido: %s", e)
            except zmq.Again:
                time.sleep(0.05)

    def _hilo_health(self) -> None:
        while self._activo:
            try:
                req = self.rep_health.recv_json(flags=zmq.NOBLOCK)
                if req.get("tipo") == "ping":
                    self.rep_health.send_json({"status": "ok", "rol": "principal"})
                else:
                    self.rep_health.send_json({"status": "error", "error": "tipo desconocido"})
            except zmq.Again:
                time.sleep(0.1)

    def ejecutar(self) -> None:
        threading.Thread(target=self._hilo_escritura, daemon=True, name="bd-escritura").start()
        threading.Thread(target=self._hilo_health, daemon=True, name="bd-health").start()
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
        self.rep_health.close()
        self.ctx.term()
        self.store.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="../PC1/config.json")
    parser.add_argument("--db", default=DB_PATH_DEFAULT)
    args = parser.parse_args()
    cfg = cargar_config(args.config)
    BDPrincipal(cfg, db_path=args.db).ejecutar()


if __name__ == "__main__":
    main()
