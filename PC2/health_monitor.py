"""Health check de PC3; actualiza estado global pc3_activo."""
from __future__ import annotations
import argparse
import logging
import threading
import time

import zmq

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import cargar_config
from common.pc3_state import pc3_activo, set_pc3_activo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("HealthMonitor")


class HealthMonitor:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._activo = True
        hc = cfg.get("health_check", {})
        self.intervalo = hc.get("intervalo_seg", 5)
        self.max_intentos = hc.get("max_intentos", 3)
        self.timeout_ms = hc.get("timeout_ms", 3000)
        self.fallos_consecutivos = 0
        red = cfg["red"]
        self.endpoint = f"tcp://{red['pc3_ip']}:{red['health_ack_port']}"
        self.ctx = zmq.Context()

    def _ping_pc3(self) -> bool:
        req = self.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        req.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        req.connect(self.endpoint)
        try:
            req.send_json({"tipo": "ping", "ts": time.time()})
            resp = req.recv_json()
            return resp.get("status") == "ok"
        except zmq.Again:
            return False
        finally:
            req.close()

    def _ciclo(self) -> None:
        while self._activo:
            ok = self._ping_pc3()
            if ok:
                if not pc3_activo():
                    log.info("[HEALTH] PC3 recuperado — reanudando escritura a principal")
                self.fallos_consecutivos = 0
                set_pc3_activo(True)
            else:
                self.fallos_consecutivos += 1
                log.warning(
                    "[HEALTH] Ping PC3 falló (%s/%s)",
                    self.fallos_consecutivos,
                    self.max_intentos,
                )
                if self.fallos_consecutivos >= self.max_intentos:
                    if pc3_activo():
                        log.error(
                            "[HEALTH] PC3 no disponible — failover: usar réplica y puerto 5601"
                        )
                    set_pc3_activo(False)
            time.sleep(self.intervalo)

    def ejecutar(self) -> None:
        log.info("[HEALTH] Monitoreo PC3 en %s", self.endpoint)
        set_pc3_activo(True)
        hilo = threading.Thread(target=self._ciclo, daemon=True, name="health-loop")
        hilo.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self._activo = False
            self.ctx.term()


def main():
    parser = argparse.ArgumentParser(description="Health Monitor — PC2")
    parser.add_argument("--config", default="../PC1/config.json")
    args = parser.parse_args()
    cfg = cargar_config(args.config)
    HealthMonitor(cfg).ejecutar()


if __name__ == "__main__":
    main()
