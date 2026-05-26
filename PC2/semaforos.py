from __future__ import annotations
import zmq
import json
import time
import logging
import threading
import argparse

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import cargar_config
from common.coordinacion import CoordinadorSemaforos

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("CtrlSemaforos")

T_WATCHDOG_DEFAULT = 60


class Semaforo:
    def __init__(self, interseccion: str, tiempo_rojo: int = 15, alias: str | None = None):
        self.interseccion = interseccion
        self.alias = alias or interseccion
        self.estado = "ROJO"
        self.duracion_seg = tiempo_rojo
        self.ts_cambio = time.time()
        self.tiempo_rojo = tiempo_rojo

    def cambiar_estado(self, nuevo_estado: str, duracion: int) -> None:
        if nuevo_estado not in ("VERDE", "ROJO"):
            log.warning("[%s] Estado inválido: %s", self.interseccion, nuevo_estado)
            return
        anterior = self.estado
        self.estado = nuevo_estado
        self.duracion_seg = duracion
        self.ts_cambio = time.time()
        log.info(
            "[SEMAFORO] %s (%s) | %s → %s | Duración: %ss",
            self.alias, self.interseccion, anterior, nuevo_estado, duracion,
        )

    def tiempo_restante(self) -> float:
        return max(0.0, self.duracion_seg - (time.time() - self.ts_cambio))

    def __repr__(self):
        return (
            f"Semaforo({self.interseccion}, {self.estado}, "
            f"{self.duracion_seg}s, restante={self.tiempo_restante():.1f}s)"
        )


class ControlSemaforos:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        red = cfg["red"]
        reglas = cfg.get("reglas", {})
        self.tiempo_rojo = reglas.get("tiempo_rojo_default", 15)
        self.secret_key = cfg["seguridad"]["secret_key"]
        self.t_watchdog = T_WATCHDOG_DEFAULT
        self._activo = True
        self.ultimo_cmd_ts = time.time()
        self._lock = threading.Lock()

        self.coordinador = CoordinadorSemaforos(cfg)
        self.semaforos = {
            intersec: Semaforo(
                intersec, self.tiempo_rojo, self.coordinador.alias(intersec),
            )
            for intersec in cfg.get("semaforos", [])
        }
        log.info("[SEMAFOROS] %s semáforos: %s", len(self.semaforos), list(self.semaforos.keys()))

        self.ctx = zmq.Context()
        self.pull_socket = self.ctx.socket(zmq.PULL)
        self.pull_socket.setsockopt(zmq.RCVHWM, 1000)
        endpoint = f"tcp://0.0.0.0:{red['semaf_port']}"
        self.pull_socket.bind(endpoint)
        log.info("[SEMAFOROS] PULL en %s", endpoint)

    def _validar_token(self, cmd: dict) -> bool:
        if cmd.get("token") != self.secret_key:
            log.warning("[SEMAFOROS] TOKEN inválido para %s", cmd.get("interseccion"))
            return False
        return True

    def _procesar_comando(self, raw: bytes) -> None:
        try:
            cmd = json.loads(raw.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning("[SEMAFOROS] JSON inválido: %s", e)
            return
        if not self._validar_token(cmd):
            return
        interseccion = cmd.get("interseccion")
        estado = cmd.get("estado")
        duracion = cmd.get("duracion_seg", 15)
        motivo = cmd.get("motivo", "sin_motivo")
        alias_cmd = cmd.get("alias") or self.coordinador.alias(interseccion)
        with self._lock:
            if interseccion not in self.semaforos:
                self.semaforos[interseccion] = Semaforo(
                    interseccion, self.tiempo_rojo, alias_cmd,
                )
            self.semaforos[interseccion].cambiar_estado(estado, duracion)
        self.ultimo_cmd_ts = time.time()
        log.info(
            "[SEMAFOROS] Ejecutado | %s (%s) → %s | %ss | %s",
            alias_cmd, interseccion, estado, duracion, motivo,
        )

    def _hilo_ciclo(self) -> None:
        """Tras VERDE expira → ROJO; tras ROJO expira permanece hasta nuevo comando."""
        while self._activo:
            time.sleep(0.5)
            with self._lock:
                for sem in self.semaforos.values():
                    if sem.tiempo_restante() > 0:
                        continue
                    if sem.estado == "VERDE":
                        sem.cambiar_estado("ROJO", sem.tiempo_rojo)
                    elif sem.estado == "ROJO":
                        sem.ts_cambio = time.time()

    def _modo_seguro(self) -> None:
        log.warning("[SEMAFOROS] WATCHDOG — modo seguro VERDE 15s")
        with self._lock:
            for sem in self.semaforos.values():
                sem.cambiar_estado("VERDE", 15)
        self.ultimo_cmd_ts = time.time()

    def _hilo_watchdog(self) -> None:
        while self._activo:
            time.sleep(5)
            if time.time() - self.ultimo_cmd_ts > self.t_watchdog:
                self._modo_seguro()

    def ejecutar(self) -> None:
        threading.Thread(target=self._hilo_watchdog, daemon=True).start()
        threading.Thread(target=self._hilo_ciclo, daemon=True, name="ciclo-sem").start()
        log.info("[SEMAFOROS] Ciclo automático VERDE→ROJO activo.")
        try:
            while self._activo:
                try:
                    raw = self.pull_socket.recv(flags=zmq.NOBLOCK)
                    self._procesar_comando(raw)
                except zmq.Again:
                    time.sleep(0.05)
        except KeyboardInterrupt:
            log.info("[SEMAFOROS] Detenido.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        self.pull_socket.close()
        self.ctx.term()


def main():
    parser = argparse.ArgumentParser(description="Control de Semáforos — PC2")
    parser.add_argument("--config", default="../PC1/config.json")
    args = parser.parse_args()
    cfg = cargar_config(args.config)
    ControlSemaforos(cfg).ejecutar()


if __name__ == "__main__":
    main()
