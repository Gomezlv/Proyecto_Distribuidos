import zmq
import json
import time
import logging
import threading
import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'PC1'))
from sensor_base import cargar_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("CtrlSemaforos")

T_WATCHDOG_DEFAULT = 60   # segundos sin comandos antes de modo seguro



class Semaforo:

    def __init__(self, interseccion: str):
        self.interseccion = interseccion
        self.estado       = "ROJO"
        self.duracion_seg = 15
        self.ts_cambio    = time.time()

    def cambiar_estado(self, nuevo_estado: str, duracion: int) -> None:
        if nuevo_estado not in ("VERDE", "ROJO"):
            log.warning(f"[{self.interseccion}] Estado inválido: {nuevo_estado!r}")
            return
        anterior = self.estado
        self.estado       = nuevo_estado
        self.duracion_seg = duracion
        self.ts_cambio    = time.time()
        log.info(
            f"[SEMAFORO] {self.interseccion} | "
            f"{anterior} → {nuevo_estado} "
            f"| Duración: {duracion}s"
        )

    def tiempo_restante(self) -> float:
        transcurrido = time.time() - self.ts_cambio
        return max(0.0, self.duracion_seg - transcurrido)

    def to_dict(self) -> dict:
        return {
            "interseccion":   self.interseccion,
            "estado":         self.estado,
            "duracion_seg":   self.duracion_seg,
            "tiempo_restante": round(self.tiempo_restante(), 1),
            "ts_cambio":      self.ts_cambio,
        }

    def __repr__(self):
        return (f"Semaforo({self.interseccion}, {self.estado}, "
                f"{self.duracion_seg}s, restante={self.tiempo_restante():.1f}s)")


class ControlSemaforos:

    def __init__(self, cfg: dict):
        self.cfg        = cfg
        red             = cfg["red"]
        self.secret_key = cfg["seguridad"]["secret_key"]
        self.t_watchdog = T_WATCHDOG_DEFAULT
        self._activo    = True
        self.ultimo_cmd_ts = time.time()

        # Inicializar semáforos desde config
        self.semaforos: dict[str, Semaforo] = {
            intersec: Semaforo(intersec)
            for intersec in cfg.get("semaforos", [])
        }
        log.info(f"[SEMAFOROS] {len(self.semaforos)} semáforos inicializados: "
                 f"{list(self.semaforos.keys())}")

        # Socket PULL
        self.ctx         = zmq.Context()
        self.pull_socket = self.ctx.socket(zmq.PULL)
        self.pull_socket.setsockopt(zmq.RCVHWM, 1000)
        endpoint = f"tcp://0.0.0.0:{red['semaf_port']}"
        self.pull_socket.bind(endpoint)
        log.info(f"[SEMAFOROS] PULL escuchando en {endpoint}")

    def _validar_token(self, cmd: dict) -> bool:
        token = cmd.get("token", "")
        if token != self.secret_key:
            log.warning(
                f"[SEMAFOROS] TOKEN INVÁLIDO en comando para {cmd.get('interseccion')} "
                f"— token recibido: {token!r}"
            )
            return False
        return True

    def _procesar_comando(self, raw: bytes) -> None:
        try:
            cmd = json.loads(raw.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning(f"[SEMAFOROS] Comando JSON inválido: {e}")
            return

        if not self._validar_token(cmd):
            return

        interseccion = cmd.get("interseccion")
        estado       = cmd.get("estado")
        duracion     = cmd.get("duracion_seg", 15)
        motivo       = cmd.get("motivo", "sin_motivo")

        if interseccion not in self.semaforos:
            # Crear semáforo dinámicamente si no estaba en config
            self.semaforos[interseccion] = Semaforo(interseccion)
            log.info(f"[SEMAFOROS] Nuevo semáforo creado: {interseccion}")

        self.semaforos[interseccion].cambiar_estado(estado, duracion)
        self.ultimo_cmd_ts = time.time()

        log.info(
            f"[SEMAFOROS] Ejecutado | {interseccion} → {estado} "
            f"| {duracion}s | Motivo: {motivo}"
        )

    def _modo_seguro(self) -> None:

        log.warning(
            f"[SEMAFOROS] WATCHDOG: sin comandos por {self.t_watchdog}s "
            "— activando MODO SEGURO (ciclo fijo 15s)"
        )
        for sem in self.semaforos.values():
            sem.cambiar_estado("VERDE", 15)
        self.ultimo_cmd_ts = time.time()   # resetea el watchdog

    def _hilo_watchdog(self) -> None:
        while self._activo:
            time.sleep(5)
            if time.time() - self.ultimo_cmd_ts > self.t_watchdog:
                self._modo_seguro()

    def imprimir_estado(self) -> None:
        print("\n--- Estado actual de semáforos ---")
        for sem in self.semaforos.values():
            print(f"  {sem}")
        print("-----------------------------------\n")

    def ejecutar(self) -> None:
        hilo_wd = threading.Thread(
            target=self._hilo_watchdog, daemon=True, name="watchdog"
        )
        hilo_wd.start()

        log.info("[SEMAFOROS] Servicio de Control iniciado. Esperando comandos...")
        try:
            while self._activo:
                try:
                    raw = self.pull_socket.recv(flags=zmq.NOBLOCK)
                    self._procesar_comando(raw)
                except zmq.Again:
                    time.sleep(0.05)
        except KeyboardInterrupt:
            log.info("[SEMAFOROS] Detenido por usuario.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        self.pull_socket.close()
        self.ctx.term()
        log.info("[SEMAFOROS] Recursos ZMQ liberados.")
        self.imprimir_estado()

def main():
    parser = argparse.ArgumentParser(description="Control de Semáforos — PC2")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    cfg     = cargar_config(args.config)
    servicio = ControlSemaforos(cfg)
    servicio.ejecutar()


if __name__ == "__main__":
    main()