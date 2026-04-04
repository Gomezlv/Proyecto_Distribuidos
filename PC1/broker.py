"""
broker.py — Broker ZeroMQ para PC1.

Actúa como intermediario entre los sensores y el Servicio de Analítica (PC2).

Responsabilidades:
  1. Suscribirse a los 3 topics de sensores (espira_inductiva, camara, gps)
     en los puertos 5551, 5552 y 5553.
  2. Validar el formato mínimo de cada mensaje JSON recibido.
  3. Re-publicar los mensajes válidos en el puerto 5560 hacia la Analítica.

Diseños:
  - Diseño Base (single-thread): un único loop recv → validar → PUB.
  - Diseño Multihilos:           un hilo SUB por topic, todos comparten el
                                 mismo socket PUB (protegido con Lock).

Uso:
    python broker.py                 # diseño base (single-thread)
    python broker.py --multihilo     # diseño multihilo (E2)
    python broker.py --config otra_config.json
"""
from __future__ import annotations
import zmq
import json
import logging
import argparse
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from sensor_base import cargar_config   # reutilizamos la función de carga


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("BrokerZMQ")


# Campos mínimos requeridos por tipo de sensor
CAMPOS_REQUERIDOS = {
    "camara":           {"sensor_id", "tipo_sensor", "interseccion", "volumen", "velocidad_promedio"},
    "espira_inductiva": {"sensor_id", "tipo_sensor", "interseccion", "vehiculos_contados", "intervalo_segundos"},
    "gps":              {"sensor_id", "tipo_sensor", "interseccion", "nivel_congestion", "velocidad_promedio"},
}


def validar_mensaje(raw: bytes) -> dict | None:
    """
    Decodifica y valida que el JSON tenga los campos mínimos según su tipo.

    Returns:
        dict si el mensaje es válido, None si es inválido.
    """
    try:
        evento = json.loads(raw.decode())
        tipo   = evento.get("tipo_sensor", "")
        if tipo not in CAMPOS_REQUERIDOS:
            log.warning(f"[BROKER] tipo_sensor desconocido: {tipo!r} — descartado")
            return None
        faltantes = CAMPOS_REQUERIDOS[tipo] - evento.keys()
        if faltantes:
            log.warning(f"[BROKER] Campos faltantes {faltantes} en {tipo} — descartado")
            return None
        return evento
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        log.warning(f"[BROKER] JSON inválido: {e} — descartado")
        return None


# ===========================================================================
# Diseño Base — single-thread
# ===========================================================================

class BrokerZMQ:
    """
    Broker ZMQ en modo single-thread.

    Atributos:
        ctx            : zmq.Context
        sub_sockets    : list[zmq.Socket] — uno por sensor type/port
        pub_socket     : zmq.Socket — PUB hacia Analítica (PC2)
        topics_config  : list[tuple(topic, port)]
        usar_hilos     : bool — False en este diseño
        _activo        : bool — flag de control del loop
    """

    def __init__(self, cfg: dict):
        self.cfg        = cfg
        self.usar_hilos = False
        self._activo    = True

        red = cfg["red"]
        self.pc1_ip          = red["pc1_ip"]
        self.broker_pub_port = red["broker_pub_port"]

        self.topics_config = [
            ("espira_inductiva", red["sensor_espira_port"]),
            ("camara",           red["sensor_camara_port"]),
            ("gps",              red["sensor_gps_port"]),
        ]

        self.ctx        = zmq.Context()
        self.sub_sockets = []
        self.pub_socket  = None
        self._pub_lock   = threading.Lock()  # usado sólo en modo multihilo

    def inicializar(self) -> None:
        """Crea y configura los sockets SUB y PUB."""
        # Socket PUB → Analítica
        self.pub_socket = self.ctx.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.SNDHWM, 1000)
        pub_endpoint = f"tcp://0.0.0.0:{self.broker_pub_port}"
        self.pub_socket.bind(pub_endpoint)
        log.info(f"[BROKER] PUB escuchando en {pub_endpoint}")

        # Sockets SUB — uno por tipo de sensor
        for topic, port in self.topics_config:
            sub = self.ctx.socket(zmq.SUB)
            sub.setsockopt(zmq.RCVHWM, 1000)
            endpoint = f"tcp://0.0.0.0:{port}"
            sub.bind(endpoint)
            sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            self.sub_sockets.append((topic, sub))
            log.info(f"[BROKER] SUB '{topic}' en {endpoint}")

    def procesar_mensaje(self, topic_recv: bytes, raw: bytes) -> None:
        """Valida y re-publica un mensaje. Compartido por ambos diseños."""
        evento = validar_mensaje(raw)
        if evento is None:
            return
        payload = json.dumps(evento, ensure_ascii=False).encode()
        topic_b = topic_recv if isinstance(topic_recv, bytes) else topic_recv.encode()
        with self._pub_lock:
            self.pub_socket.send_multipart([topic_b, payload])
        log.info(f"[BROKER] Reenviado → {topic_b.decode()} | {evento.get('sensor_id')}")

    def ejecutar(self) -> None:
        """Loop single-thread usando zmq.Poller para multiplexar los SUBs."""
        self.inicializar()
        poller = zmq.Poller()
        for _, sub in self.sub_sockets:
            poller.register(sub, zmq.POLLIN)

        log.info("[BROKER] Iniciado en modo single-thread. Esperando mensajes...")
        try:
            while self._activo:
                eventos = dict(poller.poll(timeout=1000))  # 1s timeout
                for topic_str, sub in self.sub_sockets:
                    if sub in eventos:
                        partes = sub.recv_multipart()
                        if len(partes) == 2:
                            self.procesar_mensaje(partes[0], partes[1])
        except KeyboardInterrupt:
            log.info("[BROKER] Detenido por usuario.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        for _, sub in self.sub_sockets:
            sub.close()
        if self.pub_socket:
            self.pub_socket.close()
        self.ctx.term()
        log.info("[BROKER] Recursos ZMQ liberados.")


# ===========================================================================
# Diseño Multihilos — un hilo por topic (E2)
# ===========================================================================

class BrokerZMQMultihilo(BrokerZMQ):
    """
    Broker ZMQ multihilo: un hilo receptor por cada topic/sensor.
    Todos comparten el mismo socket PUB protegido con un Lock.
    Este es el diseño alternativo para comparar rendimiento en E2.
    """

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.usar_hilos = True
        self._hilos: list[threading.Thread] = []

    def _worker_topic(self, topic_str: str, sub: zmq.Socket) -> None:
        """Worker de hilo: recibe de un SUB específico y re-publica."""
        log.info(f"[BROKER-MT] Hilo para topic '{topic_str}' iniciado.")
        try:
            while self._activo:
                try:
                    partes = sub.recv_multipart(flags=zmq.NOBLOCK)
                    if len(partes) == 2:
                        self.procesar_mensaje(partes[0], partes[1])
                except zmq.Again:
                    threading.Event().wait(0.01)  # sleep breve sin bloquear
        except Exception as e:
            log.error(f"[BROKER-MT] Error en hilo {topic_str}: {e}")

    def ejecutar_multihilo(self) -> None:
        """Lanza un hilo por SUB topic y espera a que terminen."""
        self.inicializar()
        log.info("[BROKER-MT] Iniciado en modo MULTIHILO.")
        for topic_str, sub in self.sub_sockets:
            t = threading.Thread(
                target=self._worker_topic,
                args=(topic_str, sub),
                daemon=True,
                name=f"broker-{topic_str}"
            )
            t.start()
            self._hilos.append(t)
        try:
            for t in self._hilos:
                t.join()
        except KeyboardInterrupt:
            log.info("[BROKER-MT] Detenido por usuario.")
        finally:
            self.cerrar()


# ===========================================================================
# Entry point
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Broker ZMQ — PC1")
    parser.add_argument("--multihilo", action="store_true",
                        help="Usar diseño multihilo (1 hilo por topic)")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    cfg = cargar_config(args.config)

    if args.multihilo:
        log.info("=== BROKER ZMQ — MODO MULTIHILO ===")
        broker = BrokerZMQMultihilo(cfg)
        broker.ejecutar_multihilo()
    else:
        log.info("=== BROKER ZMQ — MODO SINGLE-THREAD ===")
        broker = BrokerZMQ(cfg)
        broker.ejecutar()


if __name__ == "__main__":
    main()