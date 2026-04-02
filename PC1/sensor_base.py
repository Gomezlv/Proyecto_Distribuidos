import zmq
import json
import time
import logging
import threading
from datetime import datetime, timezone
from abc import ABC, abstractmethod


def cargar_config(ruta="config.json"):
    with open(ruta, "r") as f:
        return json.load(f)


def ts_ahora():
    """Timestamp ISO 8601 en UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SensorBase(ABC):

    def __init__(self, sensor_id: str, tipo_sensor: str, interseccion: str,
                 intervalo_seg: int, broker_host: str, broker_port: int):
        self.sensor_id     = sensor_id
        self.tipo_sensor   = tipo_sensor
        self.interseccion  = interseccion
        self.intervalo_seg = intervalo_seg
        self.topic         = tipo_sensor          # 'camara', 'espira_inductiva', 'gps'
        self._activo       = True
        self._lock         = threading.Lock()

        # Logger con nombre del sensor
        self.log = logging.getLogger(self.sensor_id)

        # Configurar socket ZMQ PUB
        self.ctx        = zmq.Context()
        self.socket_pub = self.ctx.socket(zmq.PUB)
        self.socket_pub.setsockopt(zmq.LINGER, 2000)   # espera 2s al cerrar
        self.socket_pub.setsockopt(zmq.SNDHWM, 1000)   # high-water mark
        endpoint = f"tcp://{broker_host}:{broker_port}"
        self.socket_pub.connect(endpoint)

        # Pausa inicial para que ZMQ complete la conexión (slow-joiner)
        time.sleep(0.5)
        self.log.info(f"[{self.sensor_id}] Conectado a Broker en {endpoint}")

    @abstractmethod
    def generar_evento(self) -> dict:
        ...

    def publicar(self, evento: dict) -> None:

        payload = json.dumps(evento, ensure_ascii=False)
        self.socket_pub.send_multipart([
            self.topic.encode(),
            payload.encode()
        ])
        self.log.info(f"[PUB] {self.sensor_id} → {self.topic} | {payload}")

    def ejecutar(self) -> None:

        self.log.info(
            f"[{self.sensor_id}] Iniciando en {self.interseccion} "
            f"| Intervalo: {self.intervalo_seg}s"
        )
        try:
            while self._activo:
                evento = self.generar_evento()
                self.publicar(evento)
                time.sleep(self.intervalo_seg)
        except KeyboardInterrupt:
            self.log.info(f"[{self.sensor_id}] Detenido por usuario.")
        finally:
            self.detener()

    def detener(self) -> None:
        with self._lock:
            self._activo = False
        self.socket_pub.close()
        self.ctx.term()
        self.log.info(f"[{self.sensor_id}] Recursos ZMQ liberados.")