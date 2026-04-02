import random
import argparse
import logging
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from sensor_base import SensorBase, cargar_config, ts_ahora


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)


class SensorEspira(SensorBase):

    INTERVALO_CONTEO = 30  #se puede cambiar

    def __init__(self, sensor_id: str, interseccion: str, intervalo_seg: int,
                 broker_host: str, broker_port: int,
                 veh_min=0, veh_max=50):
        super().__init__(
            sensor_id=sensor_id,
            tipo_sensor="espira_inductiva",
            interseccion=interseccion,
            intervalo_seg=intervalo_seg,
            broker_host=broker_host,
            broker_port=broker_port
        )
        self.veh_min = veh_min
        self.veh_max = veh_max

    def generar_evento(self) -> dict:

        ahora = datetime.now(timezone.utc)
        ts_inicio = (ahora - timedelta(seconds=self.INTERVALO_CONTEO)).strftime("%Y-%m-%dT%H:%M:%SZ")
        ts_fin    = ahora.strftime("%Y-%m-%dT%H:%M:%SZ")

        vehiculos = random.randint(self.veh_min, self.veh_max)

        return {
            "sensor_id":          self.sensor_id,
            "tipo_sensor":        self.tipo_sensor,
            "interseccion":       self.interseccion,
            "vehiculos_contados": vehiculos,
            "intervalo_segundos": self.INTERVALO_CONTEO,
            "timestamp_inicio":   ts_inicio,
            "timestamp_fin":      ts_fin
        }


def main():
    parser = argparse.ArgumentParser(description="Sensor Espira Inductiva")
    parser.add_argument("--id",           default=None)
    parser.add_argument("--interseccion", default=None)
    parser.add_argument("--intervalo",    type=int, default=None)
    parser.add_argument("--config",       default="config.json")
    args = parser.parse_args()

    cfg = cargar_config(args.config)
    red = cfg["red"]

    if args.id:
        match = next((s for s in cfg["sensores"] if s["id"] == args.id), None)
        if not match:
            logging.error(f"Sensor {args.id} no encontrado en config.")
            sys.exit(1)
        sensor = SensorEspira(
            sensor_id=match["id"],
            interseccion=args.interseccion or match["interseccion"],
            intervalo_seg=args.intervalo or match["intervalo_seg"],
            broker_host=red["pc1_ip"],
            broker_port=red["sensor_espira_port"]
        )
        sensor.ejecutar()
    else:
        import threading
        hilos = []
        for s in cfg["sensores"]:
            if s["tipo"] == "espira_inductiva":
                sensor = SensorEspira(
                    sensor_id=s["id"],
                    interseccion=s["interseccion"],
                    intervalo_seg=s["intervalo_seg"],
                    broker_host=red["pc1_ip"],
                    broker_port=red["sensor_espira_port"]
                )
                t = threading.Thread(target=sensor.ejecutar, daemon=True, name=s["id"])
                t.start()
                hilos.append(t)
        for h in hilos:
            h.join()


if __name__ == "__main__":
    main()