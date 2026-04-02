import random
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from sensor_base import SensorBase, cargar_config, ts_ahora


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)


def calcular_nivel_congestion(vp: float) -> str:
# segui la misma regla del enunciado, se puede cambiar según veamos
    if vp < 10:
        return "ALTA"
    elif vp <= 39:
        return "NORMAL"
    else:
        return "BAJA"


class SensorGPS(SensorBase):

    def __init__(self, sensor_id: str, interseccion: str, intervalo_seg: int,
                 broker_host: str, broker_port: int,
                 vp_min=5, vp_max=60):
        super().__init__(
            sensor_id=sensor_id,
            tipo_sensor="gps",
            interseccion=interseccion,
            intervalo_seg=intervalo_seg,
            broker_host=broker_host,
            broker_port=broker_port
        )
        self.vp_min = vp_min
        self.vp_max = vp_max

    def generar_evento(self) -> dict:

        vp = round(random.uniform(self.vp_min, self.vp_max), 1)
        nivel = calcular_nivel_congestion(vp)

        return {
            "sensor_id":          self.sensor_id,
            "tipo_sensor":        self.tipo_sensor,
            "interseccion":       self.interseccion,
            "nivel_congestion":   nivel,
            "velocidad_promedio": vp,
            "timestamp":          ts_ahora()
        }


def main():
    parser = argparse.ArgumentParser(description="Sensor GPS de Tráfico")
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
        sensor = SensorGPS(
            sensor_id=match["id"],
            interseccion=args.interseccion or match["interseccion"],
            intervalo_seg=args.intervalo or match["intervalo_seg"],
            broker_host=red["pc1_ip"],
            broker_port=red["sensor_gps_port"]
        )
        sensor.ejecutar()
    else:
        import threading
        hilos = []
        for s in cfg["sensores"]:
            if s["tipo"] == "gps":
                sensor = SensorGPS(
                    sensor_id=s["id"],
                    interseccion=s["interseccion"],
                    intervalo_seg=s["intervalo_seg"],
                    broker_host=red["pc1_ip"],
                    broker_port=red["sensor_gps_port"]
                )
                t = threading.Thread(target=sensor.ejecutar, daemon=True, name=s["id"])
                t.start()
                hilos.append(t)
        for h in hilos:
            h.join()


if __name__ == "__main__":
    main()