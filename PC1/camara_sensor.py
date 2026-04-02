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


class SensorCamara(SensorBase):

    def __init__(self, sensor_id: str, interseccion: str, intervalo_seg: int,
                 broker_host: str, broker_port: int,
                 vol_min=0, vol_max=30, vp_min=5, vp_max=60):
        super().__init__(
            sensor_id=sensor_id,
            tipo_sensor="camara",
            interseccion=interseccion,
            intervalo_seg=intervalo_seg,
            broker_host=broker_host,
            broker_port=broker_port
        )
        self.vol_min = vol_min
        self.vol_max = vol_max
        self.vp_min  = vp_min
        self.vp_max  = vp_max

    def generar_evento(self) -> dict:

        volumen = random.randint(self.vol_min, self.vol_max)
        # Correlación leve: más volumen → menos velocidad
        vp_max_ajustado = max(self.vp_min + 2, self.vp_max - volumen)
        velocidad = round(random.uniform(self.vp_min, vp_max_ajustado), 1)

        return {
            "sensor_id":          self.sensor_id,
            "tipo_sensor":        self.tipo_sensor,
            "interseccion":       self.interseccion,
            "volumen":            volumen,
            "velocidad_promedio": velocidad,
            "timestamp":          ts_ahora()
        }


def main():
    parser = argparse.ArgumentParser(description="Sensor Cámara de Tráfico")
    parser.add_argument("--id",           default=None,  help="ID del sensor, ej: CAM-A2")
    parser.add_argument("--interseccion", default=None,  help="Intersección, ej: INT-A2")
    parser.add_argument("--intervalo",    type=int, default=None, help="Segundos entre eventos")
    parser.add_argument("--config",       default="config.json")
    args = parser.parse_args()

    cfg = cargar_config(args.config)
    red = cfg["red"]

    # Si se pasa --id, buscar en config; si no, lanzar el primer sensor de tipo camara
    if args.id:
        match = next((s for s in cfg["sensores"] if s["id"] == args.id), None)
        if not match:
            logging.error(f"Sensor {args.id} no encontrado en config.")
            sys.exit(1)
        sid          = match["id"]
        interseccion = args.interseccion or match["interseccion"]
        intervalo    = args.intervalo or match["intervalo_seg"]
    else:
        # Lanza todos los sensores de tipo cámara definidos en config
        import threading
        hilos = []
        for s in cfg["sensores"]:
            if s["tipo"] == "camara":
                sensor = SensorCamara(
                    sensor_id=s["id"],
                    interseccion=s["interseccion"],
                    intervalo_seg=s["intervalo_seg"],
                    broker_host=red["pc1_ip"],
                    broker_port=red["sensor_camara_port"]
                )
                t = threading.Thread(target=sensor.ejecutar, daemon=True, name=s["id"])
                t.start()
                hilos.append(t)
        for h in hilos:
            h.join()
        return

    sensor = SensorCamara(
        sensor_id=sid,
        interseccion=interseccion,
        intervalo_seg=intervalo,
        broker_host=red["pc1_ip"],
        broker_port=red["sensor_camara_port"]
    )
    sensor.ejecutar()


if __name__ == "__main__":
    main()