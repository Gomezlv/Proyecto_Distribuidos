import subprocess
import sys
import time
import argparse
import json
import os
import signal

DIRECTORIO = os.path.dirname(os.path.abspath(__file__))


def cargar_config(ruta):
    with open(ruta) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Lanzar todos los procesos de PC1")
    parser.add_argument("--multihilo", action="store_true")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    cfg      = cargar_config(args.config)
    sensores = cfg["sensores"]

    procesos = []

    # 1.Broker arranca primero
    broker_cmd = [sys.executable, os.path.join(DIRECTORIO, "broker.py"),
                  "--config", args.config]
    if args.multihilo:
        broker_cmd.append("--multihilo")
    p = subprocess.Popen(broker_cmd)
    procesos.append(("Broker", p))
    print(f"[PC1] Broker iniciado (PID {p.pid})")
    time.sleep(1)  # dar tiempo al broker para que haga bind

    # 2.Sensores uno por cada entrada en config.json
    sensor_scripts = {
        "camara":           "camara_sensor.py",
        "espira_inductiva": "espira_sensor.py",
        "gps":              "gps_sensor.py",
    }

    for s in sensores:
        script = sensor_scripts.get(s["tipo"])
        if not script:
            print(f"[PC1] Tipo desconocido: {s['tipo']} — omitido")
            continue
        cmd = [
            sys.executable,
            os.path.join(DIRECTORIO, script),
            "--id",           s["id"],
            "--interseccion", s["interseccion"],
            "--intervalo",    str(s["intervalo_seg"]),
            "--config",       args.config,
        ]
        p = subprocess.Popen(cmd)
        procesos.append((s["id"], p))
        print(f"[PC1] {s['id']} ({s['tipo']}) iniciado (PID {p.pid})")
        time.sleep(0.2)

    print(f"\n[PC1] {len(procesos)} procesos activos. Ctrl+C para detener.\n")

    def apagar(sig, frame):
        print("\n[PC1] Apagando todos los procesos...")
        for nombre, p in procesos:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT,  apagar)
    signal.signal(signal.SIGTERM, apagar)

    while True:
        for nombre, p in procesos:
            ret = p.poll()
            if ret is not None:
                print(f"[PC1] ADVERTENCIA: {nombre} terminó con código {ret}")
        time.sleep(5)


if __name__ == "__main__":
    main()