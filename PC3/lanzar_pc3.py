from __future__ import annotations
import subprocess
import sys
import time
import argparse
import os
import signal


DIRECTORIO = os.path.dirname(os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser(description="Lanzar BD Principal (PC3)")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    procesos = []

    def lanzar(nombre, script):
        cmd = [sys.executable,
               os.path.join(DIRECTORIO, script),
               "--config", args.config]
        p = subprocess.Popen(cmd)
        procesos.append((nombre, p))
        print(f"[PC3] {nombre} iniciado (PID {p.pid})")
        return p

    lanzar("BD-Principal", "db_main.py")

    print("\n[PC3] BD corriendo. Ctrl+C para detener.")

    def apagar(sig, frame):
        print("\n[PC3] Apagando...")
        for _, p in procesos:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, apagar)
    signal.signal(signal.SIGTERM, apagar)

    while True:
        for nombre, p in procesos:
            if p.poll() is not None:
                print(f"[PC3] {nombre} terminó.")
        time.sleep(5)


if __name__ == "__main__":
    main()