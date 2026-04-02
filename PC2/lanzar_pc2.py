import subprocess
import sys
import time
import argparse
import os
import signal

DIRECTORIO = os.path.dirname(os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser(description="Lanzar procesos de PC2")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    procesos = []

    def lanzar(nombre, script):
        cmd = [sys.executable,
               os.path.join(DIRECTORIO, script),
               "--config", args.config]
        p = subprocess.Popen(cmd)
        procesos.append((nombre, p))
        print(f"[PC2] {nombre} iniciado (PID {p.pid})")
        return p

    # 1. BD Replica — hace bind primero
    lanzar("BD-Replica",    "db_replica.py")
    time.sleep(0.8)

    # 2. Control Semaforos — hace bind
    lanzar("CtrlSemaforos", "semaforos.py")
    time.sleep(0.8)

    # 3. Analitica — conecta a todos los anteriores
    lanzar("Analitica",     "analitica.py")

    print(f"\n[PC2] {len(procesos)} procesos activos. Ctrl+C para detener.\n")

    def apagar(sig, frame):
        print("\n[PC2] Apagando todos los procesos...")
        for nombre, p in procesos:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT,  apagar)
    signal.signal(signal.SIGTERM, apagar)

    while True:
        for nombre, p in procesos:
            if p.poll() is not None:
                print(f"[PC2] ADVERTENCIA: {nombre} termino inesperadamente.")
        time.sleep(5)


if __name__ == "__main__":
    main()
