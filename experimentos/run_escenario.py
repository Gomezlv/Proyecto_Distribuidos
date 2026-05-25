#!/usr/bin/env python3
"""Ejecuta un escenario de carga durante N segundos (E1 o E2)."""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="config_escenario_a.json o b")
    parser.add_argument("--duracion", type=int, default=120, help="segundos de corrida")
    parser.add_argument("--multihilo", action="store_true", help="E2 broker multihilo")
    parser.add_argument("--out-dir", default=os.path.join(ROOT, "experimentos", "out"))
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    config_path = args.config if os.path.isabs(args.config) else os.path.join(ROOT, "PC1", args.config)

    procs = []
    py = sys.executable

    def popen(cwd, script, extra=None):
        cmd = [py, script, "--config", config_path] + (extra or [])
        return subprocess.Popen(cmd, cwd=cwd)

    procs.append(("pc3", popen(os.path.join(ROOT, "PC3"), "lanzar_pc3.py")))
    time.sleep(2)
    procs.append(("pc2", popen(os.path.join(ROOT, "PC2"), "lanzar_pc2.py")))
    time.sleep(3)
    pc1_cmd = ["lanzar_pc1.py"]
    if args.multihilo:
        pc1_cmd = ["lanzar_pc1.py", "--multihilo"]
    p1 = subprocess.Popen(
        [py] + pc1_cmd + ["--config", config_path],
        cwd=os.path.join(ROOT, "PC1"),
    )
    procs.append(("pc1", p1))

    print(f"Corrida {args.duracion}s — config={config_path} multihilo={args.multihilo}")
    time.sleep(args.duracion)

    for nombre, p in procs:
        p.terminate()
        print(f"Detenido {nombre}")
    time.sleep(1)
    print("Listo.")


if __name__ == "__main__":
    main()
