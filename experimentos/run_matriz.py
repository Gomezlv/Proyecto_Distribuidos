#!/usr/bin/env python3
"""Ejecuta matriz E1/E2 x escenario A/B y genera CSV + gráficos."""
from __future__ import annotations
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
EXP = os.path.join(ROOT, "experimentos")


def run(cmd: list[str]) -> None:
    print(">>", " ".join(cmd))
    subprocess.run(cmd, check=False, cwd=ROOT)


def main():
    os.makedirs(os.path.join(EXP, "out"), exist_ok=True)
    matriz = [
        ("E1_A", "config_escenario_a.json", False),
        ("E2_A", "config_escenario_a.json", True),
        ("E1_B", "config_escenario_b.json", False),
        ("E2_B", "config_escenario_b.json", True),
    ]
    for etiqueta, cfg, multihilo in matriz:
        cfg_path = os.path.join(ROOT, "PC1", cfg)
        cmd_run = [PY, os.path.join(EXP, "run_escenario.py"), "--config", cfg_path, "--duracion", "90"]
        if multihilo:
            cmd_run.append("--multihilo")
        run(cmd_run)
        run([PY, os.path.join(EXP, "medir_bd.py"), "--etiqueta", etiqueta, "--intervalo", "90"])
        run([PY, os.path.join(EXP, "medir_latencia.py"), "--etiqueta", etiqueta, "--config", cfg_path])
    run([PY, os.path.join(EXP, "plot_resultados.py")])
    print("Matriz completada. Ver experimentos/out/")


if __name__ == "__main__":
    main()
