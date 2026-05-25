#!/usr/bin/env python3
"""Mide registros en BD durante un intervalo (variable dependiente 1)."""
from __future__ import annotations
import argparse
import csv
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.db_access import DatabaseStore


def contar(db_path: str) -> int:
    if not os.path.isfile(db_path):
        return 0
    store = DatabaseStore(db_path)
    n = store.contar_eventos()
    store.close()
    return n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--principal", default=os.path.join(ROOT, "PC3", "principal.db"))
    parser.add_argument("--replica", default=os.path.join(ROOT, "PC2", "replica.db"))
    parser.add_argument("--intervalo", type=int, default=120, help="segundos (2 min)")
    parser.add_argument("--muestras", type=int, default=6)
    parser.add_argument("--out", default=os.path.join(ROOT, "experimentos", "out", "medicion_bd.csv"))
    parser.add_argument("--etiqueta", default="run")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    paso = args.intervalo / max(args.muestras - 1, 1)
    filas = []

    for i in range(args.muestras):
        t = round(i * paso, 1)
        princ = contar(args.principal)
        rep = contar(args.replica)
        filas.append({
            "etiqueta": args.etiqueta,
            "t_seg": t,
            "eventos_principal": princ,
            "eventos_replica": rep,
        })
        print(f"t={t}s principal={princ} replica={rep}")
        if i < args.muestras - 1:
            time.sleep(paso)

    with open(args.out, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=filas[0].keys())
        if f.tell() == 0:
            w.writeheader()
        w.writerows(filas)
    print(f"Guardado {args.out}")


if __name__ == "__main__":
    main()
