#!/usr/bin/env python3
"""Genera gráficos desde CSV de experimentos."""
from __future__ import annotations
import argparse
import csv
import os
from collections import defaultdict

import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def leer_csv(path: str) -> list[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bd-csv", default=os.path.join(ROOT, "experimentos", "out", "medicion_bd.csv"))
    parser.add_argument("--lat-csv", default=os.path.join(ROOT, "experimentos", "out", "medicion_latencia.csv"))
    parser.add_argument("--out-dir", default=os.path.join(ROOT, "experimentos", "out"))
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    bd = leer_csv(args.bd_csv)
    if bd:
        por_etiqueta = defaultdict(lambda: {"t": [], "p": [], "r": []})
        for row in bd:
            e = row["etiqueta"]
            por_etiqueta[e]["t"].append(float(row["t_seg"]))
            por_etiqueta[e]["p"].append(int(row["eventos_principal"]))
            por_etiqueta[e]["r"].append(int(row["eventos_replica"]))
        fig, ax = plt.subplots(figsize=(8, 5))
        for etiqueta, d in por_etiqueta.items():
            ax.plot(d["t"], d["r"], marker="o", label=f"{etiqueta} réplica")
        ax.set_xlabel("Tiempo (s)")
        ax.set_ylabel("Eventos almacenados")
        ax.set_title("Registros en BD réplica (intervalo 2 min)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        out = os.path.join(args.out_dir, "grafico_eventos_bd.png")
        fig.savefig(out, dpi=120, bbox_inches="tight")
        plt.close(fig)
        print(f"Generado {out}")

    lat = leer_csv(args.lat_csv)
    if lat:
        por_etiqueta = defaultdict(list)
        for row in lat:
            if row.get("ok") in ("True", "true", True, "1"):
                por_etiqueta[row["etiqueta"]].append(float(row["latencia_total_seg"]))
        if por_etiqueta:
            fig, ax = plt.subplots(figsize=(7, 5))
            labels = list(por_etiqueta.keys())
            vals = [sum(v) / len(v) for v in por_etiqueta.values()]
            ax.bar(labels, vals, color=["#2ecc71", "#3498db", "#e74c3c", "#9b59b6"][: len(labels)])
            ax.set_ylabel("Latencia promedio (s)")
            ax.set_title("Tiempo solicitud usuario → respuesta semáforo")
            out = os.path.join(args.out_dir, "grafico_latencia.png")
            fig.savefig(out, dpi=120, bbox_inches="tight")
            plt.close(fig)
            print(f"Generado {out}")


if __name__ == "__main__":
    main()
