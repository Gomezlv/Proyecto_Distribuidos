#!/usr/bin/env python3
"""Mide latencia comando prioridad → respuesta analítica (variable dependiente 2)."""
from __future__ import annotations
import argparse
import csv
import json
import os
import sys
import time

import zmq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.config_loader import cargar_config
from common.protocol import OP_COMANDO_PRIORIDAD


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=os.path.join(ROOT, "PC1", "config.json"))
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--interseccion", default="INT-B2")
    parser.add_argument("--repeticiones", type=int, default=5)
    parser.add_argument("--out", default=os.path.join(ROOT, "experimentos", "out", "medicion_latencia.csv"))
    parser.add_argument("--etiqueta", default="run")
    args = parser.parse_args()

    cfg = cargar_config(args.config)
    host = args.host or cfg["red"]["pc3_ip"]
    port = args.port or cfg["red"]["monitor_rep_port"]
    token = cfg["seguridad"]["secret_key"]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    filas = []
    ctx = zmq.Context()

    for i in range(args.repeticiones):
        req = ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, 5000)
        req.connect(f"tcp://{host}:{port}")
        payload = {
            "operacion": OP_COMANDO_PRIORIDAD,
            "interseccion": args.interseccion,
            "duracion_seg": 45,
            "token": token,
        }
        t0 = time.time()
        req.send_json(payload)
        resp = req.recv_json()
        t1 = time.time()
        req.close()
        lat = t1 - t0
        lat_int = None
        if resp.get("ok") and resp.get("analitica"):
            lat_int = resp["analitica"].get("latencia_seg")
        filas.append({
            "etiqueta": args.etiqueta,
            "intento": i + 1,
            "latencia_total_seg": round(lat, 4),
            "latencia_analitica_seg": lat_int,
            "ok": resp.get("ok"),
        })
        print(f"intento {i+1}: {lat:.4f}s ok={resp.get('ok')}")
        time.sleep(1)

    ctx.term()
    write_header = not os.path.isfile(args.out)
    with open(args.out, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=filas[0].keys())
        if write_header:
            w.writeheader()
        w.writerows(filas)
    print(json.dumps({"promedio_seg": sum(r["latencia_total_seg"] for r in filas) / len(filas)}, indent=2))


if __name__ == "__main__":
    main()
