"""Cliente REQ de demostración para monitoreo (PC3 o failover PC2)."""
from __future__ import annotations
import argparse
import json
import sys

import zmq

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import cargar_config
from common.protocol import (
    OP_COMANDO_PRIORIDAD,
    OP_ESTADO_BD,
    OP_QUERY_HISTORICO,
    OP_QUERY_INTERSECCION,
    OP_QUERY_PRIORIZACIONES,
)


def enviar(host: str, port: int, payload: dict, timeout_ms: int = 5000) -> dict:
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, timeout_ms)
    req.setsockopt(zmq.SNDTIMEO, timeout_ms)
    req.connect(f"tcp://{host}:{port}")
    req.send_json(payload)
    try:
        resp = req.recv_json()
        return resp
    finally:
        req.close()
        ctx.term()


def main():
    parser = argparse.ArgumentParser(description="CLI consulta monitoreo")
    parser.add_argument("--config", default="../PC1/config.json")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument(
        "comando",
        choices=["estado", "historico", "interseccion", "priorizaciones", "ambulancia"],
    )
    parser.add_argument("--t-ini", default="2026-01-01T00:00:00Z")
    parser.add_argument("--t-fin", default="2026-12-31T23:59:59Z")
    parser.add_argument("--interseccion", default="INT-B2")
    parser.add_argument("--duracion", type=int, default=60)
    args = parser.parse_args()

    cfg = cargar_config(args.config)
    red = cfg["red"]
    host = args.host or red["pc3_ip"]
    port = args.port or red["monitor_rep_port"]
    token = cfg["seguridad"]["secret_key"]

    if args.comando == "estado":
        payload = {"operacion": OP_ESTADO_BD}
    elif args.comando == "historico":
        payload = {"operacion": OP_QUERY_HISTORICO, "t_ini": args.t_ini, "t_fin": args.t_fin}
    elif args.comando == "interseccion":
        payload = {"operacion": OP_QUERY_INTERSECCION, "interseccion": args.interseccion}
    elif args.comando == "priorizaciones":
        payload = {"operacion": OP_QUERY_PRIORIZACIONES}
    else:
        payload = {
            "operacion": OP_COMANDO_PRIORIDAD,
            "interseccion": args.interseccion,
            "duracion_seg": args.duracion,
            "token": token,
        }

    print(f"Conectando a tcp://{host}:{port} ...")
    resp = enviar(host, port, payload)
    print(json.dumps(resp, indent=2, ensure_ascii=False))
    sys.exit(0 if resp.get("ok") else 1)


if __name__ == "__main__":
    main()
