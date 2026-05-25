#!/usr/bin/env python3
"""Copia eventos de réplica a principal tras recuperar PC3 (backfill opcional)."""
from __future__ import annotations
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.db_access import DatabaseStore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--replica", default=os.path.join(ROOT, "PC2", "replica.db"))
    parser.add_argument("--principal", default=os.path.join(ROOT, "PC3", "principal.db"))
    args = parser.parse_args()

    rep = DatabaseStore(args.replica)
    princ = DatabaseStore(args.principal)
    eventos = rep.query_historico("1970-01-01T00:00:00Z", "2099-12-31T23:59:59Z")
    n = 0
    for ev in eventos:
        ev["tipo_msg"] = "evento"
        princ.insertar_evento(ev)
        n += 1
    rep.close()
    princ.close()
    print(f"Backfill: {n} eventos copiados a principal.")


if __name__ == "__main__":
    main()
