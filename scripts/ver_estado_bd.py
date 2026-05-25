#!/usr/bin/env python3
"""Muestra conteos de BD principal y réplica lado a lado."""
from __future__ import annotations
import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.db_access import DatabaseStore


def resumen(path: str) -> dict | None:
    if not os.path.isfile(path):
        return None
    s = DatabaseStore(path)
    r = s.resumen_tablas()
    s.close()
    return r


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--principal", default=os.path.join(ROOT, "PC3", "principal.db"))
    parser.add_argument("--replica", default=os.path.join(ROOT, "PC2", "replica.db"))
    parser.add_argument("--intervalo", type=float, default=5.0)
    parser.add_argument("--veces", type=int, default=0, help="0 = infinito")
    args = parser.parse_args()

    n = 0
    try:
        while args.veces == 0 or n < args.veces:
            p = resumen(args.principal)
            r = resumen(args.replica)
            print("=" * 60)
            print(f"Principal ({args.principal}):")
            if p:
                for t, info in p.items():
                    print(f"  {t}: {info['filas']} (último {info['ultimo']})")
            else:
                print("  (no existe)")
            print(f"Réplica ({args.replica}):")
            if r:
                for t, info in r.items():
                    print(f"  {t}: {info['filas']} (último {info['ultimo']})")
            else:
                print("  (no existe)")
            n += 1
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\nFin.")


if __name__ == "__main__":
    main()
