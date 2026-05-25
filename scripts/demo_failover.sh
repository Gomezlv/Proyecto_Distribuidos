#!/usr/bin/env bash
# Demo: consulta en PC3 y tras apagar PC3 usar monitor failover en PC2 (puerto 5601).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
CONFIG="${ROOT}/PC1/config.json"

echo "=== Consulta normal (PC3:5600) ==="
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" estado

echo ""
echo "=== Simule apagado de PC3 (Ctrl+C en lanzar_pc3.py) y ejecute: ==="
echo "$PY ${ROOT}/PC3/consulta_cli.py --config $CONFIG --host 127.0.0.1 --port 5601 estado"
echo "$PY ${ROOT}/PC3/consulta_cli.py --config $CONFIG --host 127.0.0.1 --port 5601 ambulancia --interseccion INT-B2"
