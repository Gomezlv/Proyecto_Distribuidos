#!/usr/bin/env bash
# Caso 2 — Congestión en avenida B (≤ 4 min)
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
CONFIG="${ROOT}/PC1/config_sustent_caso2.json"

echo "=== Caso 2: Congestión en avenida ==="
echo "Config: $CONFIG"
echo ""
echo "Arranque: PC3 → PC2 → PC1 con config_sustent_caso2.json"
echo ""
echo "Observe en logs PC2 (analítica/semáforos):"
echo "  - [CONGESTION] INT-B2 / INT-B4 -> verde 30s"
echo "  - [BLOQUEO] INT-A2 / INT-C3 -> ROJO 30s"
echo ""
read -r -p "Pulse Enter tras ~2 min de observación..."

echo ""
echo "Consulta calle (debe mostrar estados recientes):"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" interseccion --interseccion INT-A2

echo ""
echo "Estado BD:"
"$PY" "${ROOT}/scripts/ver_estado_bd.py" --veces 2

echo "=== Fin caso 2 ==="
