#!/usr/bin/env bash
# Caso 1 — Tráfico normal (≤ 4 min). Ejecutar con el sistema ya en marcha.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
CONFIG="${ROOT}/PC1/config_sustent_caso1.json"

echo "=== Caso 1: Tráfico normal ==="
echo "Config: $CONFIG"
echo ""
echo "1) Arranque (en 3 PCs, orden PC3 → PC2 → PC1):"
echo "   cd PC3 && $PY lanzar_pc3.py --config ../PC1/config_sustent_caso1.json"
echo "   cd PC2 && $PY lanzar_pc2.py --config ../PC1/config_sustent_caso1.json"
echo "   cd PC1 && $PY lanzar_pc1.py --config config_sustent_caso1.json"
echo ""
echo "2) Observe logs [SEMAFORO] VERDE→ROJO (15s) y rojos en calles al verde de avenida."
echo "   Espere ~2 minutos..."
read -r -p "   Pulse Enter cuando haya visto ciclos normales..."

echo ""
echo "3) Consulta intersección:"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" interseccion --interseccion INT-B2

echo ""
echo "4) Estado BD principal y réplica:"
"$PY" "${ROOT}/scripts/ver_estado_bd.py" --veces 3

echo ""
echo "=== Fin caso 1 ==="
