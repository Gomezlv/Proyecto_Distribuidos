#!/usr/bin/env bash
# Caso 3 — Monitoreo: ambulancia + consultas BD (≤ 4 min)
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
CONFIG="${ROOT}/PC1/config_sustent_caso3.json"
T_INI="2026-01-01T00:00:00Z"
T_FIN="2026-12-31T23:59:59Z"

echo "=== Caso 3: Monitoreo y consulta ==="
echo "Sistema en marcha con config_sustent_caso3.json (PC3→PC2→PC1)"
echo ""
read -r -p "Pulse Enter cuando el tráfico base esté estable (~1 min)..."

echo ""
echo "1) Priorizar ambulancia en Avenida-B2 (45s):"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" ambulancia \
  --interseccion INT-B2 --duracion 45

echo ""
echo "Observe logs: verde INT-B2, rojo en calles. Espere fin de priorización..."
sleep 10
read -r -p "Pulse Enter tras ver retorno a tráfico normal en logs..."

echo ""
echo "2) Consulta priorizaciones registradas:"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" priorizaciones

echo ""
echo "3) Consulta histórico (eventos en BD):"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" historico \
  --t-ini "$T_INI" --t-fin "$T_FIN"

echo ""
echo "=== Fin caso 3 ==="
