#!/usr/bin/env bash
# Caso 4 — Tolerancia a fallos (PC3 caído, monitor en PC2:5601) ≤ 4 min
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
CONFIG="${ROOT}/PC1/config_sustent_caso4.json"

# Leer IP PC2 del config (requiere python)
PC2_IP="$("$PY" -c "
import json
with open('${CONFIG}') as f:
    print(json.load(f)['red']['pc2_ip'])
")"

echo "=== Caso 4: Tolerancia a fallos ==="
echo "Config: $CONFIG"
echo "PC2 (failover): $PC2_IP puerto 5601"
echo ""
echo "PREVIO: Arranque PC3 → PC2 → PC1 con config_sustent_caso4.json"
echo "        Deje correr ~1 min para generar datos."
echo ""

echo "--- Paso 1: Consulta con PC3 vivo (puerto 5600) ---"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" estado

echo ""
echo "--- Paso 2: APAGUE PC3 (Ctrl+C en lanzar_pc3.py en la máquina PC3) ---"
echo "        Espere ~15s (3 pings health × 5s). PC1 y PC2 deben seguir activos."
read -r -p "Pulse Enter cuando PC3 esté apagado y health haya marcado failover..."

echo ""
echo "--- Paso 3: Consultas al monitor failover en PC2 ---"
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" --host "$PC2_IP" --port 5601 estado
"$PY" "${ROOT}/PC3/consulta_cli.py" --config "$CONFIG" --host "$PC2_IP" --port 5601 \
  interseccion --interseccion INT-B2

echo ""
echo "--- Paso 4: BD — réplica sigue creciendo; principal congelada ---"
"$PY" "${ROOT}/scripts/ver_estado_bd.py" --veces 3

echo ""
echo "--- Paso 5 (opcional tras revivir PC3): sincronizar réplica → principal ---"
echo "$PY ${ROOT}/scripts/sync_replica_to_principal.py"
echo ""
echo "=== Fin caso 4 ==="
