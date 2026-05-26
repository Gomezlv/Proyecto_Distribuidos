# Sustentación — Caso 4: Tolerancia a fallos

Demuestra que al **caer PC3** (BD principal + monitor), el sistema sigue operando: sensores publican, analítica y semáforos en PC2 continúan, y las **consultas** se atienden por el **monitor failover** en PC2 (puerto **5601**).

## Objetivo

- Provocar falla de PC3 (`Ctrl+C` en `lanzar_pc3.py`).
- Consultar estado e intersección vía `--host <IP_PC2> --port 5601`.
- Mostrar que la **réplica** en PC2 acumula operaciones mientras la principal queda detenida.

## Requisitos

- 3 PCs con IPs **distintas** en [`PC1/config_sustent_caso4.json`](PC1/config_sustent_caso4.json):
  - `pc1_ip` → máquina sensores
  - `pc2_ip` → analítica, semáforos, réplica, health, monitor failover
  - `pc3_ip` → BD principal, monitor
- Puertos 5570 (health), 5600, 5601 abiertos entre PCs.

## Ejecución

```bash
# PC3
cd PC3 && python lanzar_pc3.py --config ../PC1/config_sustent_caso4.json

# PC2
cd PC2 && python lanzar_pc2.py --config ../PC1/config_sustent_caso4.json

# PC1
cd PC1 && python lanzar_pc1.py --config config_sustent_caso4.json
```

## Cómo probar (failover)

```bash
# Con PC3 activo
python PC3/consulta_cli.py --config PC1/config_sustent_caso4.json estado

# Tras apagar PC3 (~15 s)
python PC3/consulta_cli.py --config PC1/config_sustent_caso4.json \
  --host 192.168.1.20 --port 5601 estado

python scripts/ver_estado_bd.py --veces 3
```

Guion paso a paso: `bash scripts/demo_caso4.sh`

## Flujo de demostración (≤ 4 min)

| Paso | Acción |
|------|--------|
| 1 | Sistema completo ~1 min |
| 2 | `consulta_cli estado` → PC3 OK |
| 3 | Apagar PC3; logs `[HEALTH] PC3 no disponible` en PC2 |
| 4 | `consulta_cli --host PC2 --port 5601 estado` + `interseccion` |
| 5 | `ver_estado_bd.py`: réplica ↑, principal sin nuevas filas |

## Resultado esperado

- Health check marca `pc3_activo = False` tras 3 fallos.
- Analítica loguea `solo réplica` al persistir.
- Consultas REQ/REP responden en puerto 5601.
- Sensores en PC1 siguen publicando (ver logs broker/analítica).

## Errores comunes

| Problema | Solución |
|----------|----------|
| `pc2_ip == pc3_ip` | Editar config; el loader emite warning |
| Timeout en 5601 | `monitor_failover.py` debe estar en `lanzar_pc2` |
| Réplica no crece | PC2 `db_replica.py` activo; analítica conectada |
| Consulta OK pero sin datos nuevos | Normal si PC3 murió antes de eventos; dejar PC1 corriendo |

## Recuperación

```bash
python scripts/sync_replica_to_principal.py
```

Ver [EXPLICACION_TECNICA.md](EXPLICACION_TECNICA.md).
