# Guion video (máx. 10 minutos)

## 0:00–2:00 — Distribución en 3 máquinas

- Diagrama PC1 (sensores + broker), PC2 (analítica, semáforos, réplica, health, monitor failover), PC3 (BD + monitor).
- Mostrar `PC1/config.dist.json` con IPs reales.
- Orden de arranque: `lanzar_pc3.py` → `lanzar_pc2.py` → `lanzar_pc1.py`.

## 2:00–4:00 — Parámetros y cuadrícula

- `config.json`: ciudad 4×5, lista de sensores y semáforos.
- Explicar asignación: cada sensor en una intersección; semáforos en INT-A2, etc.
- Intervalos por tipo (cámara 10s, espira 30s, GPS 15s).

## 4:00–6:00 — Patrones ZeroMQ

- Logs: PUB/SUB sensores→broker→analítica.
- PUSH/PULL analítica→BD y semáforos.
- REQ/REP monitor↔cliente y monitor→analítica (prioridad).

## 6:00–8:00 — Consultas y ambulancia

- `python PC3/consulta_cli.py historico --t-ini ... --t-fin ...`
- `python PC3/consulta_cli.py interseccion --interseccion INT-B4`
- `python PC3/consulta_cli.py ambulancia --interseccion INT-B2`
- `scripts/ver_estado_bd.py` en paralelo.

## 8:00–10:00 — Falla PC3 y failover

- Apagar procesos PC3.
- Health detecta fallo; analítica solo escribe réplica.
- `consulta_cli.py --host PC2_IP --port 5601 estado`
- Comparar conteos réplica vs principal; mencionar `sync_replica_to_principal.py` opcional.
