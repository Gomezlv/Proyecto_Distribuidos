# Checklist sustentación

## Antes de la demo

- [ ] `pip install -r requirements.txt` en las 3 máquinas
- [ ] Copiar `PC1/config.dist.json` → `config.json` con IPs reales
- [ ] Firewall: puertos 5551–5565, 5580–5581, 5590, 5599, 5570, 5600–5601 abiertos entre PCs
- [ ] Sin `venv/` en el zip de entrega

## Arranque (orden)

1. PC3: `cd PC3 && python lanzar_pc3.py`
2. PC2: `cd PC2 && python lanzar_pc2.py`
3. PC1: `cd PC1 && python lanzar_pc1.py`

## Funcionalidad a demostrar

- [ ] Logs de sensores, broker, analítica, semáforos (VERDE→ROJO)
- [ ] `python scripts/ver_estado_bd.py --veces 3`
- [ ] `python PC3/consulta_cli.py estado`
- [ ] `python PC3/consulta_cli.py historico --t-ini 2026-01-01T00:00:00Z --t-fin 2026-12-31T23:59:59Z`
- [ ] `python PC3/consulta_cli.py ambulancia --interseccion INT-B2`
- [ ] Apagar PC3 → `consulta_cli.py --host <PC2> --port 5601 estado`

## Preguntas preparadas

- REQ/REP en monitor puerto 5600 / failover 5601
- Health check: `health_monitor.py` → `db_main` REP 5570
- PUSH/PULL hacia semáforos (justificado vs PUB/SUB en informe 1)

## Entregables

- [ ] Video ≤ 10 min (guion en `docs/guion_video.md`)
- [ ] Informe rendimiento (`docs/informe_rendimiento.md` + gráficos)
- [ ] README actualizado
