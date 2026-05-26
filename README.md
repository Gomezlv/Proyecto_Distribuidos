# Gestión Inteligente de Tráfico Urbano
Autores: Viviana Gómez, Sara Ocampo

Sistema distribuido con **ZeroMQ** en 3 PCs: sensores y broker (PC1), analítica/semáforos/réplica (PC2), BD principal y monitoreo (PC3).

## Requisitos

- Python 3.12+
- Dependencias:

```bash
pip install -r requirements.txt
```

## Configuración (3 máquinas)

1. Copiar plantilla y editar IPs:

```bash
cp PC1/config.dist.json PC1/config.json
```

2. Ajustar `pc1_ip`, `pc2_ip`, `pc3_ip` en `PC1/config.json`.

## Ejecución

**Orden obligatorio:** PC3 → PC2 → PC1.

```bash
# Terminal PC3
cd PC3
python lanzar_pc3.py

# Terminal PC2
cd PC2
python lanzar_pc2.py

# Terminal PC1
cd PC1
python lanzar_pc1.py
```

Broker multihilo (experimento E2):

```bash
cd PC1
python lanzar_pc1.py --multihilo
```

## Monitoreo (REQ/REP)

```bash
cd PC3
python consulta_cli.py estado
python consulta_cli.py historico --t-ini 2026-05-01T00:00:00Z --t-fin 2026-05-31T23:59:59Z
python consulta_cli.py interseccion --interseccion INT-A2
python consulta_cli.py priorizaciones
python consulta_cli.py ambulancia --interseccion INT-B2 --duracion 60
```

## Failover (PC3 completo caído)

Tras apagar PC3, el health check en PC2 marca la réplica como activa. Reconectar el cliente al monitor en PC2:

```bash
python PC3/consulta_cli.py --host <IP_PC2> --port 5601 estado
python PC3/consulta_cli.py --host <IP_PC2> --port 5601 ambulancia --interseccion INT-B2
```

Ver `scripts/demo_failover.sh` y `scripts/ver_estado_bd.py`.

## Experimentos de rendimiento

[experimentos/README.md](experimentos/README.md).

```bash
python experimentos/run_matriz.py
python experimentos/plot_resultados.py
```

## Tests

```bash
python -m pytest tests/ -v
```
