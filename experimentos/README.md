# Experimentos de rendimiento (Tabla 1)

## Variables

| Tipo | Variable |
|------|----------|
| Independiente | Diseño broker (E1 single-thread / E2 multihilo) |
| Independiente | Escenario A (1 sensor/tipo, 10s) / B (6 sensores, 5s) |
| Dependiente 1 | Eventos en `eventos_sensores` en ventana de 2 min |
| Dependiente 2 | Latencia comando `ambulancia` → respuesta analítica |

## Requisitos

```bash
pip install -r requirements.txt
```

Sistema en ejecución (PC3 → PC2 → PC1) o usar `run_escenario.py`.

## Comandos

```bash
# Una corrida manual
python experimentos/run_escenario.py --config PC1/config_escenario_a.json --duracion 120
python experimentos/medir_bd.py --etiqueta E1_A --intervalo 120
python experimentos/medir_latencia.py --etiqueta E1_A

# Matriz completa (larga ~30+ min)
python experimentos/run_matriz.py

# Gráficos
python experimentos/plot_resultados.py
```

Salida: `experimentos/out/*.csv` y `*.png`.

## HW/SW (completar en informe)

- CPU/RAM de las 3 máquinas o VMs
- Python 3.12, pyzmq, matplotlib
- Herramientas: scripts anteriores + logs de servicios
