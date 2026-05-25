# Informe de rendimiento (plantilla ≤ 5 páginas)

## 1. Introducción

Breve descripción del sistema y objetivo de comparar diseño E1 (broker single-thread) vs E2 (broker multihilo).

## 2. Especificaciones HW/SW

| Ítem | Valor |
|------|-------|
| PC1 | CPU / RAM / SO |
| PC2 | CPU / RAM / SO |
| PC3 | CPU / RAM / SO |
| Python | 3.12.x |
| Librerías | pyzmq, matplotlib |
| Herramientas | `experimentos/run_matriz.py`, `medir_bd.py`, `medir_latencia.py` |

## 3. Metodología (Tabla 1)

### Escenarios

- **A:** 1 sensor por tipo, intervalo 10 s (`config_escenario_a.json`)
- **B:** 6 sensores, intervalo 5 s (`config_escenario_b.json`)

### Diseños

- **E1:** broker sin `--multihilo`
- **E2:** broker con `--multihilo`

### Variables dependientes

1. Eventos en `eventos_sensores` medidos cada 2 min (`medir_bd.py`)
2. Latencia comando prioridad (`medir_latencia.py`)

## 4. Resultados

*(Insertar tablas desde `experimentos/out/medicion_bd.csv` y `medicion_latencia.csv`)*

*(Insertar figuras `grafico_eventos_bd.png` y `grafico_latencia.png`)*

### Tabla ejemplo

| Etiqueta | Eventos réplica (t=120s) | Latencia media (s) |
|----------|--------------------------|-------------------|
| E1_A | | |
| E2_A | | |
| E1_B | | |
| E2_B | | |

## 5. Análisis

- ¿E2 procesa más eventos bajo carga alta (escenario B)?
- ¿La latencia REQ/REP se mantiene aceptable?
- Limitaciones: localhost vs 3 máquinas, SQLite single-writer.

## 6. Conclusiones

- Diseño más escalable según resultados: E1 / E2 (justificar con números).
- Trabajo futuro: ROUTER/DEALER, réplica maestro-esclavo real.

## Referencias

- Enunciado curso SD 2026-10
- ZeroMQ Guide: https://zguide.zeromq.org/
