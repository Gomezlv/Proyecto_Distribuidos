# Sustentación — Caso 1: Tráfico normal

Sistema distribuido de semáforos (Python 3.12 + ZeroMQ en 3 PCs). Esta rama demuestra **tráfico normal** sin congestión, con coordinación perpendicular y datos en BD principal y réplica.

## Objetivo del caso

Mostrar que, con pocos semáforos y tráfico bajo, el sistema:

- Cambia luces **VERDE → ROJO** con tiempos predecibles (15 s).
- Evita choques: si la **avenida** (`INT-B2`) está en verde, las **calles** perpendiculares (`INT-A2`, `INT-C3`) quedan en rojo.
- Persiste eventos en **BD principal (PC3)** y **réplica (PC2)**.

## Qué demuestra

| Elemento | Evidencia |
|----------|-----------|
| 3 semáforos nombrados | Logs `Avenida-B2`, `Calle-A2`, `Calle-C3` |
| Ciclo normal | `[ANALITICA] NORMAL` y verde 15 s |
| Seguridad perpendicular | Rojo en calles antes del verde de avenida |
| Persistencia | `scripts/ver_estado_bd.py` |

## Requisitos

- Python 3.12+, `pip install -r requirements.txt`
- 3 PCs en red con IPs distintas en `PC1/config_sustent_caso1.json`
- Puertos abiertos: 5551–5553, 5560, 5565, 5580–5581, 5590, 5570, 5600–5601

## Configuración

1. Copie y edite IPs en [`PC1/config_sustent_caso1.json`](PC1/config_sustent_caso1.json):

```json
"pc1_ip": "<IP_PC1>",
"pc2_ip": "<IP_PC2>",
"pc3_ip": "<IP_PC3>"
```

2. Use la plantilla [`PC1/config_sustent.dist.json`](PC1/config_sustent.dist.json) como referencia.

## Cómo ejecutar

**Orden:** PC3 → PC2 → PC1.

```bash
# PC3
cd PC3 && python lanzar_pc3.py --config ../PC1/config_sustent_caso1.json

# PC2
cd PC2 && python lanzar_pc2.py --config ../PC1/config_sustent_caso1.json

# PC1
cd PC1 && python lanzar_pc1.py --config config_sustent_caso1.json
```

## Cómo probar

```bash
# Consulta de una intersección (desde cualquier máquina con acceso a PC3)
python PC3/consulta_cli.py --config PC1/config_sustent_caso1.json interseccion --interseccion INT-B2

# Comparar BD principal y réplica
python scripts/ver_estado_bd.py --veces 3
```

Guion interactivo: `bash scripts/demo_caso1.sh`

## Flujo de demostración (≤ 4 min)

1. **0:00–0:30** — Arranque en PC3, PC2, PC1.
2. **0:30–2:30** — Mostrar logs de semáforos: verde 15 s, luego rojo; al activar avenida, calles en rojo.
3. **2:30–3:00** — `consulta_cli.py interseccion --interseccion INT-B2`.
4. **3:00–4:00** — `ver_estado_bd.py`: conteos en principal y réplica creciendo.

## Resultado esperado

- Logs `[SEMAFORO] Avenida-B2 (INT-B2) | ROJO → VERDE | Duración: 15s`.
- Antes del verde de `INT-B2`, logs de `Calle-A2` y `Calle-C3` en **ROJO**.
- Ambas bases con filas en `eventos_sensores` y `estados_semaforos`.

## Errores comunes

| Problema | Solución |
|----------|----------|
| Sin datos en analítica | Verificar `pc1_ip` en config y que el broker esté arriba |
| `pc3_ip` igual a `pc2_ip` | Asignar IPs distintas a cada PC |
| Semáforos todo verde (watchdog) | Asegurar que analítica envía comandos; intervalo sensores 8 s |
| Timeout en consulta | Firewall puerto 5600; PC3 debe estar activo |

## Documentación técnica

Ver [EXPLICACION_TECNICA.md](EXPLICACION_TECNICA.md) para la sustentación oral.
