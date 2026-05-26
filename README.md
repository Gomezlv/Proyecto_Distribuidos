# Sustentación — Caso 3: Monitoreo y consulta

Demuestra el **módulo de monitoreo** en PC3: priorización tipo ambulancia (ola verde) y **consultas a la base de datos**.

## Objetivo

1. **Ambulancia / ola verde:** verde prolongado en `INT-B2` (Avenida-B2) y rojo en calles que la cruzan; luego retorno al tráfico normal.
2. **Consulta BD:** `priorizaciones` e `historico` vía `consulta_cli.py`.

## Requisitos

- Python 3.12+, 3 PCs, [`PC1/config_sustent_caso3.json`](PC1/config_sustent_caso3.json) con IPs correctas.
- Sistema arrancado: PC3 → PC2 → PC1.

## Ejecución

```bash
cd PC3 && python lanzar_pc3.py --config ../PC1/config_sustent_caso3.json
cd PC2 && python lanzar_pc2.py --config ../PC1/config_sustent_caso3.json
cd PC1 && python lanzar_pc1.py --config config_sustent_caso3.json
```

## Comandos clave

```bash
# Ambulancia (ola verde 45 s en avenida B2)
python PC3/consulta_cli.py --config PC1/config_sustent_caso3.json \
  ambulancia --interseccion INT-B2 --duracion 45

# Consultas elegidas para la sustentación
python PC3/consulta_cli.py --config PC1/config_sustent_caso3.json priorizaciones
python PC3/consulta_cli.py --config PC1/config_sustent_caso3.json historico \
  --t-ini 2026-01-01T00:00:00Z --t-fin 2026-12-31T23:59:59Z
```

Guion: `bash scripts/demo_caso3.sh`

## Flujo de demostración (≤ 4 min)

| Minuto | Acción |
|--------|--------|
| 0–1 | Tráfico base estable (sensores `normal`) |
| 1–2 | Comando `ambulancia` → logs PRIORIZACION + semáforos |
| 2–3 | Esperar fin de 45 s; observar vuelta a NORMAL |
| 3–4 | `priorizaciones` + `historico` en pantalla |

## Resultado esperado

- Respuesta JSON `ok: true` en ambulancia con latencia registrada.
- Tabla `alertas_congestion` / priorizaciones con entrada `PRIORIZACION`.
- Histórico con eventos de sensores y estados de semáforo.

## Errores comunes

| Problema | Solución |
|----------|----------|
| `token inválido` | Mismo `secret_key` en config y monitor |
| Timeout ambulancia | PC2 analítica activa; puerto 5565 abierto |
| Consulta vacía | Ejecutar ambulancia antes; ampliar rango `--t-fin` |

Ver [EXPLICACION_TECNICA.md](EXPLICACION_TECNICA.md).
