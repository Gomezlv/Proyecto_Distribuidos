# Sustentación — Caso 2: Congestión en avenida

Demostración de **congestión en la avenida B** con priorización del flujo en esa vía y **detención prolongada** de las calles perpendiculares.

## Objetivo

- Sensores de avenida (`CAM-B2`, `CAM-B4`) publican tráfico **congestionado** (valores fijos).
- Calles (`INT-A2`, `INT-C3`) permanecen en **ROJO 30 s** mientras la avenida está congestionada.
- Semáforos de avenida reciben **VERDE 30 s** (congestión) frente a 15 s en tráfico normal.

## Cuadrícula (4 semáforos)

```
        Col2      Col3
 A  |         | INT-A2  |
 B  | INT-B2  | INT-B4  |  ← Avenida B (congestión)
 C  |         | INT-C3  |
```

## Requisitos y configuración

Igual que caso 1: Python 3.12+, 3 PCs, editar IPs en [`PC1/config_sustent_caso2.json`](PC1/config_sustent_caso2.json).

## Ejecución

```bash
cd PC3 && python lanzar_pc3.py --config ../PC1/config_sustent_caso2.json
cd PC2 && python lanzar_pc2.py --config ../PC1/config_sustent_caso2.json
cd PC1 && python lanzar_pc1.py --config config_sustent_caso2.json
```

## Cómo probar

```bash
bash scripts/demo_caso2.sh
python PC3/consulta_cli.py --config PC1/config_sustent_caso2.json interseccion --interseccion INT-A2
```

## Flujo de demostración (≤ 4 min)

1. Arranque PC3 → PC2 → PC1.
2. **1–2 min:** Logs `[CONGESTION] INT-B2` / `INT-B4` con `verde 30s`.
3. Mostrar `[BLOQUEO] INT-A2` / `INT-C3` con **ROJO 30s** (comparar con 15 s del caso 1).
4. Consulta BD de alertas: `consulta_cli.py interseccion` o revisar tabla `alertas_congestion` vía histórico.

## Resultado esperado

- Avenida en verde extendido; calles en rojo **30 s** por `bloqueo_avenida_congestionada` o `conflicto_CONGESTION`.
- Alertas de congestión persistidas en BD.

## Errores comunes

| Problema | Solución |
|----------|----------|
| Calles reciben verde | Verificar perfiles `congestion` en sensores CAM-B2/B4 |
| No aparece CONGESTION | Q≥15 o Vp<10 en reglas; perfiles ya lo fijan |
| Tiempos iguales a caso 1 | Usar `config_sustent_caso2.json`, no caso 1 |

Ver [EXPLICACION_TECNICA.md](EXPLICACION_TECNICA.md).
