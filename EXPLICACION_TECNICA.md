# Explicación técnica — Caso 2: Congestión

## Arquitectura

Misma topología 3 PCs del caso 1. La diferencia está en la **lógica de analítica** y los **perfiles de sensor**.

## Componentes modificados

- `PC2/analitica.py`: `_hay_congestion_avenida()` y bloqueo de calles.
- `common/perfil_sensor.py`: perfiles `congestion` y `normal`.
- `PC1/config_sustent_caso2.json`: 4 semáforos, cámaras en avenida con congestión simulada.

## Decisiones técnicas

1. **Congestión determinista** en sensores de avenida para demo reproducible.
2. **Doble mecanismo de rojo en calles:**
   - Al abrir verde en avenida: conflictos con `tiempo_rojo_congestion` (30 s).
   - Si la calle reporta NORMAL pero la avenida sigue congestionada: comando explícito `bloqueo_avenida_congestionada`.
3. **Verde 30 s** en avenida vía `ReglasTrafico.calcular_duracion_verde(CONGESTION)`.

## Flujo interno

```
CAM-B2 (congestion) → Analítica → CONGESTION en INT-B2
  → ROJO 30s en INT-A2, INT-C3 (conflictos)
  → VERDE 30s en INT-B2

ESP-A2 (normal) en INT-A2 mientras avenida congestionada
  → [BLOQUEO] ROJO 30s (no se otorga verde a la calle)
```

## Patrones

- Coordinador de dominio (`CoordinadorSemaforos`).
- Evaluación por intersección con política global de avenida (estado derivado del conjunto de semáforos en fila B).

## Riesgos

- Si solo una cámara de avenida falla, `_hay_congestion_avenida` puede no detectar congestión en la otra intersección hasta recibir su evento.
- Bloqueo de calles depende de que los sensores de avenida sigan publicando.

## Mejoras futuras

- Ventana deslizante de congestión por eje completo.
- Publicar alerta única por avenida, no por intersección.

## Guión oral

"Mientras la avenida B está congestionada, forzamos rojo prolongado en las calles que la cruzan. Los logs muestran CONGESTION con 30 segundos de verde en avenida y BLOQUEO con 30 segundos de rojo en calle, frente a 15 segundos en el caso normal."
