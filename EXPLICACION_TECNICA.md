# Explicación técnica — Caso 1: Tráfico normal

## Arquitectura involucrada

```
PC1 (sensores + broker) --PUB/SUB--> PC2 (analítica)
                                      |--PUSH--> semáforos (PC2)
                                      |--PUSH--> réplica SQLite (PC2)
                                      |--PUSH--> principal SQLite (PC3)
PC3 (monitor REQ/REP, BD principal)
```

## Componentes modificados

| Archivo | Rol |
|---------|-----|
| `common/coordinacion.py` | Mapa de conflictos y alias |
| `common/perfil_sensor.py` | Valores fijos `perfil: normal` |
| `PC2/analitica.py` | Rojo en conflictos antes de verde |
| `PC2/semaforos.py` | Logs con alias legibles |
| `PC1/*_sensor.py` | Perfiles deterministas |
| `PC1/config_sustent_caso1.json` | Cuadrícula 3 semáforos / 3 sensores |

## Decisiones técnicas

1. **Mapa explícito de conflictos** en JSON: fácil de explicar en pizarra sin algoritmo opaco.
2. **Orden rojo → verde**: al profesor le basta ver en logs que primero se bloquean las calles.
3. **Perfiles fijos**: elimina azar de `random` para que el estado sea siempre `NORMAL`.
4. **Sin cambiar protocolo ZMQ**: solo payload extra `alias` en comandos de semáforo.

## Patrones utilizados

- **PUB/SUB** sensores → broker → analítica.
- **PUSH/PULL** analítica → semáforos y BD.
- **Coordinador** como objeto de dominio (SRP) separado de `ServicioAnalitica`.

## Flujo interno

```
Sensor (perfil normal) → Broker → Analítica.evaluar() → NORMAL
  → conflictos(INT-X) → ROJO en INT-A2, INT-C3 (si X es avenida)
  → VERDE en INT-X, duración 15s
  → Semáforo: al expirar VERDE → ROJO 15s
  → Persistencia dual (réplica siempre, principal si PC3 vivo)
```

## Código agregado (resumen)

`CoordinadorSemaforos.conflictos_de()` lee `mapa_conflictos`.  
`_enviar_verde_coordinado()` en analítica llama `_aplicar_rojos_conflictivos()` y luego verde.

## Justificación de diseño

El enunciado exige demostrar que **no hay choque** entre avenida y calles. La implementación actual por intersección independiente no lo garantizaba; el coordinador cierra esa brecha sin un controlador central de fases complejo.

## Riesgos y limitaciones

- Solo cubre conflictos definidos en config (no inferencia automática de cuadrícula completa).
- No sincroniza verde simultáneo en dos semáforos de la misma avenida (caso 1 tiene uno en fila B).
- Watchdog de semáforos puede poner todo verde si analítica deja de enviar comandos > 60 s (config `semaforos_watchdog_seg` documentado pero no cableado aún en código).

## Mejoras futuras

- Fases rotativas automáticas (avenida / calles) sin depender de cada evento de sensor.
- UI web de estado en tiempo real.
- Métricas Prometheus de latencia comando→semáforo.

## Diagrama textual para exposición oral

```
[CAM-B2] --normal--> [Analítica] --ROJO--> Calle-A2, Calle-C3
                         |
                         +--VERDE--> Avenida-B2 (15s)
                         |
                         v
                    [Semáforos] VERDE→ROJO automático
                         |
                         v
              [BD réplica] + [BD principal]
```

## Guión oral sugerido (2 min)

1. "Tenemos tres PCs: adquisición, procesamiento y persistencia."
2. "Los sensores publican valores fijos de tráfico ligero; la analítica clasifica NORMAL."
3. "Antes de abrir la avenida en verde, forzamos rojo en las calles que la cruzan."
4. "En los logs ven el alias Avenida-B2 y la duración de 15 segundos."
5. "Cerramos mostrando que principal y réplica guardan los mismos tipos de eventos."
