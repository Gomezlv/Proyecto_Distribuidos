# Explicación técnica — Caso 3: Monitoreo y consulta

## Arquitectura del flujo ambulancia

```
consulta_cli (REQ) → monitor.py PC3:5600 (REP)
  → operacion comando_prioridad
  → analitica.py PC2:5565 (REP)
  → _enviar_verde_coordinado(INT-B2)
       → ROJO conflictos (INT-A2, INT-C3) 30s
       → VERDE INT-B2 45s
  → persistencia alerta PRIORIZACION en BD
```

## Componentes

| Componente | Función |
|------------|---------|
| `PC3/consulta_cli.py` | Cliente de demostración |
| `PC3/monitor.py` | REQ/REP, delega a analítica |
| `common/monitor_core.py` | SQL consultas |
| `PC2/analitica.py` | `procesar_comando_manual` + coordinación |

## Decisiones técnicas

- **REQ/REP** para comando crítico (ambulancia) con respuesta inmediata y latencia medible.
- Coordinación perpendicular reutilizada: el profesor ve rojo en calles antes del verde de la ambulancia.
- Consultas **`priorizaciones`** y **`historico`**: evidencian persistencia y trazabilidad post-demo.

## Patrones

- Cadena REQ/REP Monitor → Analítica.
- Comando idempotente con `token` compartido en config.

## Código relevante

`procesar_comando_manual` valida token, llama `_enviar_verde_coordinado`, persiste alerta con `nivel: PRIORIZACION`.

## Riesgos

- Si PC3 cae durante ambulancia, usar failover puerto 5601 (caso 4).
- Duración de ola verde no cancela comandos posteriores de sensores; pueden superponerse ciclos (aceptable en demo).

## Mejoras futuras

- Cola de prioridades y cancelación explícita al fin de ambulancia.
- API REST además de CLI.

## Guión oral

"El operador envía ambulancia desde el CLI; el monitor en PC3 reenvía a analítica en PC2, que abre ola verde coordinada. Luego consultamos la BD para demostrar que la priorización quedó registrada junto con el histórico de eventos."
