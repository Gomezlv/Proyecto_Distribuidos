"""Coordinación de semáforos en intersecciones conflictivas (sustentación)."""
from __future__ import annotations


class CoordinadorSemaforos:
    """Resuelve conflictos perpendiculares según mapa en config."""

    def __init__(self, cfg: dict):
        coord = cfg.get("coordinacion", {})
        self.avenida_filas: list[str] = coord.get("avenida_filas", [])
        self.tiempo_rojo_normal: int = coord.get("tiempo_rojo_normal", 15)
        self.tiempo_rojo_congestion: int = coord.get("tiempo_rojo_congestion", 30)
        self.mapa_conflictos: dict[str, list[str]] = coord.get("mapa_conflictos", {})
        self.alias_map: dict[str, str] = coord.get("alias", {})

    def conflictos_de(self, interseccion: str) -> list[str]:
        return list(self.mapa_conflictos.get(interseccion, []))

    def alias(self, interseccion: str) -> str:
        return self.alias_map.get(interseccion, interseccion)

    def es_avenida(self, interseccion: str) -> bool:
        if len(interseccion) < 6 or not interseccion.startswith("INT-"):
            return False
        fila = interseccion[4]
        return fila in self.avenida_filas

    def duracion_rojo(self, modo: str) -> int:
        if modo in ("congestion", "severo", "priorizacion"):
            return self.tiempo_rojo_congestion
        return self.tiempo_rojo_normal
