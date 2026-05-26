"""Valores deterministas de sensores para demos de sustentación."""
from __future__ import annotations
import random


PERFILES_CAMARA = {
    "normal": {"volumen": 2, "velocidad_promedio": 42.0},
    "congestion": {"volumen": 22, "velocidad_promedio": 7.0},
    "severo": {"volumen": 28, "velocidad_promedio": 4.0},
}

PERFILES_ESPIRA = {
    "normal": {"vehiculos_contados": 5},
    "congestion": {"vehiculos_contados": 8},
}

PERFILES_GPS = {
    "normal": {"velocidad_promedio": 45.0, "nivel_congestion": "BAJA"},
    "congestion": {"velocidad_promedio": 7.0, "nivel_congestion": "ALTA"},
}


def aplicar_perfil(sensor_cfg: dict | None) -> dict | None:
    """Devuelve valores fijos si el sensor define 'perfil', si no None (aleatorio)."""
    if not sensor_cfg:
        return None
    perfil = sensor_cfg.get("perfil")
    if not perfil:
        return None
    tipo = sensor_cfg.get("tipo", "")
    if tipo == "camara":
        base = PERFILES_CAMARA.get(perfil, PERFILES_CAMARA["normal"])
    elif tipo == "espira_inductiva":
        base = PERFILES_ESPIRA.get(perfil, PERFILES_ESPIRA["normal"])
    elif tipo == "gps":
        base = PERFILES_GPS.get(perfil, PERFILES_GPS["normal"])
    else:
        return None
    out = dict(base)
    for k, v in sensor_cfg.items():
        if k in ("volumen", "velocidad_promedio", "vehiculos_contados", "nivel_congestion"):
            out[k] = v
    return out


def volumen_aleatorio(vol_min: int, vol_max: int) -> int:
    return random.randint(vol_min, vol_max)


def velocidad_aleatoria(vp_min: float, vp_max: float, volumen: int = 0) -> float:
    vp_max_ajustado = max(vp_min + 2, vp_max - volumen) if volumen else vp_max
    return round(random.uniform(vp_min, vp_max_ajustado), 1)
