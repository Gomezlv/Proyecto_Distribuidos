"""Carga centralizada de config.json."""
from __future__ import annotations
import json
import os


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def default_config_path() -> str:
    return os.path.join(repo_root(), "PC1", "config.json")


def cargar_config(ruta: str | None = None) -> dict:
    path = ruta or default_config_path()
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    red = cfg.get("red", {})
    pc2 = red.get("pc2_ip")
    pc3 = red.get("pc3_ip")
    if pc2 and pc3 and pc2 == pc3:
        import warnings
        warnings.warn(
            f"[config] pc2_ip y pc3_ip son iguales ({pc2}). "
            "En 3 PCs físicas deben ser distintas.",
            stacklevel=2,
        )
    return cfg
