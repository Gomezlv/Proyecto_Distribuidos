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
        return json.load(f)
