"""Estado global compartido: disponibilidad de PC3 (health check)."""
from __future__ import annotations
import threading

_lock = threading.Lock()
_pc3_activo = True


def pc3_activo() -> bool:
    with _lock:
        return _pc3_activo


def set_pc3_activo(activo: bool) -> None:
    global _pc3_activo
    with _lock:
        _pc3_activo = activo
