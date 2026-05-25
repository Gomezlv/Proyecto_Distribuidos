"""Añade la raíz del repositorio a sys.path."""
from __future__ import annotations
import os
import sys


def setup_repo_path() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    return root
