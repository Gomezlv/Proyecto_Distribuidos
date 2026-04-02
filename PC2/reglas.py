from dataclasses import dataclass
from enum import Enum


class EstadoTrafico(str, Enum):
    NORMAL       = "NORMAL"
    MODERADO     = "MODERADO"
    CONGESTION   = "CONGESTION"
    SEVERO       = "SEVERO"
    PRIORIZACION = "PRIORIZACION"


@dataclass
class ReglasTrafico:

    Q_normal_max:            int   = 5
    Vp_normal_min:           float = 35.0
    D_normal_max:            int   = 20
    Q_congestion:            int   = 15
    Vp_congestion:           float = 10.0
    Q_severo:                int   = 25
    Vp_severo:               float = 5.0
    tiempo_verde_normal:     int   = 15
    tiempo_verde_moderado:   int   = 20
    tiempo_verde_congestion: int   = 30
    tiempo_verde_severo:     int   = 45

    @classmethod
    def desde_config(cls, cfg_reglas: dict) -> "ReglasTrafico":
        """Construye desde el bloque 'reglas' del config.json."""
        return cls(
            Q_normal_max            = cfg_reglas.get("Q_normal_max",            5),
            Vp_normal_min           = cfg_reglas.get("Vp_normal_min",           35.0),
            D_normal_max            = cfg_reglas.get("D_normal_max",            20),
            Q_congestion            = cfg_reglas.get("Q_congestion",            15),
            Vp_congestion           = cfg_reglas.get("Vp_congestion",           10.0),
            Q_severo                = cfg_reglas.get("Q_severo",                25),
            Vp_severo               = cfg_reglas.get("Vp_severo",               5.0),
            tiempo_verde_normal     = cfg_reglas.get("tiempo_verde_normal",     15),
            tiempo_verde_moderado   = cfg_reglas.get("tiempo_verde_moderado",   20),
            tiempo_verde_congestion = cfg_reglas.get("tiempo_verde_congestion", 30),
            tiempo_verde_severo     = cfg_reglas.get("tiempo_verde_severo",     45),
        )

    def evaluar(self, Q: float, Vp: float, D: float) -> EstadoTrafico:

        if Q >= self.Q_severo or Vp < self.Vp_severo:
            return EstadoTrafico.SEVERO

        if (Q >= self.Q_congestion or
                Vp < self.Vp_congestion or
                D >= self.D_normal_max * 2):
            return EstadoTrafico.CONGESTION

        if (Q < self.Q_normal_max and
                Vp > self.Vp_normal_min and
                D < self.D_normal_max):
            return EstadoTrafico.NORMAL

        return EstadoTrafico.MODERADO

    def calcular_duracion_verde(self, estado: EstadoTrafico,
                                 dur_override: int | None = None) -> int:
        if dur_override is not None:
            return dur_override
        mapa = {
            EstadoTrafico.NORMAL:       self.tiempo_verde_normal,
            EstadoTrafico.MODERADO:     self.tiempo_verde_moderado,
            EstadoTrafico.CONGESTION:   self.tiempo_verde_congestion,
            EstadoTrafico.SEVERO:       self.tiempo_verde_severo,
            EstadoTrafico.PRIORIZACION: 60,   # default ambulancia
        }
        return mapa.get(estado, self.tiempo_verde_normal)