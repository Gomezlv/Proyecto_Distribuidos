import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinacion import CoordinadorSemaforos

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "PC2"))
from reglas import ReglasTrafico, EstadoTrafico


class TestCongestionCaso2(unittest.TestCase):

    def test_perfil_congestion_supera_umbral(self):
        r = ReglasTrafico()
        estado = r.evaluar(22, 7.0, 10)
        self.assertEqual(estado, EstadoTrafico.CONGESTION)

    def test_rojo_congestion_mas_largo(self):
        c = CoordinadorSemaforos({
            "coordinacion": {
                "tiempo_rojo_normal": 15,
                "tiempo_rojo_congestion": 30,
            }
        })
        self.assertGreater(c.duracion_rojo("congestion"), c.duracion_rojo("normal"))


if __name__ == "__main__":
    unittest.main()
