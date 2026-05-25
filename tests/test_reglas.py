import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "PC2"))

from reglas import ReglasTrafico, EstadoTrafico


class TestReglas(unittest.TestCase):
    def test_normal(self):
        r = ReglasTrafico()
        self.assertEqual(r.evaluar(3, 40, 10), EstadoTrafico.NORMAL)

    def test_congestion(self):
        r = ReglasTrafico()
        self.assertEqual(r.evaluar(20, 8, 10), EstadoTrafico.CONGESTION)

    def test_severo(self):
        r = ReglasTrafico()
        self.assertEqual(r.evaluar(30, 3, 10), EstadoTrafico.SEVERO)

    def test_priorizacion_duracion(self):
        r = ReglasTrafico()
        self.assertEqual(r.calcular_duracion_verde(EstadoTrafico.PRIORIZACION), 60)


if __name__ == "__main__":
    unittest.main()
