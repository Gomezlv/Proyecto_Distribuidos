import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinacion import CoordinadorSemaforos


CFG = {
    "coordinacion": {
        "avenida_filas": ["B"],
        "tiempo_rojo_normal": 15,
        "tiempo_rojo_congestion": 30,
        "alias": {"INT-B2": "Avenida-B2", "INT-A2": "Calle-A2"},
        "mapa_conflictos": {
            "INT-B2": ["INT-A2", "INT-C3"],
            "INT-A2": ["INT-B2"],
        },
    }
}


class TestCoordinacion(unittest.TestCase):

    def setUp(self):
        self.coord = CoordinadorSemaforos(CFG)

    def test_conflictos_avenida(self):
        self.assertEqual(self.coord.conflictos_de("INT-B2"), ["INT-A2", "INT-C3"])

    def test_alias(self):
        self.assertEqual(self.coord.alias("INT-B2"), "Avenida-B2")

    def test_es_avenida(self):
        self.assertTrue(self.coord.es_avenida("INT-B2"))
        self.assertFalse(self.coord.es_avenida("INT-A2"))

    def test_duracion_rojo_normal_vs_congestion(self):
        self.assertEqual(self.coord.duracion_rojo("normal"), 15)
        self.assertEqual(self.coord.duracion_rojo("congestion"), 30)


if __name__ == "__main__":
    unittest.main()
