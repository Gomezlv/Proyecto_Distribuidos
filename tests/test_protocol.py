import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import ok_response, error_response, OP_QUERY_HISTORICO


class TestProtocol(unittest.TestCase):
    def test_ok_response(self):
        r = ok_response({"x": 1}, backend="principal")
        self.assertTrue(r["ok"])
        self.assertEqual(r["backend"], "principal")
        self.assertEqual(r["x"], 1)

    def test_error_response(self):
        r = error_response("fallo")
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "fallo")

    def test_op_constants(self):
        self.assertEqual(OP_QUERY_HISTORICO, "query_historico")


if __name__ == "__main__":
    unittest.main()
