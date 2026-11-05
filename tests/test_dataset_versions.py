"""Cortes por versión del dataset (mes de la foto del Padrón + fecha de corte)."""

import unittest

from dataset_versions import VERSION_CORTES, mes_de_version, parse_cortes


class TestDatasetVersions(unittest.TestCase):
    def test_sin_valor_usa_todas_las_versiones(self):
        esperado = list(VERSION_CORTES.values())
        self.assertEqual(parse_cortes(""), esperado)
        self.assertEqual(parse_cortes("todas"), esperado)
        self.assertEqual(esperado, [("202506", "2025-06-30"), ("202512", "2025-12-31")])

    def test_lista_configurable(self):
        self.assertEqual(
            parse_cortes("202503:2025-03-31, 202509:2025-09-30"),
            [("202503", "2025-03-31"), ("202509", "2025-09-30")],
        )

    def test_corte_mal_formado_falla(self):
        for malo in ("2025:2025-06-30", "202513:2025-06-30", "202506:30-06-2025"):
            with self.assertRaises(ValueError):
                parse_cortes(malo)

    def test_mes_de_version(self):
        self.assertEqual(mes_de_version("v1"), "202506")
        self.assertEqual(mes_de_version("v2"), "202512")
        with self.assertRaises(ValueError):
            mes_de_version("v9")


if __name__ == "__main__":
    unittest.main()
