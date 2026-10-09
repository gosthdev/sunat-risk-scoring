import sqlite3
import unittest
from pathlib import Path

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent
_QUERIES_DIR = _ROOT / "src" / "analysis" / "queries"


class ApproxPercentileAggregate:
    """Implementación de APPROX_PERCENTILE para emular la función de Trino/Athena en SQLite."""

    def __init__(self):
        self.values = []
        self.percentile = 0.5

    def step(self, value, percentile=0.5):
        if value is not None:
            self.values.append(value)
            self.percentile = percentile

    def finalize(self):
        if not self.values:
            return None
        return float(np.percentile(self.values, self.percentile * 100))


class TestAnalysisQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = sqlite3.connect(":memory:")
        cls.conn.row_factory = sqlite3.Row
        cls.conn.create_aggregate("APPROX_PERCENTILE", 2, ApproxPercentileAggregate)
        cls.conn.create_aggregate("approx_percentile", 2, ApproxPercentileAggregate)

        cur = cls.conn.cursor()
        cur.execute(
            """
            CREATE TABLE gold_regional_summary (
                departamento TEXT NOT NULL,
                ruc_activos INTEGER NOT NULL,
                pct_informalidad REAL NOT NULL,
                recaudacion_soles REAL NOT NULL,
                concentracion_pricos INTEGER NOT NULL
            );
            """
        )

        # Datos representativos de departamentos peruanos (escala real / proxy)
        cls.sample_regions = [
            ("LIMA", 1_350_000, 56.4, 85_000_000_000.0, 14_500),
            ("AREQUIPA", 160_000, 67.8, 3_800_000_000.0, 620),
            ("LA LIBERTAD", 175_000, 71.5, 3_100_000_000.0, 580),
            ("PIURA", 140_000, 74.2, 2_400_000_000.0, 420),
            ("LAMBAYEQUE", 115_000, 75.0, 1_600_000_000.0, 290),
            ("CUSCO", 110_000, 77.1, 1_900_000_000.0, 310),
            ("JUNIN", 105_000, 78.4, 1_200_000_000.0, 210),
            ("PUNO", 95_000, 85.9, 750_000_000.0, 170),
            ("CAJAMARCA", 72_000, 88.3, 920_000_000.0, 130),
            ("AYACUCHO", 48_000, 87.5, 310_000_000.0, 65),
            ("APURIMAC", 32_000, 89.1, 220_000_000.0, 45),
            ("HUANCAVELICA", 24_000, 91.4, 140_000_000.0, 28),
        ]
        cur.executemany(
            "INSERT INTO gold_regional_summary VALUES (?, ?, ?, ?, ?);",
            cls.sample_regions,
        )
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_ruc_activos_por_departamento_query(self):
        """Valida que ruc_activos_por_departamento.sql ejecute limpiamente y retorne columnas y orden esperados."""
        sql_path = _QUERIES_DIR / "ruc_activos_por_departamento.sql"
        self.assertTrue(sql_path.exists(), f"No existe {sql_path}")

        query = sql_path.read_text(encoding="utf-8")
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

        # Validar columnas devueltas
        col_names = [desc[0] for desc in cur.description]
        self.assertEqual(col_names, ["departamento", "ruc_activos", "pct_del_total"])

        # Debe tener el número de filas correspondiente a las regiones de prueba
        self.assertEqual(len(rows), len(self.sample_regions))

        # El primer departamento debe ser LIMA (mayor volumen de RUC activos)
        self.assertEqual(rows[0]["departamento"], "LIMA")

        # Verificar orden descendente
        activos_list = [r["ruc_activos"] for r in rows]
        self.assertEqual(activos_list, sorted(activos_list, reverse=True))

        # La suma de porcentajes debe aproximar el 100%
        suma_pct = sum(r["pct_del_total"] for r in rows)
        self.assertAlmostEqual(suma_pct, 100.0, delta=0.5)

    def test_informalidad_vs_ruc_activos_query(self):
        """Valida cuadrantes (Lima en Alto RUC/Baja inf, Cajamarca en Bajo RUC/Alta inf) y medianas."""
        sql_path = _QUERIES_DIR / "informalidad_vs_ruc_activos.sql"
        self.assertTrue(sql_path.exists(), f"No existe {sql_path}")

        query = sql_path.read_text(encoding="utf-8")
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

        # Validar columnas devueltas
        col_names = [desc[0] for desc in cur.description]
        self.assertEqual(
            col_names,
            [
                "departamento",
                "ruc_activos",
                "pct_informalidad",
                "cuadrante",
                "mediana_ruc",
                "mediana_informalidad",
            ],
        )

        rows_dict = {r["departamento"]: dict(r) for r in rows}

        # Validación explícita de plan.md:
        # LIMA: Alto RUC / Baja Informalidad
        self.assertEqual(
            rows_dict["LIMA"]["cuadrante"],
            "Alto RUC / Baja Informalidad",
            f"LIMA debe estar en Alto RUC / Baja Informalidad, obtenido: {rows_dict['LIMA']['cuadrante']}",
        )
        # CAJAMARCA: Bajo RUC / Alta Informalidad
        self.assertEqual(
            rows_dict["CAJAMARCA"]["cuadrante"],
            "Bajo RUC / Alta Informalidad",
            f"CAJAMARCA debe estar en Bajo RUC / Alta Informalidad, obtenido: {rows_dict['CAJAMARCA']['cuadrante']}",
        )

        # Verificar orden por pct_informalidad DESC
        inf_list = [r["pct_informalidad"] for r in rows]
        self.assertEqual(inf_list, sorted(inf_list, reverse=True))

    def test_concentracion_pricos_query(self):
        """Valida que concentracion_pricos.sql ordene por concentración de PRICOS y calcule porcentajes correctamente."""
        sql_path = _QUERIES_DIR / "concentracion_pricos.sql"
        self.assertTrue(sql_path.exists(), f"No existe {sql_path}")

        query = sql_path.read_text(encoding="utf-8")
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

        # Validar columnas devueltas
        col_names = [desc[0] for desc in cur.description]
        self.assertEqual(
            col_names,
            [
                "departamento",
                "concentracion_pricos",
                "ruc_activos",
                "pct_pricos_sobre_activos",
                "recaudacion_soles",
                "pct_recaudacion_regional",
            ],
        )

        # LIMA debe liderar la concentración
        self.assertEqual(rows[0]["departamento"], "LIMA")
        self.assertGreater(rows[0]["pct_pricos_sobre_activos"], 1.0)
        self.assertGreater(rows[0]["pct_recaudacion_regional"], 70.0)

        # Verificar orden descendente de concentración
        pct_list = [r["pct_pricos_sobre_activos"] for r in rows]
        self.assertEqual(pct_list, sorted(pct_list, reverse=True))

        # Suma de recaudación regional debe aproximar el 100%
        suma_recaudacion = sum(r["pct_recaudacion_regional"] for r in rows)
        self.assertAlmostEqual(suma_recaudacion, 100.0, delta=0.5)


if __name__ == "__main__":
    unittest.main()
