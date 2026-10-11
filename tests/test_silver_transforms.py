"""Validación de RUC y cuarentena de la capa Silver (transformaciones puras)."""

import shutil
import unittest

from silver_transforms import (
    MOTIVO_COL,
    CuarentenaExcedidaError,
    construir_reporte_calidad,
    preparar_ordenes_compra,
    preparar_padron_ruc,
    ruc_valido,
    separar_ordenes_compra,
    separar_padron_ruc,
    verificar_tasa_cuarentena,
)

HAS_JAVA = shutil.which("java") is not None


@unittest.skipUnless(HAS_JAVA, "requiere Java para PySpark")
class TestSilverTransforms(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-silver-transforms")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ruc_valido_exige_once_digitos(self):
        from pyspark.sql.functions import col

        df = self.spark.createDataFrame(
            [
                ("20100000001",),
                (" 20100000001 ",),
                ("12345678",),
                ("2010000000A",),
                (None,),
            ],
            "ruc string",
        )
        res = {
            r["ruc"]: r["ok"]
            for r in df.select("ruc", ruc_valido(col("ruc")).alias("ok")).collect()
        }
        self.assertTrue(res["20100000001"])
        self.assertTrue(res[" 20100000001 "])
        self.assertFalse(res["12345678"])
        self.assertFalse(res["2010000000A"])
        self.assertFalse(res[None])

    def test_padron_separa_ruc_invalidos_a_cuarentena(self):
        bronze = self.spark.createDataFrame(
            [
                (20100000001, "lima ", 202501, 2025, 1),
                (123, "cusco", 202501, 2025, 1),
                (None, "puno", 202501, 2025, 1),
                (20100000001, "lima ", 202501, 2025, 1),  # duplicado exacto
            ],
            "RUC long, Departamento string, PERIODO_PUBLICACION int, anio int, mes int",
        )
        silver, cuarentena = separar_padron_ruc(preparar_padron_ruc(bronze))

        self.assertEqual([r["RUC"] for r in silver.collect()], [20100000001])
        self.assertEqual(silver.collect()[0]["Departamento"], "LIMA")
        motivos = {r[MOTIVO_COL] for r in cuarentena.collect()}
        self.assertEqual(motivos, {"ruc_invalido"})
        self.assertEqual(cuarentena.count(), 2)

    def test_ordenes_separa_entidad_y_contratista_invalidos(self):
        bronze = self.spark.createDataFrame(
            [
                ("O1", 20100000001, "20200000002", 10.0, "lima", 2025, 1),  # válida
                (
                    "O2",
                    20100000001,
                    None,
                    5.0,
                    "lima",
                    2025,
                    1,
                ),  # sin contratista: válida
                ("O3", 20100000001, "45678912", 5.0, "lima", 2025, 1),  # DNI
                ("O4", 99, "20200000002", 5.0, "lima", 2025, 1),  # entidad corta
                ("O5", None, "45678912", 5.0, "lima", 2025, 1),  # ambos
                ("O6", 20100000001, "20200000002", -1.0, "lima", 2025, 1),  # monto < 0
            ],
            "orden string, ruc_entidad long, ruc_contratista string, "
            "monto_total_orden_original double, departamento_entidad string, anio int, mes int",
        )
        silver, cuarentena = separar_ordenes_compra(preparar_ordenes_compra(bronze))

        self.assertEqual(sorted(r["orden"] for r in silver.collect()), ["O1", "O2"])
        motivos = {r["orden"]: r[MOTIVO_COL] for r in cuarentena.collect()}
        self.assertEqual(
            motivos,
            {
                "O3": "ruc_contratista_invalido",
                "O4": "ruc_entidad_invalido",
                "O5": "ruc_entidad_invalido;ruc_contratista_invalido",
            },
        )
        # El monto negativo es regla de negocio: se descarta, no va a cuarentena.
        self.assertNotIn("O6", motivos)

    def test_todo_ruc_en_silver_tiene_once_digitos(self):
        bronze = self.spark.createDataFrame(
            [(20100000001 + i, "lima", 202501, 2025, 1) for i in range(5)]
            + [(i, "lima", 202501, 2025, 1) for i in range(1, 6)],
            "RUC long, Departamento string, PERIODO_PUBLICACION int, anio int, mes int",
        )
        silver, _ = separar_padron_ruc(preparar_padron_ruc(bronze))
        self.assertEqual(silver.count(), 5)
        self.assertTrue(all(len(str(r["RUC"])) == 11 for r in silver.collect()))

    def _padron_con_dos_meses(self):
        filas = (
            [(20100000001 + i, "lima", 202501, 2025, 1) for i in range(8)]
            + [(5, "lima", 202501, 2025, 1), (6, "lima", 202501, 2025, 1)]
            + [(20100000001 + i, "lima", 202502, 2025, 2) for i in range(4)]
        )
        return self.spark.createDataFrame(
            filas,
            "RUC long, Departamento string, PERIODO_PUBLICACION int, anio int, mes int",
        )

    def _reporte(self):
        bronze = self._padron_con_dos_meses()
        entrada = preparar_padron_ruc(bronze)
        silver, cuarentena = separar_padron_ruc(entrada)
        cuarentena_bronze = self.spark.createDataFrame(
            [(2025, 1), (2025, 1)], "anio int, mes int"
        )
        return construir_reporte_calidad(
            "padron_ruc", bronze, entrada, silver, cuarentena, cuarentena_bronze
        )

    def test_reporte_de_calidad_por_fuente_y_mes(self):
        filas = {(r["anio"], r["mes"]): r for r in self._reporte().collect()}

        enero = filas[(2025, 1)]
        self.assertEqual(enero["fuente"], "padron_ruc")
        self.assertEqual(enero["filas_entrada"], 10)
        self.assertEqual(enero["filas_cuarentena_ruc"], 2)
        self.assertEqual(enero["filas_silver"], 8)
        self.assertAlmostEqual(enero["tasa_cuarentena_ruc"], 0.2)
        # 2 rechazadas en bronze frente a 10 limpias -> 2 / 12
        self.assertAlmostEqual(enero["tasa_cuarentena_bronze"], 2 / 12)

        febrero = filas[(2025, 2)]
        self.assertEqual(febrero["filas_cuarentena_ruc"], 0)
        self.assertEqual(febrero["tasa_cuarentena_ruc"], 0.0)
        self.assertEqual(febrero["filas_cuarentena_bronze"], 0)

    def test_tasa_sobre_el_maximo_aborta_antes_de_escribir(self):
        reporte = self._reporte()
        verificar_tasa_cuarentena(reporte, 0.5)  # 20 % < 50 %: no lanza
        with self.assertRaises(CuarentenaExcedidaError) as ctx:
            verificar_tasa_cuarentena(reporte, 0.1)
        self.assertIn("padron_ruc 2025-1", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
