"""Validación de RUC y cuarentena de la capa Silver (transformaciones puras)."""

import shutil
import unittest

from silver_transforms import (
    MOTIVO_COL,
    CuarentenaExcedidaError,
    SilverIncompletoError,
    construir_reporte_calidad,
    preparar_ordenes_compra,
    preparar_padron_ruc,
    ruc_valido,
    separar_ordenes_compra,
    separar_padron_ruc,
    verificar_silver_escrito,
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
                "O6": "monto_negativo",
            },
        )
        # Un monto negativo no se descarta en silencio: va a cuarentena con su motivo.
        self.assertEqual(motivos["O6"], "monto_negativo")

    def test_ordenes_repetidas_conservan_la_ultima_carga_y_orden_nulo_no_colapsa(self):
        bronze = self.spark.createDataFrame(
            [
                ("O1", 20100000001, "20200000002", 10.0, "lima", 2025, 1),
                ("O1", 20100000001, "20200000002", 99.0, "lima", 2025, 3),
                ("O1", 20100000001, "20200000002", 50.0, "lima", 2025, 2),
                (None, 20100000001, "20200000002", 1.0, "lima", 2025, 1),
                (None, 20100000001, "20200000003", 2.0, "lima", 2025, 1),
            ],
            "orden string, ruc_entidad long, ruc_contratista string, "
            "monto_total_orden_original double, departamento_entidad string, anio int, mes int",
        )
        filas = preparar_ordenes_compra(bronze).collect()

        o1 = [r for r in filas if r["orden"] == "O1"]
        self.assertEqual(len(o1), 1)
        self.assertEqual((o1[0]["mes"], o1[0]["monto_total_orden_original"]), (3, 99.0))
        self.assertEqual(len([r for r in filas if r["orden"] is None]), 2)

    def test_monto_nulo_y_negativo_van_a_cuarentena_con_su_motivo(self):
        bronze = self.spark.createDataFrame(
            [
                ("O1", 20100000001, "20200000002", None, "lima", 2025, 1),
                ("O2", 20100000001, "20200000002", -5.0, "lima", 2025, 1),
                ("O3", 20100000001, "20200000002", 0.0, "lima", 2025, 1),
            ],
            "orden string, ruc_entidad long, ruc_contratista string, "
            "monto_total_orden_original double, departamento_entidad string, anio int, mes int",
        )
        silver, cuarentena = separar_ordenes_compra(bronze)

        self.assertEqual([r["orden"] for r in silver.collect()], ["O3"])
        motivos = {r["orden"]: r[MOTIVO_COL] for r in cuarentena.collect()}
        self.assertEqual(motivos, {"O1": "monto_nulo", "O2": "monto_negativo"})

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

    def test_silver_escrito_debe_coincidir_con_el_reporte(self):
        reporte = self._reporte()
        # Mismos conteos por mes que filas_silver: no lanza.
        esperado = [
            (r["anio"], r["mes"], r["filas_silver"]) for r in reporte.collect()
        ]
        completo = self.spark.createDataFrame(
            [(a, m) for a, m, n in esperado for _ in range(n)], "anio int, mes int"
        )
        verificar_silver_escrito(reporte, completo)

        # Falta un mes entero (el caso real de 5 de 12 meses): lanza.
        sin_febrero = completo.filter("mes != 2")
        with self.assertRaises(SilverIncompletoError):
            verificar_silver_escrito(reporte, sin_febrero)


if __name__ == "__main__":
    unittest.main()
