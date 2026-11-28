"""El replicador genera copias que el Job 01 real lee sin errores y con claves únicas."""

import importlib
import importlib.util
import shutil
import tempfile
import unittest
from pathlib import Path

HAS_SPARK = (
    shutil.which("java") is not None and importlib.util.find_spec("pyspark") is not None
)

PADRON = [
    ("20100000001", "ACTIVO", "202501"),
    ("10456789012", "ACTIVO", "202501"),
    ("20100000002", "BAJA", "202501"),
    ("no-es-ruc", "ACTIVO", "202501"),  # el Job 01 la manda a cuarentena: no se copia
]
ORDENES = [
    ("20100000001", "OC-1", "4600000000"),  # contratista que está en el Padrón
    ("12345678", "OC-2", "17"),  # DNI: no se toca
]


@unittest.skipUnless(HAS_SPARK, "requiere Java y PySpark")
class TestReplicadorLocal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-replicador")
            .getOrCreate()
        )
        cls.replicador = importlib.import_module("s_replicate_dataset")
        cls.bronze_io = importlib.import_module("bronze_io")
        cls.esquemas = importlib.import_module("schema_definitions")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _csv(self, carpeta: Path, esquema, valores: list[dict]) -> None:
        carpeta.mkdir(parents=True)
        nombres = esquema.fieldNames()
        lineas = [",".join(nombres)] + [
            ",".join(v.get(n, "") for n in nombres) for v in valores
        ]
        (carpeta / "datos.csv").write_text("\n".join(lineas) + "\n", encoding="utf-8")

    def _raw(self, raiz: Path) -> None:
        self._csv(
            raiz / "padron_ruc" / "anio=2025" / "mes=01",
            self.esquemas.PADRON_RUC_SCHEMA,
            [{"RUC": r, "Estado": e, "PERIODO_PUBLICACION": p} for r, e, p in PADRON],
        )
        self._csv(
            raiz / "ordenes_compra" / "anio=2025" / "mes=01",
            self.esquemas.ORDENES_COMPRA_SCHEMA,
            [
                {
                    "ruc_contratista": c,
                    "orden": o,
                    "nro_de_orden": n,
                    "monto_total_orden_original": "10.5",
                    "fecha_registro": "2025-01-15",
                }
                for c, o, n in ORDENES
            ],
        )

    def test_copias_legibles_por_el_job_01_con_claves_unicas(self):
        with tempfile.TemporaryDirectory() as tmp:
            raiz = Path(tmp)
            self._raw(raiz / "raw")
            destino = (raiz / "perf").as_uri()
            reporte = self.replicador.replicar(
                self.spark, (raiz / "raw").as_uri(), destino, factor=3
            )
            self.assertEqual(reporte["rucs_distintos"], 3)

            self.assertTrue(
                (raiz / "perf" / "padron_ruc" / "anio=2025" / "mes=01").is_dir()
            )
            padron = self.bronze_io.leer_raw(
                self.spark,
                f"{destino}/padron_ruc",
                self.esquemas.PADRON_RUC_SCHEMA,
                None,
            )
            self.assertEqual(padron.filter("_corrupt_record IS NOT NULL").count(), 0)
            rucs = [str(f.RUC) for f in padron.collect()]
            self.assertEqual(len(rucs), 6)  # 3 filas válidas x copias 1 y 2
            self.assertEqual(len(set(rucs)), 6)
            self.assertTrue(all(len(r) == 11 and int(r) >= 21 * 10**9 for r in rucs))

            ordenes = self.bronze_io.leer_raw(
                self.spark,
                f"{destino}/ordenes_compra",
                self.esquemas.ORDENES_COMPRA_SCHEMA,
                None,
            ).collect()
            self.assertEqual(len(ordenes), 4)
            self.assertTrue(all(o._corrupt_record is None for o in ordenes))
            self.assertEqual(
                {o.orden for o in ordenes}, {"OC-1-R1", "OC-1-R2", "OC-2-R1", "OC-2-R2"}
            )
            self.assertEqual(
                {o.nro_de_orden for o in ordenes if o.orden.startswith("OC-1")},
                {14600000000, 24600000000},
            )
            self.assertEqual(
                {o.ruc_contratista for o in ordenes if o.orden.startswith("OC-2")},
                {"12345678"},
            )
            # Cada orden copiada apunta a un contribuyente copiado del Padrón.
            contratistas = {
                o.ruc_contratista for o in ordenes if o.orden.startswith("OC-1")
            }
            self.assertTrue(contratistas <= set(rucs))
            self.assertEqual(len(contratistas), 2)

    def test_destino_fuera_de_perf_se_rechaza(self):
        with self.assertRaises(ValueError):
            self.replicador.replicar(
                self.spark, "s3://x/raw", "s3://sunat-risk-scoring-raw", factor=2
            )
