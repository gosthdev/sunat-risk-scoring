import unittest
from unittest.mock import MagicMock, patch

from spark_session_factory import create_spark_session


class TestSparkSessionFactory(unittest.TestCase):
    @patch("spark_session_factory.SparkSession")
    def test_create_spark_session_configs(self, mock_spark):
        builder = MagicMock()
        mock_spark.builder.appName.return_value = builder
        builder.config.return_value = builder
        mock_session = MagicMock()
        builder.getOrCreate.return_value = mock_session

        session = create_spark_session("test_app_unit")

        mock_spark.builder.appName.assert_called_once_with("test_app_unit")
        self.assertEqual(session, mock_session)

        # Extraer todas las configuraciones aplicadas
        applied_configs = dict(call.args for call in builder.config.call_args_list)

        # Validaciones de configuración general
        self.assertEqual(applied_configs.get("spark.sql.shuffle.partitions"), "64")
        self.assertEqual(
            applied_configs.get("spark.sql.sources.partitionOverwriteMode"), "dynamic"
        )
        self.assertEqual(
            applied_configs.get("spark.sql.parquet.compression.codec"), "snappy"
        )
        self.assertEqual(
            applied_configs.get("spark.sql.session.timeZone"), "America/Lima"
        )

        # Validaciones de rebase mode para compatibilidad con fechas antiguas
        self.assertEqual(
            applied_configs.get("spark.sql.parquet.datetimeRebaseModeInWrite"),
            "CORRECTED",
        )
        self.assertEqual(
            applied_configs.get("spark.sql.parquet.datetimeRebaseModeInRead"),
            "CORRECTED",
        )
        self.assertEqual(
            applied_configs.get("spark.sql.parquet.int96RebaseModeInWrite"), "CORRECTED"
        )
        self.assertEqual(
            applied_configs.get("spark.sql.parquet.int96RebaseModeInRead"), "CORRECTED"
        )
        self.assertEqual(
            applied_configs.get("spark.sql.avro.datetimeRebaseModeInWrite"), "CORRECTED"
        )
        self.assertEqual(
            applied_configs.get("spark.sql.avro.datetimeRebaseModeInRead"), "CORRECTED"
        )
        self.assertEqual(
            applied_configs.get("spark.sql.parquet.enableVectorizedReader"), "false"
        )
        self.assertEqual(applied_configs.get("spark.sql.parquet.mergeSchema"), "true")


if __name__ == "__main__":
    unittest.main()
