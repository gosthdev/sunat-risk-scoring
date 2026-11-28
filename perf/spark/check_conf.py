"""Muestra qué spark.sql.shuffle.partitions usa realmente el pipeline en EMR.

create_spark_session fija 64 en el código; este script se envía con
--conf spark.sql.shuffle.partitions=<N> para ver cuál de los dos gana.
Se envía con --py-files common.zip, igual que los jobs del pipeline.
"""

import json

from spark_session_factory import create_spark_session

spark = create_spark_session("perf_check_conf")
resultado = {
    "pedido_en_submit": spark.sparkContext.getConf().get(
        "spark.sql.shuffle.partitions", None
    ),
    "efectivo_en_sesion": spark.conf.get("spark.sql.shuffle.partitions"),
    "aqe_activado": spark.conf.get("spark.sql.adaptive.enabled"),
    "aqe_agrupa_particiones": spark.conf.get(
        "spark.sql.adaptive.coalescePartitions.enabled"
    ),
    "version_spark": spark.version,
}
print("PERF_CHECK_CONF " + json.dumps(resultado))
spark.stop()
