-- Verifica que la prevalencia (% de SSCO) es similar en train y test.
-- Según D8: el split es estratificado, así que la diferencia entre
-- prevalencia_train y prevalencia_test debe ser < 10% relativo.
--
-- Ejemplo: si prevalencia global es 0.52%, train debería estar entre
-- 0.47% y 0.57%. Fuera de ese rango → el split no fue estratificado.
--
-- Comparar con: contrato C1 (campo "prevalencia esperada") y reporte de job 06.

SELECT
    split,
    COUNT(*)                                                AS total_rows,
    SUM(label)                                              AS total_positives,
    ROUND(100.0 * SUM(label) / COUNT(*), 4)                AS prevalence_pct
FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v1'
GROUP BY split

UNION ALL

-- Fila de totales globales para comparación rápida
SELECT
    'TOTAL'                                                 AS split,
    COUNT(*)                                                AS total_rows,
    SUM(label)                                              AS total_positives,
    ROUND(100.0 * SUM(label) / COUNT(*), 4)                AS prevalence_pct
FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v1'

ORDER BY split;