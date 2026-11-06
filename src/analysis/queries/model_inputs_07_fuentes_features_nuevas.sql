-- Cobertura por label de las fuentes de las features nuevas, antes de construirlas.
-- Una feature solo vale si su fuente tiene datos para una fracción útil de los positivos.
--   con_ordenes          -> features temporales y te_entidad (n_ordenes nulo = sin órdenes)
--   con_fecha_orden      -> features temporales (antiguedad sale de la primera fecha_de_emision)
--   con_entidad          -> te_entidad
--   con_ciiu / depto / correlativo -> te_ciiu, te_ciiu_depto, te_cohorte_ruc

SELECT
    split,
    label,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(CASE WHEN n_ordenes > 0 THEN 1 END) / COUNT(*), 2) AS pct_con_ordenes,
    ROUND(100.0 * COUNT(antiguedad_contratacion_estado_dias) / COUNT(*), 2) AS pct_con_fecha_orden,
    ROUND(100.0 * COUNT(CASE WHEN n_entidades_distintas > 0 THEN 1 END) / COUNT(*), 2) AS pct_con_entidad,
    ROUND(100.0 * COUNT(ciiu_principal) / COUNT(*), 2) AS pct_con_ciiu,
    ROUND(100.0 * COUNT(departamento) / COUNT(*), 2) AS pct_con_departamento,
    ROUND(100.0 * COUNT(ruc_correlativo) / COUNT(*), 2) AS pct_con_correlativo,
    COUNT(DISTINCT ciiu_principal) AS n_ciiu,
    COUNT(DISTINCT CAST(ruc_correlativo / 1000 AS BIGINT)) AS n_cohortes
FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v3'
GROUP BY split, label
ORDER BY split, label;
