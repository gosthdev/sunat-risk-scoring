-- Conteo de RUCs activos por departamento y porcentaje sobre el total nacional.
-- Fuente: ssco_catalog.gold_regional_summary
SELECT
    departamento,
    ruc_activos,
    ROUND(
        ruc_activos * 100.0 / NULLIF(SUM(ruc_activos) OVER (), 0),
        2
    ) AS pct_del_total
FROM gold_regional_summary
ORDER BY ruc_activos DESC;
