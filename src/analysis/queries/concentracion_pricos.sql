-- Concentración de Principales Contribuyentes (PRICOS) por departamento y su peso en recaudación regional.
-- Fuente: ssco_catalog.gold_regional_summary
SELECT
    departamento,
    concentracion_pricos,
    ruc_activos,
    ROUND(
        concentracion_pricos * 100.0 / NULLIF(ruc_activos, 0),
        4
    ) AS pct_pricos_sobre_activos,
    recaudacion_soles,
    ROUND(
        recaudacion_soles * 100.0 / NULLIF(SUM(recaudacion_soles) OVER (), 0),
        2
    ) AS pct_recaudacion_regional
FROM gold_regional_summary
ORDER BY pct_pricos_sobre_activos DESC;
