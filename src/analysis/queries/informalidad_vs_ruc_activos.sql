-- Cruce EPEN (informalidad laboral) vs RUCs activos por departamento.
-- Clasifica cada región en 4 cuadrantes respecto a las medianas nacionales.
-- Fuente: ssco_catalog.gold_regional_summary
WITH medianas AS (
    SELECT
        APPROX_PERCENTILE(ruc_activos, 0.5)      AS mediana_ruc,
        APPROX_PERCENTILE(pct_informalidad, 0.5) AS mediana_informalidad
    FROM gold_regional_summary
)
SELECT
    g.departamento,
    g.ruc_activos,
    g.pct_informalidad,
    CASE
        WHEN g.ruc_activos >= m.mediana_ruc AND g.pct_informalidad >= m.mediana_informalidad
            THEN 'Alto RUC / Alta Informalidad'
        WHEN g.ruc_activos >= m.mediana_ruc AND g.pct_informalidad < m.mediana_informalidad
            THEN 'Alto RUC / Baja Informalidad'
        WHEN g.ruc_activos < m.mediana_ruc AND g.pct_informalidad >= m.mediana_informalidad
            THEN 'Bajo RUC / Alta Informalidad'
        ELSE 'Bajo RUC / Baja Informalidad'
    END AS cuadrante,
    m.mediana_ruc,
    m.mediana_informalidad
FROM gold_regional_summary g
CROSS JOIN medianas m
ORDER BY g.pct_informalidad DESC;
