-- Vista para QuickSight: resumen regional (EPEN, PRICOS, recaudación) + riesgo del modelo por departamento, corrida y split.
-- Las columnas regionales (ruc_activos, pct_informalidad, recaudacion_soles, concentracion_pricos) vienen de un
-- snapshot único (gold_regional_summary) y se repiten por run_id/split: en QuickSight usar MAX/AVG, no SUM.
-- OJO: split='train' tiene scores inflados (el modelo se ajustó ahí); filtrar split='test' por defecto.
-- Departamentos sin predicciones salen con run_id NULL.
-- experimento: id extraído del run_id (p.ej. 'E5'; NULL si el formato no coincide). es_ultima_corrida = 1 si
-- la corrida tiene la fecha más reciente de su (experimento, dataset_version) en model_runs.
-- Fuente: sunat_ssco.gold_regional_summary, predictions, model_runs
CREATE OR REPLACE VIEW sunat_ssco.dash_regional AS
WITH runs AS (
    -- shortcut: este CTE se repite en las 3 vistas dash_*; extraer a una vista común si se añade otra.
    SELECT
        x.run_id,
        x.model_name,
        x.variant,
        x.dataset_version,
        x.experimento,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY COALESCE(x.experimento, x.run_id), x.dataset_version
                ORDER BY x.fecha DESC, x.run_id DESC
            ) = 1 THEN 1
            ELSE 0
        END AS es_ultima_corrida
    FROM (
        SELECT
            run_id,
            model_name,
            variant,
            dataset_version,
            fecha,
            REGEXP_EXTRACT(run_id, '^ssco-run-([^-]+)-', 1) AS experimento
        FROM sunat_ssco.model_runs
    ) x
),
ranked AS (
    SELECT
        run_id,
        split,
        UPPER(TRIM(departamento)) AS departamento,
        score,
        label,
        ROW_NUMBER() OVER (
            PARTITION BY run_id, split
            ORDER BY score DESC, ruc ASC
        ) AS rank_en_split,
        COUNT(*) OVER (
            PARTITION BY run_id, split
        ) AS n_en_split
    FROM sunat_ssco.predictions
),
modelo AS (
    SELECT
        run_id,
        split,
        departamento,
        COUNT(*)                                                             AS n_ruc_scoreados,
        SUM(label)                                                           AS n_ssco_reales,
        AVG(score)                                                           AS score_promedio,
        SUM(CASE WHEN rank_en_split <= n_en_split * 0.01 THEN 1 ELSE 0 END)  AS n_en_top_1pct
    FROM ranked
    GROUP BY run_id, split, departamento
)
SELECT
    g.departamento,
    g.ruc_activos,
    g.pct_informalidad,
    g.recaudacion_soles,
    g.concentracion_pricos,
    ROUND(g.concentracion_pricos * 100.0 / NULLIF(g.ruc_activos, 0), 4)  AS pct_pricos_sobre_activos,
    md.run_id,
    r.model_name,
    r.variant,
    r.dataset_version,
    r.experimento,
    r.es_ultima_corrida,
    md.split,
    md.n_ruc_scoreados,
    md.n_ssco_reales,
    ROUND(md.score_promedio, 4)                                          AS score_promedio,
    md.n_en_top_1pct,
    md.n_en_top_1pct * 1.0 / NULLIF(md.n_ruc_scoreados, 0)               AS pct_top_1pct_del_depto
FROM sunat_ssco.gold_regional_summary g
LEFT JOIN modelo md
    ON UPPER(TRIM(g.departamento)) = md.departamento
LEFT JOIN runs r
    ON r.run_id = md.run_id
