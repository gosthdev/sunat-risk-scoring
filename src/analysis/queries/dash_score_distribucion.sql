-- Vista para QuickSight: distribución de scores por corrida, split y label (20 bins de 0.05).
-- OJO: split='train' tiene scores inflados (el modelo se ajustó ahí); filtrar split='test' por defecto.
-- pr_auc y roc_auc son métricas de test: solo se pegan a las filas split='test' (NULL en train).
-- experimento: id extraído del run_id (p.ej. 'E5'; NULL si el formato no coincide). es_ultima_corrida = 1 si
-- la corrida tiene la fecha más reciente de su (experimento, dataset_version) en model_runs.
-- Fuente: sunat_ssco.predictions, model_runs, model_metrics
CREATE OR REPLACE VIEW sunat_ssco.dash_score_distribucion AS
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
bins AS (
    SELECT
        p.run_id,
        p.split,
        p.label,
        LEAST(CAST(FLOOR(p.score * 20) AS INTEGER), 19) AS bin_idx,
        COUNT(*)                                        AS n_ruc,
        AVG(p.score)                                    AS score_promedio
    FROM sunat_ssco.predictions p
    GROUP BY
        p.run_id,
        p.split,
        p.label,
        LEAST(CAST(FLOOR(p.score * 20) AS INTEGER), 19)
)
SELECT
    b.run_id,
    r.model_name,
    r.variant,
    r.dataset_version,
    r.experimento,
    r.es_ultima_corrida,
    b.split,
    b.label,
    b.bin_idx,
    b.bin_idx / 20.0                                    AS bin_inicio,
    (b.bin_idx + 1) / 20.0                              AS bin_fin,
    b.n_ruc,
    ROUND(b.score_promedio, 4)                          AS score_promedio,
    b.n_ruc * 1.0 / SUM(b.n_ruc) OVER (
        PARTITION BY b.run_id, b.split, b.label
    )                                                   AS pct_en_bin,
    m.pr_auc,
    m.roc_auc
FROM bins b
JOIN runs r
    ON r.run_id = b.run_id
LEFT JOIN sunat_ssco.model_metrics m
    ON m.run_id = b.run_id
    AND b.split = 'test'
