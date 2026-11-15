-- Vista para QuickSight: top 1000 RUC por score de riesgo SSCO, por corrida y split, con features.
-- rank_en_split reemplaza a rank_global (que mezcla train y test).
-- OJO: split='train' tiene scores inflados (el modelo se ajustó ahí); filtrar split='test' por defecto.
-- antiguedad_contratacion_estado_dias es un proxy (primera orden de compra), no la antigüedad real del RUC.
-- experimento: id extraído del run_id (p.ej. 'E5'; NULL si el formato no coincide). es_ultima_corrida = 1 si
-- la corrida tiene la fecha más reciente de su (experimento, dataset_version) en model_runs.
-- Fuente: sunat_ssco.predictions, model_runs, gold_ruc_features
CREATE OR REPLACE VIEW sunat_ssco.dash_ranking AS
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
mapa_version AS (
    SELECT * FROM (VALUES
        ('v1', '202506'),
        ('v2', '202512'),
        ('v3', '202603')
    ) AS t (dataset_version, mes_referencia)
),
ranked AS (
    SELECT
        run_id,
        split,
        ruc,
        score,
        label,
        departamento,
        contrata_con_estado,
        ROW_NUMBER() OVER (
            PARTITION BY run_id, split
            ORDER BY score DESC, ruc ASC
        ) AS rank_en_split,
        COUNT(*) OVER (
            PARTITION BY run_id, split
        ) AS n_en_split
    FROM sunat_ssco.predictions
)
SELECT
    k.run_id,
    r.model_name,
    r.variant,
    r.dataset_version,
    r.experimento,
    r.es_ultima_corrida,
    k.split,
    k.ruc,
    k.score,
    k.label,
    k.rank_en_split,
    k.rank_en_split * 1.0 / k.n_en_split        AS percentil_rank,
    k.departamento,
    k.contrata_con_estado,
    f.tipo_contribuyente,
    f.ciiu_principal,
    f.nro_trabajadores,
    f.n_ordenes,
    f.monto_total_soles,
    f.n_entidades_distintas,
    f.es_prico,
    f.see_afiliado,
    f.antiguedad_contratacion_estado_dias,
    f.see_antiguedad_dias,
    f.see_n_tipos,
    f.cambio_estado,
    f.cambio_tipo_facturacion,
    f.meses_hueco,
    f.nuevo_en_periodo,
    f.meses_en_padron
FROM ranked k
JOIN runs r
    ON r.run_id = k.run_id
JOIN mapa_version m
    ON m.dataset_version = r.dataset_version
LEFT JOIN sunat_ssco.gold_ruc_features f
    ON CAST(f.mes_referencia AS VARCHAR) = m.mes_referencia
    AND CAST(f.ruc AS VARCHAR) = CAST(k.ruc AS VARCHAR)
WHERE k.rank_en_split <= 1000
