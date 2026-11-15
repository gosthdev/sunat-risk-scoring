-- Vista para QuickSight: top 1000 RUC por score de riesgo SSCO, por corrida y split, con features.
-- rank_en_split reemplaza a rank_global (que mezcla train y test).
-- OJO: split='train' tiene scores inflados (el modelo se ajustó ahí); filtrar split='test' por defecto.
-- antiguedad_contratacion_estado_dias es un proxy (primera orden de compra), no la antigüedad real del RUC.
-- Fuente: sunat_ssco.predictions, model_runs, gold_ruc_features
CREATE OR REPLACE VIEW sunat_ssco.dash_ranking AS
WITH mapa_version AS (
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
    f.antiguedad_contratacion_estado_dias
FROM ranked k
JOIN sunat_ssco.model_runs r
    ON r.run_id = k.run_id
JOIN mapa_version m
    ON m.dataset_version = r.dataset_version
LEFT JOIN sunat_ssco.gold_ruc_features f
    ON CAST(f.mes_referencia AS VARCHAR) = m.mes_referencia
    AND CAST(f.ruc AS VARCHAR) = CAST(k.ruc AS VARCHAR)
WHERE k.rank_en_split <= 1000
