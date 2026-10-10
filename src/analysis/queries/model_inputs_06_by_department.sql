-- Distribuye los SSCO positivos por departamento.
-- Sirve para detectar si algún departamento concentra casi todos los
-- positivos (sesgo geográfico que el modelo puede sobreajustar).
-- También confirma que la normalización de departamentos funcionó:
-- no deben aparecer variantes como "LIMA " o "Lima" por separado.
--
-- Comparar con: lista oficial SSCO (¿hay concentración en Lima?).

SELECT
    departamento,
    COUNT(*)                                        AS total_rucs,
    SUM(label)                                      AS total_positives,
    ROUND(100.0 * SUM(label) / COUNT(*), 4)         AS prevalence_pct,
    ROUND(100.0 * SUM(label) / SUM(SUM(label)) OVER (), 2) AS pct_of_all_positives
FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v1'
GROUP BY departamento
ORDER BY total_positives DESC;

-- Si aparece un departamento con nombre en minúsculas o con tilde,
-- la normalización de region_normalizer.py no se aplicó bien → avisar a Dev A.