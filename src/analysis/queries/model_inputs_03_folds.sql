-- Verifica que los 5 folds de validación cruzada tienen una distribución
-- balanceada de positivos. Si un fold tiene 0 positivos, la métrica de CV
-- de ese fold no tiene sentido y el entrenamiento puede fallar.
--
-- Según D8: fold va de 0 a 4 para split='train'; es NULL para split='test'.
-- Los 5 folds deberían tener un número similar de positivos entre sí.
--
-- Señal de alerta: si un fold tiene 0 positivos, avisar a Dev A.

SELECT
    fold,
    COUNT(*)                                    AS total_rows,
    SUM(label)                                  AS total_positives,
    ROUND(100.0 * SUM(label) / COUNT(*), 4)     AS prevalence_pct
FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v1'
GROUP BY fold
ORDER BY fold;

-- Nota: la fila con fold = NULL corresponde a split='test'.
-- En Athena, NULL se muestra al final del ORDER BY.