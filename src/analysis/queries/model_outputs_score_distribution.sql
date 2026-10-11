-- Distribución de scores por clase (label 0 vs label 1) del modelo oficial.
-- Histograma de 20 bins entre 0 y 1; útil para detectar separación y calibración.
--
-- Reemplazar 'run_E1_LR_V2' con el run_id del modelo campeón antes de ejecutar.

SELECT
    label,
    FLOOR(score * 20) / 20.0        AS bin_inicio,
    FLOOR(score * 20) / 20.0 + 0.05 AS bin_fin,
    COUNT(*)                         AS n_ruc,
    ROUND(AVG(score), 4)             AS score_promedio
FROM predictions
WHERE
    run_id    = 'run_E1_LR_V2'
    AND split = 'test'
GROUP BY
    label,
    FLOOR(score * 20)
ORDER BY
    label ASC,
    bin_inicio ASC;
