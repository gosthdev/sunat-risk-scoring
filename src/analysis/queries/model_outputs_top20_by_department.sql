-- Top-20 RUC con mayor score de riesgo SSCO, agrupados por departamento.
-- Solo evalúa el conjunto de test del modelo oficial (run_id campeón).
--
-- Reemplazar 'run_E1_LR_V2' con el run_id del modelo campeón antes de ejecutar.

SELECT
    departamento,
    ruc,
    score,
    rank_global,
    label
FROM (
    SELECT
        departamento,
        ruc,
        score,
        rank_global,
        label,
        ROW_NUMBER() OVER (
            PARTITION BY departamento
            ORDER BY score DESC
        ) AS rank_en_dept
    FROM predictions
    WHERE
        run_id     = 'run_E1_LR_V2'
        AND split  = 'test'
        AND departamento IS NOT NULL
) ranked
WHERE rank_en_dept <= 20
ORDER BY
    departamento ASC,
    score DESC;
