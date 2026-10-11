-- Leaderboard unificado de experimentos: cruza run_records (C3) con métricas de test (C4).
-- Ordenado por PR-AUC de test descendente; el modelo oficial es el primero de variante V2.
--
-- Uso: ejecutar en Athena workgroup 'sunat-ssco' sobre la base 'sunat_ssco'.
-- Reemplazar <model_version_oficial> con el run_id del modelo campeón antes de publicar.

SELECT
    r.run_id,
    r.model_name,
    r.variant,
    r.dataset_version,
    r.usa_pesos,
    r.metricas_cv.cv_pr_auc_mean   AS cv_pr_auc_mean,
    r.metricas_cv.cv_pr_auc_std    AS cv_pr_auc_std,
    r.n_train,
    r.n_test,
    r.n_positivos_train,
    r.n_positivos_test,
    m.pr_auc,
    m.pr_auc_ic_bajo,
    m.pr_auc_ic_alto,
    m.roc_auc,
    m.recall_at_1pct,
    m.precision_at_1pct,
    m.lift_at_1pct,
    m.recall_at_5pct,
    m.precision_at_5pct,
    m.lift_at_5pct,
    m.recall_lima,
    m.recall_resto,
    m.brier,
    m.tp_1pct,
    m.fp_1pct,
    m.fn_1pct,
    m.tn_1pct,
    m.instancia,
    m.segundos_facturables,
    m.costo_estimado_usd,
    r.fecha
FROM model_runs r
JOIN model_metrics m
    ON r.run_id = m.run_id
ORDER BY
    m.pr_auc DESC NULLS LAST,
    r.fecha DESC;
