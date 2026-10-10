-- Reporta el porcentaje de nulos por cada feature del dataset.
-- Sirve para detectar features que llegaron vacías o que no se cruzaron bien.
--
-- IMPORTANTE: esta query cubre las columnas conocidas al momento de B1.
-- Cuando Dev A entregue el catálogo completo de features (tarea A2),
-- agregar las columnas nuevas siguiendo el mismo patrón.
--
-- Un nulo en 'label' o 'ruc' es un error crítico → avisar a Dev A.
-- Un nulo en features de órdenes de compra es esperado: muchos RUC
-- no contratan con el Estado (cantidad_contratos_estado = 0 → los demás nulos).

SELECT
    COUNT(*) AS total_rows,

    -- Columnas obligatorias del contrato C1 (deben tener 0% nulos)
    ROUND(100.0 * COUNT(CASE WHEN ruc   IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_ruc,
    ROUND(100.0 * COUNT(CASE WHEN label IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_label,
    ROUND(100.0 * COUNT(CASE WHEN split IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_split,

    -- Features del Padrón RUC
    ROUND(100.0 * COUNT(CASE WHEN estado            IS NULL AND Estado IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_estado,
    ROUND(100.0 * COUNT(CASE WHEN condicion         IS NULL AND Condicion IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_condicion,
    ROUND(100.0 * COUNT(CASE WHEN nro_trabajadores  IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_nro_trabajadores,
    ROUND(100.0 * COUNT(CASE WHEN departamento      IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_departamento,
    ROUND(100.0 * COUNT(CASE WHEN ciiu_principal    IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_ciiu_principal,
    ROUND(100.0 * COUNT(CASE WHEN sin_trabajadores  IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_sin_trabajadores,

    -- Features de Órdenes de Compra (nulos esperados para RUC sin contratos)
    ROUND(100.0 * COUNT(CASE WHEN n_ordenes         IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_n_ordenes,
    ROUND(100.0 * COUNT(CASE WHEN monto_total_soles IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_monto_total_soles


FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v1';