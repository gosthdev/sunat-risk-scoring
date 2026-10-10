-- Verifica que la tabla tiene exactamente una fila por RUC.
-- Si total_rows != distinct_rucs hay RUCs duplicados → error en job 06.
--
-- Cómo correrla: pegar en Athena, seleccionar base de datos sunat_ssco.
-- Comparar con: el log que imprime job 06 al terminar.

SELECT
    COUNT(*)             AS total_rows,
    COUNT(DISTINCT ruc)  AS distinct_rucs,
    COUNT(*) - COUNT(DISTINCT ruc) AS duplicated_rucs  -- debe ser 0
FROM sunat_ssco.model_inputs
WHERE dataset_version = 'v1';