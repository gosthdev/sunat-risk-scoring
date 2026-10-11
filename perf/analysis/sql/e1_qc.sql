-- Qc: Agregado del año completo (Control)
-- Demuestra que una consulta que recorre todos los meses escanea exactamente
-- el mismo volumen de bytes en P que en N (o ligeramente más en P por overhead de archivos)
SELECT
    Estado,
    count(*) AS total_registros,
    count(DISTINCT RUC) AS total_rucs_unicos
FROM ${TABLE}
WHERE cast(anio as integer) = 2025
GROUP BY Estado
ORDER BY total_registros DESC;
