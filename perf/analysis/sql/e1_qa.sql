-- Qa: Conteo por estado de un único mes específico
-- Demuestra el beneficio de la poda de particiones (P escanea ~1/12 de bytes respecto a N)
SELECT
    Estado,
    count(*) AS total_contribuyentes
FROM ${TABLE}
WHERE cast(anio as integer) = 2024 AND cast(mes as integer) = 12
GROUP BY Estado
ORDER BY total_contribuyentes DESC;
