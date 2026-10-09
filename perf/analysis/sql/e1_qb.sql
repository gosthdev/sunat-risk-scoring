-- Qb: Activos por departamento de un único mes específico
-- Filtro adicional por Estado = 'ACTIVO' sobre una partición mensual
SELECT
    Departamento,
    count(*) AS total_activos
FROM ${TABLE}
WHERE cast(anio as integer) = 2024 AND cast(mes as integer) = 12 AND Estado = 'ACTIVO'
GROUP BY Departamento
ORDER BY total_activos DESC;
