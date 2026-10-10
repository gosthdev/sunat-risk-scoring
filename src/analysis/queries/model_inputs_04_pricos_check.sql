-- Verifica que ningún RUC del dataset de entrenamiento es un PRICO.
-- Según D2: los PRICOS se excluyen de la población antes de entrenar.
-- Si este conteo devuelve > 0, hay un error en job 06 (filtro no aplicado).
--
-- Comparar con: contrato C1 (regla "sin PRICOS").

SELECT
    COUNT(*) AS rucs_in_pricos  -- debe ser 0
FROM sunat_ssco.model_inputs mi
INNER JOIN sunat_ssco.pricos p
    ON mi.ruc = p.ruc
WHERE mi.dataset_version = 'v1';

-- Si el resultado es 0: ✓ correcto, ningún PRICO se filtró.
-- Si el resultado es > 0: abrir issue para Dev A con los RUCs encontrados.
-- Query para investigar cuáles son (solo si el resultado anterior > 0):
--
-- SELECT mi.ruc, mi.label, mi.split
-- FROM sunat_ssco.model_inputs mi
-- INNER JOIN sunat_ssco.pricos p ON mi.ruc = p.ruc
-- WHERE mi.dataset_version = 'v1'
-- LIMIT 20;