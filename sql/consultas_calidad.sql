-- =====================================================================
-- CONSULTAS DE CALIDAD DE DATOS (Punto 6 - Dimensiones de Calidad)
-- Para documentar en la memoria/notebook con resultados antes y después
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. DIMENSIÓN: COMPLETITUD
-- Porcentaje de disponibilidad por campo y recuento de nulos.
-- ---------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_registros,
    ROUND(100.0 * COUNT(mag) / COUNT(*), 2) AS completitud_mag_pct,
    COUNT(*) FILTER (WHERE mag IS NULL) AS nulos_mag,
    ROUND(100.0 * COUNT(depth_km) / COUNT(*), 2) AS completitud_depth_pct,
    COUNT(*) FILTER (WHERE depth_km IS NULL) AS nulos_depth,
    ROUND(100.0 * COUNT(place) / COUNT(*), 2) AS completitud_place_pct,
    COUNT(*) FILTER (WHERE place IS NULL OR place = '') AS nulos_place,
    ROUND(100.0 * COUNT(country_id) / COUNT(*), 2) AS completitud_pais_pct,
    COUNT(*) FILTER (WHERE country_id IS NULL) AS eventos_sin_pais_o_maritimos
FROM eventos_actuales;


-- ---------------------------------------------------------------------
-- 2. DIMENSIÓN: UNICIDAD
-- Comprueba que no hay identificadores duplicados en el estado actual
-- y analiza cuántas versiones históricas existen por evento.
-- ---------------------------------------------------------------------
-- Verificación de clave primaria en eventos_actuales:
SELECT 
    id, 
    COUNT(*) AS apariciones
FROM eventos_actuales
GROUP BY id
HAVING COUNT(*) > 1;

-- Distribución de revisiones por evento en eventos_versiones:
SELECT 
    num_versiones, 
    COUNT(*) AS total_eventos
FROM (
    SELECT id, COUNT(*) AS num_versiones 
    FROM eventos_versiones 
    GROUP BY id
) v
GROUP BY num_versiones
ORDER BY num_versiones DESC;


-- ---------------------------------------------------------------------
-- 3. DIMENSIÓN: VALIDEZ
-- Coordenadas finitas en rango WGS84, y eventos enviados a cuarentena.
-- ---------------------------------------------------------------------
-- Comprobación de que no hay coordenadas fuera de rango en la tabla limpia:
SELECT COUNT(*) AS coordenadas_invalidas
FROM eventos_actuales
WHERE lon < -180 OR lon > 180 OR lat < -90 OR lat > 90;

-- Resumen de la tabla de cuarentena:
SELECT 
    motivo, 
    COUNT(*) AS cantidad, 
    MIN(recibido_en) AS primer_caso,
    MAX(recibido_en) AS ultimo_caso
FROM eventos_cuarentena
GROUP BY motivo;


-- ---------------------------------------------------------------------
-- 4. DIMENSIÓN: CONSISTENCIA Y ANOMALÍAS
-- Magnitudes o profundidades negativas, consistencia de claves de país.
-- ---------------------------------------------------------------------
SELECT 
    COUNT(*) FILTER (WHERE mag < 0) AS magnitudes_negativas,
    COUNT(*) FILTER (WHERE depth_km < 0) AS profundidades_negativas,
    COUNT(*) FILTER (WHERE mag > 9.5) AS magnitudes_extremas,
    COUNT(*) FILTER (WHERE depth_km > 700) AS profundidades_ultrafundas
FROM eventos_actuales;

-- Detalle de los casos anómalos pero válidos (física sismológica):
SELECT id, place, mag, depth_km, country_name
FROM eventos_actuales
WHERE mag < 0 OR depth_km < 0
LIMIT 10;


-- ---------------------------------------------------------------------
-- 5. DIMENSIÓN: ACTUALIDAD Y LATENCIA
-- Diferencia temporal entre ocurrencia, publicación USGS y procesamiento.
-- ---------------------------------------------------------------------
SELECT 
    -- Retraso en publicación USGS (updated - time)
    ROUND(AVG((updated_ms - time_ms) / 1000.0 / 60.0), 2) AS avg_retraso_usgs_minutos,
    ROUND(MAX((updated_ms - time_ms) / 1000.0 / 60.0), 2) AS max_retraso_usgs_minutos,
    -- Retraso de ingesta respecto a la última actualización USGS (ingested_at - updated)
    ROUND(AVG((ingested_at_ms - updated_ms) / 1000.0 / 60.0), 2) AS avg_latencia_ingesta_minutos,
    -- Retraso total desde la ocurrencia hasta el procesamiento en la base de datos
    ROUND(AVG((EXTRACT(EPOCH FROM procesado_en) * 1000 - time_ms) / 1000.0 / 60.0), 2) AS avg_antiguedad_evento_minutos
FROM eventos_actuales;


-- ---------------------------------------------------------------------
-- 6. AUDITORÍA DE TRANSFORMACIONES
-- Registro de impacto de transformaciones en cada lote.
-- ---------------------------------------------------------------------
SELECT 
    log_id,
    registrado_en,
    lote_size,
    insertados,
    actualizados,
    omitidos_duplicado,
    cuarentena,
    sin_pais,
    depth_negativas
FROM transformaciones_log
ORDER BY log_id DESC
LIMIT 10;
