-- Consultas para comprobar las métricas de calidad de datos

-- 1. Completitud: porcentaje de campos presentes y recuento de nulos
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

-- 2. Unicidad: verificar que no hay duplicados de id en el estado actual
SELECT 
    id, 
    COUNT(*) AS apariciones
FROM eventos_actuales
GROUP BY id
HAVING COUNT(*) > 1;

-- Historial de versiones registradas por sismo
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

-- 3. Validez espacial y eventos aislados en cuarentena
SELECT COUNT(*) AS coordenadas_invalidas
FROM eventos_actuales
WHERE lon < -180 OR lon > 180 OR lat < -90 OR lat > 90;

SELECT 
    motivo, 
    COUNT(*) AS cantidad, 
    MIN(recibido_en) AS primer_caso,
    MAX(recibido_en) AS ultimo_caso
FROM eventos_cuarentena
GROUP BY motivo;

-- 4. Anomalías físicas registradas (magnitudes y profundidades negativas)
SELECT 
    COUNT(*) FILTER (WHERE mag < 0) AS magnitudes_negativas,
    COUNT(*) FILTER (WHERE depth_km < 0) AS profundidades_negativas,
    COUNT(*) FILTER (WHERE mag > 9.5) AS magnitudes_extremas,
    COUNT(*) FILTER (WHERE depth_km > 700) AS profundidades_ultrafundas
FROM eventos_actuales;

-- 5. Tiempos de retraso e ingesta
SELECT 
    ROUND(AVG((updated_ms - time_ms) / 1000.0 / 60.0), 2) AS avg_retraso_usgs_minutos,
    ROUND(MAX((updated_ms - time_ms) / 1000.0 / 60.0), 2) AS max_retraso_usgs_minutos,
    ROUND(AVG((ingested_at_ms - updated_ms) / 1000.0 / 60.0), 2) AS avg_latencia_ingesta_minutos,
    ROUND(AVG((EXTRACT(EPOCH FROM procesado_en) * 1000 - time_ms) / 1000.0 / 60.0), 2) AS avg_antiguedad_evento_minutos
FROM eventos_actuales;

-- 6. Últimos lotes procesados por el consumidor
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
