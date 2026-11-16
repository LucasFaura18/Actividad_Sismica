-- Tablas de extracción e ingesta (Punto 5).
-- CREATE TABLE IF NOT EXISTS: seguro de ejecutar más de una vez.
-- Para bases ya existentes: docker compose exec postgres psql -U observatorio observatorio -f /docker-entrypoint-initdb.d/002_extraccion.sql

-- Estado persistente del productor: última versión publicada por evento.
-- Permite comparar id+updated entre reinicios sin perder el historial.
CREATE TABLE IF NOT EXISTS productor_estado (
    id           TEXT        PRIMARY KEY,
    updated_ms   BIGINT      NOT NULL,
    publicado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- GeoJSON original recibido de USGS antes de cualquier transformación.
-- Clave compuesta (id, updated_ms): evita guardar copias idénticas.
-- Retención: el productor limpia filas con recibido_en > RETENCION_RAW_DIAS días.
CREATE TABLE IF NOT EXISTS eventos_raw (
    id          TEXT        NOT NULL,
    updated_ms  BIGINT      NOT NULL,
    recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fuente      TEXT        NOT NULL DEFAULT 'usgs_live',
    payload     JSONB       NOT NULL,
    PRIMARY KEY (id, updated_ms)
);

-- Registro de cada ciclo de consulta al feed USGS.
-- Incluye latencia, conteos y errores para documentar la extracción.
CREATE TABLE IF NOT EXISTS consultas_registro (
    consulta_id       BIGSERIAL   PRIMARY KEY,
    consultado_en     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    url               TEXT        NOT NULL,
    http_status       INT,
    latencia_ms       INT,
    total_features    INT,
    total_terremotos  INT,
    total_excluidos   INT,
    publicados        INT,
    error             TEXT
);

-- Índice para la limpieza por retención
CREATE INDEX IF NOT EXISTS idx_eventos_raw_recibido ON eventos_raw (recibido_en);
