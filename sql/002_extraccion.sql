-- Tablas para el estado del productor y guardado de datos raw

-- Guarda la última versión enviada a Kafka por cada evento
CREATE TABLE IF NOT EXISTS productor_estado (
    id           TEXT        PRIMARY KEY,
    updated_ms   BIGINT      NOT NULL,
    publicado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Guarda el GeoJSON original de USGS sin transformar
CREATE TABLE IF NOT EXISTS eventos_raw (
    id          TEXT        NOT NULL,
    updated_ms  BIGINT      NOT NULL,
    recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fuente      TEXT        NOT NULL DEFAULT 'usgs_live',
    payload     JSONB       NOT NULL,
    PRIMARY KEY (id, updated_ms)
);

-- Registro de peticiones al feed de USGS
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

CREATE INDEX IF NOT EXISTS idx_eventos_raw_recibido ON eventos_raw (recibido_en);
