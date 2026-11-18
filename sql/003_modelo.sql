-- Modelo de eventos procesados, versiones y cuarentena

-- Estado vigente de cada terremoto
CREATE TABLE IF NOT EXISTS eventos_actuales (
    id              TEXT            PRIMARY KEY,
    time_ms         BIGINT          NOT NULL,
    updated_ms      BIGINT          NOT NULL,
    ingested_at_ms  BIGINT          NOT NULL,
    procesado_en    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    mag             NUMERIC(5,2),
    mag_type        TEXT,
    place           TEXT,
    lon             DOUBLE PRECISION NOT NULL,
    lat             DOUBLE PRECISION NOT NULL,
    depth_km        DOUBLE PRECISION,
    country_id      TEXT,
    country_name    TEXT,
    continent       TEXT,
    subregion       TEXT,
    source          TEXT            NOT NULL DEFAULT 'usgs_live',
    mode            TEXT            NOT NULL DEFAULT 'live',
    -- Flags para marcar valores nulos o atípicos sin descartar la fila
    mag_nula        BOOLEAN         NOT NULL DEFAULT FALSE,
    mag_negativa    BOOLEAN         NOT NULL DEFAULT FALSE,
    depth_nula      BOOLEAN         NOT NULL DEFAULT FALSE,
    depth_negativa  BOOLEAN         NOT NULL DEFAULT FALSE,
    place_nulo      BOOLEAN         NOT NULL DEFAULT FALSE,
    sin_pais        BOOLEAN         NOT NULL DEFAULT FALSE
);

-- Índices para acelerar las consultas de la API
CREATE INDEX IF NOT EXISTS idx_ev_act_time    ON eventos_actuales (time_ms DESC);
CREATE INDEX IF NOT EXISTS idx_ev_act_country ON eventos_actuales (country_id);
CREATE INDEX IF NOT EXISTS idx_ev_act_mag     ON eventos_actuales (mag);
CREATE INDEX IF NOT EXISTS idx_ev_act_mode    ON eventos_actuales (mode);

-- Histórico de versiones recibidas por sismo
CREATE TABLE IF NOT EXISTS eventos_versiones (
    id              TEXT            NOT NULL,
    updated_ms      BIGINT          NOT NULL,
    procesado_en    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source          TEXT            NOT NULL DEFAULT 'usgs_live',
    mode            TEXT            NOT NULL DEFAULT 'live',
    mag             NUMERIC(5,2),
    mag_type        TEXT,
    place           TEXT,
    lon             DOUBLE PRECISION NOT NULL,
    lat             DOUBLE PRECISION NOT NULL,
    depth_km        DOUBLE PRECISION,
    PRIMARY KEY (id, updated_ms)
);

CREATE INDEX IF NOT EXISTS idx_ev_ver_id ON eventos_versiones (id);

-- Eventos descartados por fallos de validación (coordenadas fuera de rango o JSON corrupto)
CREATE TABLE IF NOT EXISTS eventos_cuarentena (
    cuarentena_id   BIGSERIAL       PRIMARY KEY,
    id              TEXT,
    updated_ms      BIGINT,
    motivo          TEXT            NOT NULL,
    detalle         TEXT,
    recibido_en     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source          TEXT,
    payload_raw     JSONB           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cuarentena_motivo ON eventos_cuarentena (motivo);

-- Registro de transformaciones por lote
CREATE TABLE IF NOT EXISTS transformaciones_log (
    log_id              BIGSERIAL   PRIMARY KEY,
    registrado_en       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lote_size           INT         NOT NULL,
    insertados          INT         NOT NULL DEFAULT 0,
    actualizados        INT         NOT NULL DEFAULT 0,
    omitidos_duplicado  INT         NOT NULL DEFAULT 0,
    cuarentena          INT         NOT NULL DEFAULT 0,
    mag_nulas           INT         NOT NULL DEFAULT 0,
    mag_negativas       INT         NOT NULL DEFAULT 0,
    depth_nulas         INT         NOT NULL DEFAULT 0,
    depth_negativas     INT         NOT NULL DEFAULT 0,
    place_nulos         INT         NOT NULL DEFAULT 0,
    sin_pais            INT         NOT NULL DEFAULT 0
);
