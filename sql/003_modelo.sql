-- Modelo de datos procesados (Punto 6 — Calidad y transformación).
-- Todas las tablas usan CREATE TABLE IF NOT EXISTS: seguras de re-ejecutar.

-- ──────────────────────────────────────────────────────────────────────
-- Estado actual: una fila por evento, siempre la versión más reciente.
-- La API consulta esta tabla para /api/eventos.
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eventos_actuales (
    id              TEXT            PRIMARY KEY,

    -- Tiempos en ms epoch UTC (conservados tal como llegan de USGS).
    -- No se convierte zona horaria: epoch UTC es independiente de zona.
    time_ms         BIGINT          NOT NULL,   -- momento del terremoto
    updated_ms      BIGINT          NOT NULL,   -- última revisión en USGS
    ingested_at_ms  BIGINT          NOT NULL,   -- cuándo lo publicó el productor
    procesado_en    TIMESTAMPTZ     NOT NULL DEFAULT NOW(), -- cuándo lo procesó el consumidor

    -- Magnitud: NULL si no disponible. Se conserva (no se rellena con 0).
    -- Una magnitud negativa no es automáticamente un error (flag, no rechazo).
    mag             NUMERIC(5,2),
    mag_type        TEXT,

    -- Localización textual: NULL afecta a completitud pero no invalida el evento.
    place           TEXT,

    -- Coordenadas WGS84; obligatorias (inválidas → cuarentena).
    lon             DOUBLE PRECISION NOT NULL,
    lat             DOUBLE PRECISION NOT NULL,

    -- Profundidad en km: NULL si no disponible.
    -- Los valores negativos son válidos en USGS (eventos muy superficiales);
    -- se conservan con flag pero no se rechazan.
    depth_km        DOUBLE PRECISION,

    -- Enriquecimiento geográfico (punto-en-polígono con Natural Earth).
    -- NULL = evento marítimo, simplificación de costa o sin cobertura. No es error.
    country_id      TEXT,
    country_name    TEXT,
    continent       TEXT,
    subregion       TEXT,

    -- Metadatos de ingesta
    source          TEXT            NOT NULL DEFAULT 'usgs_live',
    mode            TEXT            NOT NULL DEFAULT 'live',

    -- Flags de calidad (para análisis; no filtran el evento)
    mag_nula        BOOLEAN         NOT NULL DEFAULT FALSE,
    mag_negativa    BOOLEAN         NOT NULL DEFAULT FALSE,
    depth_nula      BOOLEAN         NOT NULL DEFAULT FALSE,
    depth_negativa  BOOLEAN         NOT NULL DEFAULT FALSE,
    place_nulo      BOOLEAN         NOT NULL DEFAULT FALSE,
    sin_pais        BOOLEAN         NOT NULL DEFAULT FALSE
);

-- Índices para los filtros de /api/eventos y /api/estadisticas
CREATE INDEX IF NOT EXISTS idx_ev_act_time    ON eventos_actuales (time_ms DESC);
CREATE INDEX IF NOT EXISTS idx_ev_act_country ON eventos_actuales (country_id);
CREATE INDEX IF NOT EXISTS idx_ev_act_mag     ON eventos_actuales (mag);
CREATE INDEX IF NOT EXISTS idx_ev_act_mode    ON eventos_actuales (mode);

-- ──────────────────────────────────────────────────────────────────────
-- Histórico de versiones recibidas por evento.
-- Permite auditar revisiones y detectar regresiones (updated < anterior).
-- ──────────────────────────────────────────────────────────────────────
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

-- ──────────────────────────────────────────────────────────────────────
-- Cuarentena: eventos que no pasan validación.
-- Se conservan íntegros con motivo y referencia al raw original.
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eventos_cuarentena (
    cuarentena_id   BIGSERIAL       PRIMARY KEY,
    id              TEXT,           -- NULL si el id mismo es inválido
    updated_ms      BIGINT,
    motivo          TEXT            NOT NULL,
    detalle         TEXT,
    recibido_en     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source          TEXT,
    payload_raw     JSONB           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cuarentena_motivo ON eventos_cuarentena (motivo);

-- ──────────────────────────────────────────────────────────────────────
-- Log de transformaciones por lote procesado.
-- Registra cuántos registros afecta cada decisión.
-- ──────────────────────────────────────────────────────────────────────
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
