-- Dimensión territorial de países cargados desde Natural Earth

CREATE TABLE IF NOT EXISTS dim_pais (
    country_id      TEXT        PRIMARY KEY,
    country_name    TEXT        NOT NULL,
    continent       TEXT,
    subregion       TEXT
);

-- Relación con la tabla principal de eventos (SET NULL para permitir eventos marítimos)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_eventos_pais'
    ) THEN
        ALTER TABLE eventos_actuales
        ADD CONSTRAINT fk_eventos_pais
        FOREIGN KEY (country_id) REFERENCES dim_pais (country_id)
        ON UPDATE CASCADE ON DELETE SET NULL;
    END IF;
END $$;
