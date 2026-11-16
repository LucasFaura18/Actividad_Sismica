-- =====================================================================
-- DIMENSIÓN GEOGRÁFICA: dim_pais (Punto 7 - Integración y modelado)
-- Catálogo derivado de Natural Earth (177 entidades)
-- =====================================================================

CREATE TABLE IF NOT EXISTS dim_pais (
    country_id      TEXT        PRIMARY KEY, -- ADM0_A3
    country_name    TEXT        NOT NULL,    -- NAME_ES o ADMIN
    continent       TEXT,
    subregion       TEXT
);

-- Si la tabla está vacía, se pueden insertar los países o sincronizarlos
-- mediante un script de inicialización desde paises.geojson.

-- Clave foránea en eventos_actuales (relación de dimensión opcional para eventos marítimos)
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
