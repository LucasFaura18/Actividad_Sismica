import asyncio
import json
import logging
from pathlib import Path
import asyncpg
from src.config import ajustes

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("cargar_dim_pais")

GEOJSON_PATH = Path("/app/datos/paises.geojson")


async def main():
    if not GEOJSON_PATH.exists():
        log.error("No se encuentra %s", GEOJSON_PATH)
        return

    with open(GEOJSON_PATH, encoding="utf-8") as f:
        datos = json.load(f)

    conn = await asyncpg.connect(ajustes.postgres_dsn)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_pais (
            country_id   TEXT PRIMARY KEY,
            country_name TEXT NOT NULL,
            continent    TEXT,
            subregion    TEXT
        );
    """)

    features = datos.get("features", [])
    insertados = 0
    for f in features:
        props = f.get("properties") or {}
        country_id = props.get("ADM0_A3")
        country_name = props.get("NAME_ES") or props.get("ADMIN")
        continent = props.get("CONTINENT")
        subregion = props.get("SUBREGION")

        if country_id and country_name:
            await conn.execute(
                """
                INSERT INTO dim_pais (country_id, country_name, continent, subregion)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (country_id) DO UPDATE
                SET country_name = EXCLUDED.country_name,
                    continent    = EXCLUDED.continent,
                    subregion    = EXCLUDED.subregion
                """,
                country_id, country_name, continent, subregion
            )
            insertados += 1

    log.info("Cargados %d países en dim_pais", insertados)

    # Clave foránea segura
    await conn.execute("""
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
    """)
    log.info("Clave foránea fk_eventos_pais confirmada")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
