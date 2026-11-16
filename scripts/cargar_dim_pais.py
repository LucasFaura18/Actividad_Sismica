"""
Carga el catálogo geográfico de Natural Earth (paises.geojson)
en la tabla dimensional dim_pais de PostgreSQL.
"""

import json
import asyncio
from pathlib import Path
import asyncpg
from src.config import ajustes

GEOJSON_FILE = Path(__file__).parent.parent / "recursos" / "observatorio-sismico" / "frontend" / "public" / "datos" / "paises.geojson"


async def main():
    if not GEOJSON_FILE.exists():
        print(f"Error: No existe {GEOJSON_FILE}")
        return

    with open(GEOJSON_FILE, encoding="utf-8") as f:
        datos = json.load(f)

    # Conectar a Postgres en localhost (puerto 5432 expuesto)
    # Reemplazar host por 127.0.0.1 si estamos ejecutando desde el host
    dsn = ajustes.postgres_dsn.replace("@postgres:", "@127.0.0.1:")
    conn = await asyncpg.connect(dsn)

    # Crear tabla si no existe
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

    print(f"Cargados {insertados} países en dim_pais")

    # Intentar aplicar la FK si no existe
    try:
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
        print("Restricción de clave foránea fk_eventos_pais verificada")
    except Exception as exc:
        print(f"Aviso al aplicar FK: {exc}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
