"""
Consumidor: lee eventos de Kafka, valida coordenadas, enriquece con el país
y guarda o actualiza en PostgreSQL.
"""

import asyncio
import json
import logging
import math
from pathlib import Path

import asyncpg
from aiokafka import AIOKafkaConsumer

from src.config import ajustes, esperar
from src.geografia import CatalogoGeografico

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [consumidor] %(message)s",
)
log = logging.getLogger("consumidor")
LISTO = Path("/tmp/ready")


def es_numero_finito(val) -> bool:
    return isinstance(val, (int, float)) and not math.isnan(val) and not math.isinf(val)


async def inicializar_tablas_consumidor(pool: asyncpg.Pool) -> None:
    # Comprobamos que la tabla de eventos existe
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1 FROM eventos_actuales LIMIT 1;")
    log.info("Tablas de eventos verificadas en PostgreSQL")


async def procesar_mensaje(
    pool: asyncpg.Pool,
    catalogo: CatalogoGeografico,
    raw_bytes: bytes,
    stats: dict,
) -> None:
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        # Si el JSON viene corrupto lo mandamos directo a cuarentena
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO eventos_cuarentena (motivo, detalle, payload_raw)
                VALUES ($1, $2, $3::jsonb)
                """,
                "JSON_INVALIDO",
                str(exc),
                json.dumps({"raw_hex": raw_bytes.hex()}, ensure_ascii=False),
            )
        stats["cuarentena"] += 1
        return

    source = data.get("source", "usgs_live")
    mode = data.get("mode", "live")
    ingested_at = data.get("ingested_at") or 0
    evento = data.get("event") or {}
    raw_feature = data.get("raw") or {}

    eid = evento.get("id")
    time_ms = evento.get("time")
    updated_ms = evento.get("updated")
    lon = evento.get("lon")
    lat = evento.get("lat")
    mag = evento.get("mag")
    mag_type = evento.get("magType")
    place = evento.get("place")
    depth_km = evento.get("depth_km")

    # Validación de campos obligatorios y límites de coordenadas
    motivos_cuarentena = []
    if not eid or not isinstance(eid, str):
        motivos_cuarentena.append("ID_AUSENTE_O_INVALIDO")
    if not isinstance(time_ms, int) or time_ms <= 0:
        motivos_cuarentena.append("TIME_INVALIDO")
    if not isinstance(updated_ms, int) or updated_ms <= 0:
        motivos_cuarentena.append("UPDATED_INVALIDO")

    if not es_numero_finito(lon) or not (-180.0 <= lon <= 180.0):
        motivos_cuarentena.append("LONGITUD_FUERA_DE_RANGO")
    if not es_numero_finito(lat) or not (-90.0 <= lat <= 90.0):
        motivos_cuarentena.append("LATITUD_FUERA_DE_RANGO")

    # Si falla la validación espacial o de id, va a cuarentena
    if motivos_cuarentena:
        motivo_str = "; ".join(motivos_cuarentena)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO eventos_cuarentena (id, updated_ms, motivo, detalle, source, payload_raw)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                """,
                eid,
                updated_ms if isinstance(updated_ms, int) else None,
                motivo_str,
                f"lon={lon}, lat={lat}, time={time_ms}",
                source,
                json.dumps(raw_feature or data, ensure_ascii=False),
            )
        stats["cuarentena"] += 1
        log.warning("Evento en cuarentena (%s): %s", eid, motivo_str)
        return

    # Flags de calidad para valores atípicos o nulos
    mag_nula = (mag is None or not es_numero_finito(mag))
    val_mag = None if mag_nula else float(mag)
    mag_negativa = (val_mag is not None and val_mag < 0.0)

    depth_nula = (depth_km is None or not es_numero_finito(depth_km))
    val_depth = None if depth_nula else float(depth_km)
    depth_negativa = (val_depth is not None and val_depth < 0.0)

    place_nulo = (not place or not isinstance(place, str) or not place.strip())
    val_place = None if place_nulo else place.strip()

    if mag_nula:
        stats["mag_nulas"] += 1
    if mag_negativa:
        stats["mag_negativas"] += 1
    if depth_nula:
        stats["depth_nulas"] += 1
    if depth_negativa:
        stats["depth_negativas"] += 1
    if place_nulo:
        stats["place_nulos"] += 1

    # Enriquecimiento con país mediante polígonos
    geo = catalogo.buscar(lon, lat)
    country_id = geo["country_id"]
    country_name = geo["country_name"]
    continent = geo["continent"]
    subregion = geo["subregion"]
    sin_pais = (country_id is None)

    if sin_pais:
        stats["sin_pais"] += 1

    # Guardar en base de datos controlando versiones y duplicados
    async with pool.acquire() as conn:
        async with conn.transaction():
            fila = await conn.fetchrow(
                "SELECT updated_ms FROM eventos_actuales WHERE id = $1",
                eid,
            )

            if fila is None:
                # Sismo nuevo: insertamos en eventos_actuales y en el histórico de versiones
                await conn.execute(
                    """
                    INSERT INTO eventos_actuales (
                        id, time_ms, updated_ms, ingested_at_ms,
                        mag, mag_type, place, lon, lat, depth_km,
                        country_id, country_name, continent, subregion,
                        source, mode,
                        mag_nula, mag_negativa, depth_nula, depth_negativa, place_nulo, sin_pais
                    ) VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14,
                        $15, $16,
                        $17, $18, $19, $20, $21, $22
                    )
                    """,
                    eid, time_ms, updated_ms, ingested_at,
                    val_mag, mag_type, val_place, lon, lat, val_depth,
                    country_id, country_name, continent, subregion,
                    source, mode,
                    mag_nula, mag_negativa, depth_nula, depth_negativa, place_nulo, sin_pais,
                )

                await conn.execute(
                    """
                    INSERT INTO eventos_versiones (
                        id, updated_ms, source, mode, mag, mag_type, place, lon, lat, depth_km
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (id, updated_ms) DO NOTHING
                    """,
                    eid, updated_ms, source, mode, val_mag, mag_type, val_place, lon, lat, val_depth,
                )
                stats["insertados"] += 1

            else:
                updated_existente = fila["updated_ms"]
                if updated_ms > updated_existente:
                    # Revisión más reciente: actualizamos el estado actual y guardamos versión
                    await conn.execute(
                        """
                        UPDATE eventos_actuales
                        SET time_ms = $2,
                            updated_ms = $3,
                            ingested_at_ms = $4,
                            procesado_en = NOW(),
                            mag = $5,
                            mag_type = $6,
                            place = $7,
                            lon = $8,
                            lat = $9,
                            depth_km = $10,
                            country_id = $11,
                            country_name = $12,
                            continent = $13,
                            subregion = $14,
                            source = $15,
                            mode = $16,
                            mag_nula = $17,
                            mag_negativa = $18,
                            depth_nula = $19,
                            depth_negativa = $20,
                            place_nulo = $21,
                            sin_pais = $22
                        WHERE id = $1
                        """,
                        eid, time_ms, updated_ms, ingested_at,
                        val_mag, mag_type, val_place, lon, lat, val_depth,
                        country_id, country_name, continent, subregion,
                        source, mode,
                        mag_nula, mag_negativa, depth_nula, depth_negativa, place_nulo, sin_pais,
                    )

                    await conn.execute(
                        """
                        INSERT INTO eventos_versiones (
                            id, updated_ms, source, mode, mag, mag_type, place, lon, lat, depth_km
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (id, updated_ms) DO NOTHING
                        """,
                        eid, updated_ms, source, mode, val_mag, mag_type, val_place, lon, lat, val_depth,
                    )
                    stats["actualizados"] += 1
                else:
                    # Si llega una versión repetida o más antigua, la ignoramos
                    stats["omitidos_duplicado"] += 1


async def registrar_log_transformacion(pool: asyncpg.Pool, lote_size: int, stats: dict) -> None:
    # Guarda métricas del lote procesado
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transformaciones_log (
                lote_size, insertados, actualizados, omitidos_duplicado, cuarentena,
                mag_nulas, mag_negativas, depth_nulas, depth_negativas, place_nulos, sin_pais
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
            lote_size,
            stats["insertados"],
            stats["actualizados"],
            stats["omitidos_duplicado"],
            stats["cuarentena"],
            stats["mag_nulas"],
            stats["mag_negativas"],
            stats["depth_nulas"],
            stats["depth_negativas"],
            stats["place_nulos"],
            stats["sin_pais"],
        )


async def main() -> None:
    LISTO.unlink(missing_ok=True)

    # Cargamos el mapa de países
    catalogo = CatalogoGeografico()

    async def crear_pool():
        return await asyncpg.create_pool(ajustes.postgres_dsn, min_size=1, max_size=5)

    pool: asyncpg.Pool = await esperar("PostgreSQL", crear_pool)
    await esperar("Esquema PostgreSQL", lambda: inicializar_tablas_consumidor(pool))

    # Consumidor de Kafka con confirmación manual (enable_auto_commit=False)
    topics = [ajustes.kafka_topic, "terremotos.replay"]
    consumidor = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=ajustes.kafka_servers,
        group_id=ajustes.kafka_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await esperar("Kafka", consumidor.start)
    LISTO.write_text("ok", encoding="utf-8")
    log.info(
        "Consumidor listo · topics=%s · group=%s",
        topics,
        ajustes.kafka_group,
    )

    try:
        while True:
            lote = await consumidor.getmany(timeout_ms=5000, max_records=100)
            if not lote:
                await asyncio.sleep(0.5)
                continue

            total_mensajes = sum(len(msgs) for msgs in lote.values())
            stats = {
                "insertados": 0,
                "actualizados": 0,
                "omitidos_duplicado": 0,
                "cuarentena": 0,
                "mag_nulas": 0,
                "mag_negativas": 0,
                "depth_nulas": 0,
                "depth_negativas": 0,
                "place_nulos": 0,
                "sin_pais": 0,
            }

            for topic_partition, mensajes in lote.items():
                for msg in mensajes:
                    await procesar_mensaje(pool, catalogo, msg.value, stats)

            # Confirmamos en Kafka solo después de haber guardado en Postgres
            await consumidor.commit()

            if total_mensajes > 0:
                await registrar_log_transformacion(pool, total_mensajes, stats)
                log.info(
                    "Lote procesado: %d msgs (insertados=%d, actualizados=%d, omitidos_dup=%d, cuarentena=%d, sin_pais=%d)",
                    total_mensajes,
                    stats["insertados"],
                    stats["actualizados"],
                    stats["omitidos_duplicado"],
                    stats["cuarentena"],
                    stats["sin_pais"],
                )

    finally:
        await consumidor.stop()
        await pool.close()
        LISTO.unlink(missing_ok=True)


if __name__ == "__main__":
    asyncio.run(main())
