"""
API FastAPI: endpoints para consultar los eventos sísmicos y estadísticas.
"""

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.config import ajustes, esperar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [api] %(message)s",
)
log = logging.getLogger("api")


@asynccontextmanager
async def vida(app: FastAPI):
    async def conectar():
        return await asyncpg.create_pool(ajustes.postgres_dsn, min_size=2, max_size=10)

    app.state.pool = await esperar("PostgreSQL (pool API)", conectar)
    log.info("API conectada a PostgreSQL. Lista para atender consultas.")
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(title="Observatorio de Actividad Sísmica - API", lifespan=vida)

# Configuración CORS para el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(ajustes.cors_origins) if ajustes.cors_origins else ["*"],
    allow_origin_regex=r"https://.*\.netlify\.app",
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["ngrok-skip-browser-warning", "Content-Type", "Accept"],
)


async def obtener_ultima_ingesta(pool: asyncpg.Pool) -> Optional[int]:
    # Obtiene la fecha en ms del último evento o consulta registrada
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT EXTRACT(EPOCH FROM consultado_en) * 1000 AS ts
                FROM consultas_registro
                WHERE http_status = 200
                ORDER BY consultado_en DESC
                LIMIT 1
                """
            )
            if row and row["ts"]:
                return int(row["ts"])
            ts = await conn.fetchval("SELECT MAX(ingested_at_ms) FROM eventos_actuales")
            return int(ts) if ts else None
    except Exception as exc:
        log.warning("No se pudo obtener última ingesta: %s", exc)
        return None


@app.get("/api/salud")
async def salud():
    pool: asyncpg.Pool = app.state.pool
    postgres_ok = False
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
            postgres_ok = True
    except Exception:
        log.exception("Error de conexión con PostgreSQL")
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "servicio": "api",
                "postgres": "no disponible",
                "last_ingestion": None,
            },
        )

    last_ingestion = await obtener_ultima_ingesta(pool)
    return {
        "status": "ok",
        "servicio": "api",
        "postgres": "ok" if postgres_ok else "error",
        "last_ingestion": last_ingestion,
        "timestamp": int(time.time() * 1000),
    }


def parse_timestamp_ms(val: Optional[str]) -> Optional[int]:
    if not val:
        return None
    val = val.strip()
    try:
        if val.isdigit() or (val.startswith("-") and val[1:].isdigit()):
            return int(val)
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


@app.get("/api/eventos")
async def eventos(
    start: Optional[str] = Query(None, description="Fecha inicio ISO8601 o ms epoch (inclusive)"),
    end: Optional[str] = Query(None, description="Fecha fin ISO8601 o ms epoch (exclusive)"),
    min_mag: Optional[float] = Query(None, description="Magnitud mínima (excluye nulos de mag)"),
    country_id: Optional[str] = Query(None, description="Código ADM0_A3 o 'NULL' para marítimos"),
    mode: str = Query("live", description="Modo de consulta ('live' o 'replay')"),
    limit: int = Query(2000, ge=1, le=10000, description="Límite máximo de eventos"),
):
    # Consulta de terremotos con filtros
    pool: asyncpg.Pool = app.state.pool
    ahora_ms = int(time.time() * 1000)

    # Por defecto filtramos últimas 24 horas si no se pasa fecha de inicio
    start_ms = parse_timestamp_ms(start)
    if start_ms is None:
        start_ms = ahora_ms - (24 * 60 * 60 * 1000)

    end_ms = parse_timestamp_ms(end)

    condiciones = ["mode = $1", "time_ms >= $2"]
    parametros = [mode, start_ms]
    idx = 3

    if end_ms is not None:
        condiciones.append(f"time_ms < ${idx}")
        parametros.append(end_ms)
        idx += 1

    if min_mag is not None:
        condiciones.append(f"mag IS NOT NULL AND mag >= ${idx}")
        parametros.append(min_mag)
        idx += 1

    if country_id:
        c_upper = country_id.strip().upper()
        if c_upper in ("NULL", "SIN_PAIS", "NONE"):
            condiciones.append("country_id IS NULL")
        else:
            condiciones.append(f"country_id = ${idx}")
            parametros.append(c_upper)
            idx += 1

    where_clause = " AND ".join(condiciones)

    query_count = f"SELECT count(*) FROM eventos_actuales WHERE {where_clause}"
    query_select = f"""
        SELECT id, time_ms, updated_ms, mag, mag_type, place,
               lon, lat, depth_km, country_id, country_name
        FROM eventos_actuales
        WHERE {where_clause}
        ORDER BY time_ms DESC
        LIMIT ${idx}
    """
    parametros_select = parametros + [limit]

    try:
        async with pool.acquire() as conn:
            total_count = await conn.fetchval(query_count, *parametros)
            filas = await conn.fetch(query_select, *parametros_select)
            last_ingestion = await obtener_ultima_ingesta(pool)
    except Exception as exc:
        log.exception("Error al consultar eventos_actuales")
        return JSONResponse(
            status_code=500,
            content={"error": "Fallo al consultar la base de datos", "detalle": str(exc)},
        )

    features = []
    for r in filas:
        lon = float(r["lon"])
        lat = float(r["lat"])
        depth = float(r["depth_km"]) if r["depth_km"] is not None else None
        mag_val = float(r["mag"]) if r["mag"] is not None else None

        features.append({
            "type": "Feature",
            "id": r["id"],
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat, depth],
            },
            "properties": {
                "mag": mag_val,
                "magType": r["mag_type"],
                "place": r["place"],
                "time": int(r["time_ms"]),
                "updated": int(r["updated_ms"]),
                "country_id": r["country_id"],
                "country_name": r["country_name"],
            },
        })

    is_truncated = total_count > limit
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": ahora_ms,
            "last_ingestion": last_ingestion,
            "mode": mode,
            "count": len(features),
            "total_count": total_count,
            "truncated": is_truncated,
            "start": start_ms,
            "end": end_ms,
        },
        "features": features,
    }


@app.get("/api/estadisticas")
async def estadisticas(
    start: Optional[str] = Query(None, description="Fecha inicio ISO8601 o ms epoch"),
    end: Optional[str] = Query(None, description="Fecha fin ISO8601 o ms epoch"),
    min_mag: Optional[float] = Query(None, description="Magnitud mínima"),
    country_id: Optional[str] = Query(None, description="Código ADM0_A3"),
    mode: str = Query("live", description="Modo ('live' o 'replay')"),
):
    # Agregaciones estadísticas y métricas para el dashboard
    pool: asyncpg.Pool = app.state.pool
    ahora_ms = int(time.time() * 1000)

    start_ms = parse_timestamp_ms(start)
    if start_ms is None:
        start_ms = ahora_ms - (24 * 60 * 60 * 1000)

    end_ms = parse_timestamp_ms(end)

    condiciones = ["mode = $1", "time_ms >= $2"]
    parametros = [mode, start_ms]
    idx = 3

    if end_ms is not None:
        condiciones.append(f"time_ms < ${idx}")
        parametros.append(end_ms)
        idx += 1

    if min_mag is not None:
        condiciones.append(f"mag IS NOT NULL AND mag >= ${idx}")
        parametros.append(min_mag)
        idx += 1

    if country_id:
        c_upper = country_id.strip().upper()
        if c_upper in ("NULL", "SIN_PAIS", "NONE"):
            condiciones.append("country_id IS NULL")
        else:
            condiciones.append(f"country_id = ${idx}")
            parametros.append(c_upper)
            idx += 1

    where_clause = " AND ".join(condiciones)

    try:
        async with pool.acquire() as conn:
            # 1. Totales generales del periodo
            totales = await conn.fetchrow(
                f"""
                SELECT 
                    count(*) AS total,
                    count(*) FILTER (WHERE mag IS NULL) AS mag_nulas,
                    count(*) FILTER (WHERE depth_km IS NULL) AS depth_nulas,
                    count(*) FILTER (WHERE country_id IS NULL) AS sin_pais,
                    ROUND(AVG(mag)::numeric, 2) AS mag_media,
                    MAX(mag) AS mag_max,
                    ROUND(AVG(depth_km)::numeric, 2) AS depth_media
                FROM eventos_actuales
                WHERE {where_clause}
                """,
                *parametros,
            )

            # 2. Ranking de eventos por país
            por_pais = await conn.fetch(
                f"""
                SELECT 
                    COALESCE(country_name, 'Sin país asignado (marítimo)') AS pais,
                    COALESCE(country_id, 'ND') AS country_id,
                    count(*) AS total,
                    ROUND(AVG(mag)::numeric, 2) AS mag_promedio
                FROM eventos_actuales
                WHERE {where_clause}
                GROUP BY country_name, country_id
                ORDER BY total DESC
                LIMIT 15
                """,
                *parametros,
            )

            # 3. Agrupación por ventana horaria UTC
            por_hora = await conn.fetch(
                f"""
                SELECT 
                    to_char(to_timestamp(time_ms / 1000) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:00:00"Z"') AS hora_utc,
                    count(*) AS total,
                    ROUND(AVG(mag)::numeric, 2) AS mag_promedio
                FROM eventos_actuales
                WHERE {where_clause}
                GROUP BY hora_utc
                ORDER BY hora_utc ASC
                """,
                *parametros,
            )

            # 4. Distribución por tramos de magnitud
            dist_mag = await conn.fetchrow(
                f"""
                SELECT 
                    count(*) FILTER (WHERE mag < 2.0) AS menor_2,
                    count(*) FILTER (WHERE mag >= 2.0 AND mag < 4.0) AS de_2_a_3_9,
                    count(*) FILTER (WHERE mag >= 4.0 AND mag < 5.0) AS de_4_a_4_9,
                    count(*) FILTER (WHERE mag >= 5.0 AND mag < 6.0) AS de_5_a_5_9,
                    count(*) FILTER (WHERE mag >= 6.0) AS mayor_o_igual_6,
                    count(*) FILTER (WHERE mag IS NULL) AS no_disponible
                FROM eventos_actuales
                WHERE {where_clause}
                """,
                *parametros,
            )

            # 5. Distribución por tramos de profundidad
            dist_depth = await conn.fetchrow(
                f"""
                SELECT 
                    count(*) FILTER (WHERE depth_km < 70.0) AS superficial,
                    count(*) FILTER (WHERE depth_km >= 70.0 AND depth_km < 300.0) AS intermedia,
                    count(*) FILTER (WHERE depth_km >= 300.0) AS profunda,
                    count(*) FILTER (WHERE depth_km IS NULL) AS no_disponible
                FROM eventos_actuales
                WHERE {where_clause}
                """,
                *parametros,
            )

            # 6. Comparación con la media histórica de 2025 para mag >= 4.0
            cond_24h_m4 = ["mode = 'live'", f"time_ms >= {ahora_ms - 86400000}", "mag >= 4.0"]
            if country_id and country_id.upper() not in ("NULL", "SIN_PAIS"):
                cond_24h_m4.append(f"country_id = '{country_id.upper()}'")
            reciente_m4 = await conn.fetchval(
                f"SELECT count(*) FROM eventos_actuales WHERE {' AND '.join(cond_24h_m4)}"
            )

            media_historica_diaria_m4 = 38.35
            nota_comparativa = (
                "Referencia global 2025: ~14.000 terremotos mag>=4.0 anuales distribuidos en 365 días (media ~38,35/día). "
                "Los datos de las últimas 24h pueden estar aún incompletos debido a retrasos de reporte en estaciones lejanas."
            )

    except Exception as exc:
        log.exception("Error al calcular estadísticas")
        return JSONResponse(
            status_code=500,
            content={"error": "Fallo al calcular estadísticas", "detalle": str(exc)},
        )

    return {
        "periodo": {
            "start_ms": start_ms,
            "end_ms": end_ms or ahora_ms,
            "duracion_horas": round(((end_ms or ahora_ms) - start_ms) / 3600000.0, 2),
        },
        "resumen": {
            "total_eventos": totales["total"] or 0,
            "mag_media": float(totales["mag_media"]) if totales["mag_media"] else None,
            "mag_maxima": float(totales["mag_max"]) if totales["mag_max"] else None,
            "depth_media_km": float(totales["depth_media"]) if totales["depth_media"] else None,
            "sin_pais_total": totales["sin_pais"] or 0,
            "mag_nulas": totales["mag_nulas"] or 0,
            "depth_nulas": totales["depth_nulas"] or 0,
        },
        "por_pais": [
            {
                "pais": p["pais"],
                "country_id": p["country_id"],
                "total": p["total"],
                "mag_promedio": float(p["mag_promedio"]) if p["mag_promedio"] else None,
            }
            for p in por_pais
        ],
        "por_hora_utc": [
            {
                "hora_utc": h["hora_utc"],
                "total": h["total"],
                "mag_promedio": float(h["mag_promedio"]) if h["mag_promedio"] else None,
            }
            for h in por_hora
        ],
        "distribucion_magnitud": {
            "menor_2": dist_mag["menor_2"] or 0,
            "de_2_a_3_9": dist_mag["de_2_a_3_9"] or 0,
            "de_4_a_4_9": dist_mag["de_4_a_4_9"] or 0,
            "de_5_a_5_9": dist_mag["de_5_a_5_9"] or 0,
            "mayor_o_igual_6": dist_mag["mayor_o_igual_6"] or 0,
            "no_disponible": dist_mag["no_disponible"] or 0,
        },
        "distribucion_profundidad": {
            "superficial_menos_70km": dist_depth["superficial"] or 0,
            "intermedia_70_300km": dist_depth["intermedia"] or 0,
            "profunda_mas_300km": dist_depth["profunda"] or 0,
            "no_disponible": dist_depth["no_disponible"] or 0,
        },
        "comparacion_historica_m4": {
            "ultimas_24h_con_mag_gte_4": reciente_m4 or 0,
            "media_diaria_historica_2025": media_historica_diaria_m4,
            "diferencia_pct": round(((reciente_m4 - media_historica_diaria_m4) / media_historica_diaria_m4) * 100, 1) if media_historica_diaria_m4 else 0,
            "nota": nota_comparativa,
        },
    }
