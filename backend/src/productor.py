"""
Productor: consulta el feed USGS cada POLL_SECONDS y publica en Kafka
únicamente los eventos nuevos o con una versión posterior (updated mayor).

──────────────────────────────────────────────────────────────────────
Estado persistente (PostgreSQL · tabla productor_estado)
  id → updated_ms de la última versión publicada.
  Se carga al arrancar y se actualiza tras cada ack de Kafka.
  No se pierde entre reinicios del contenedor.

Garantía at-least-once:
  1. Guardar raw en eventos_raw.
  2. Publicar en Kafka y esperar ack (acks="all").
  3. Actualizar productor_estado.
  Si el proceso cae entre los pasos 2 y 3, el mensaje puede reenviarse.
  El consumidor tolera este duplicado verificando updated antes de persistir.

Mensaje Kafka (valor JSON, clave = id del evento en UTF-8):
  {
    "source":      "usgs_live",
    "ingested_at": <ms Unix epoch UTC>,
    "mode":        "live",          ← "replay" en el productor histórico
    "event":       { id, time, updated, mag, magType, type, place,
                     lon, lat, depth_km },
    "raw":         { <feature GeoJSON original de USGS> }
  }

Gestión de errores HTTP:
  - 429 Too Many Requests: espera Retry-After o 2× POLL_SECONDS.
  - 5xx: registra error, omite el ciclo, no borra estado anterior.
  - Timeout: registra, omite el ciclo.
  - JSON inválido: registra, omite el ciclo.

Eventos que salen del feed:
  «No aparece en la ventana actual» ≠ «USGS lo ha eliminado».
  Puede haber envejecido más de 24 h (comportamiento normal del feed).
  El estado en productor_estado se conserva; no se borra al desaparecer.

  LIMITACIÓN documentada (fuera del alcance mínimo):
  La reconciliación de eventos eliminados, fusionados o revisados cuando ya
  están fuera del feed live requiere cruzar con el catálogo histórico.
  No se implementa en este punto; se documenta como ampliación pendiente.

Recuperación tras parada > 24 h:
  Los eventos del hueco no aparecen en el feed live (ventana de solo 24 h).
  Procedimiento: ejecutar scripts/descargar_historico.py para el rango
  afectado y luego el script de replay (productor_replay.py, punto siguiente).
  El replay usa el mismo topic con mode="replay" para mantener el aislamiento.
"""

import asyncio
import json
import logging
import time
from pathlib import Path

import asyncpg
import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from src.config import ajustes, esperar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [productor] %(message)s",
)
log = logging.getLogger("productor")
LISTO = Path("/tmp/ready")

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
TIMEOUT_HTTP = 30       # segundos para la petición USGS
RETENCION_RAW_DIAS = 7  # días que se conserva el raw antes de limpiar

# ──────────────────────────────────────────────────────────────────────
# PostgreSQL: esquema propio del productor
# ──────────────────────────────────────────────────────────────────────

CREAR_TABLAS = """
CREATE TABLE IF NOT EXISTS productor_estado (
    id           TEXT        PRIMARY KEY,
    updated_ms   BIGINT      NOT NULL,
    publicado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS eventos_raw (
    id          TEXT        NOT NULL,
    updated_ms  BIGINT      NOT NULL,
    recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fuente      TEXT        NOT NULL DEFAULT 'usgs_live',
    payload     JSONB       NOT NULL,
    PRIMARY KEY (id, updated_ms)
);

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
"""


async def inicializar_tablas(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREAR_TABLAS)
    log.info("Tablas del productor verificadas/creadas")


async def cargar_estado(pool: asyncpg.Pool) -> dict[str, int]:
    """Carga id → updated_ms desde productor_estado al arrancar."""
    async with pool.acquire() as conn:
        filas = await conn.fetch("SELECT id, updated_ms FROM productor_estado")
    estado = {f["id"]: f["updated_ms"] for f in filas}
    log.info("Estado cargado: %d eventos conocidos", len(estado))
    return estado


async def guardar_raw(
    pool: asyncpg.Pool,
    feature: dict,
    fuente: str = "usgs_live",
) -> None:
    """
    Guarda el GeoJSON original antes de cualquier transformación.
    ON CONFLICT DO NOTHING evita duplicar versiones idénticas.
    """
    eid = feature.get("id", "")
    updated = (feature.get("properties") or {}).get("updated") or 0
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO eventos_raw (id, updated_ms, fuente, payload)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (id, updated_ms) DO NOTHING
            """,
            eid, updated, fuente, json.dumps(feature, ensure_ascii=False),
        )


async def registrar_consulta(pool: asyncpg.Pool, **kwargs) -> None:
    """Escribe una fila en consultas_registro con los datos del ciclo."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO consultas_registro
              (url, http_status, latencia_ms, total_features, total_terremotos,
               total_excluidos, publicados, error)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """,
            kwargs.get("url", USGS_URL),
            kwargs.get("http_status"),
            kwargs.get("latencia_ms"),
            kwargs.get("total_features"),
            kwargs.get("total_terremotos"),
            kwargs.get("total_excluidos"),
            kwargs.get("publicados"),
            kwargs.get("error"),
        )


async def marcar_publicado(
    pool: asyncpg.Pool, eid: str, updated_ms: int
) -> None:
    """Actualiza productor_estado DESPUÉS de recibir el ack de Kafka."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO productor_estado (id, updated_ms)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE
              SET updated_ms   = EXCLUDED.updated_ms,
                  publicado_en = NOW()
            """,
            eid, updated_ms,
        )


async def limpiar_raw(pool: asyncpg.Pool) -> None:
    """Elimina eventos_raw más antiguos que RETENCION_RAW_DIAS días."""
    async with pool.acquire() as conn:
        eliminados = await conn.fetchval(
            """
            WITH deleted AS (
                DELETE FROM eventos_raw
                WHERE recibido_en < NOW() - ($1 || ' days')::interval
                RETURNING 1
            )
            SELECT COUNT(*) FROM deleted
            """,
            str(RETENCION_RAW_DIAS),
        )
    if eliminados:
        log.info("Limpieza raw: %d filas eliminadas (> %d días)", eliminados, RETENCION_RAW_DIAS)


# ──────────────────────────────────────────────────────────────────────
# Kafka
# ──────────────────────────────────────────────────────────────────────

async def asegurar_topic() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=ajustes.kafka_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(
                    name=ajustes.kafka_topic,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"retention.ms": str(ajustes.kafka_retention_ms)},
                )
            ]
        )
        log.info("Topic %s creado", ajustes.kafka_topic)
    except TopicAlreadyExistsError:
        log.info("Topic %s ya existía", ajustes.kafka_topic)
    finally:
        await admin.close()


# ──────────────────────────────────────────────────────────────────────
# Extracción USGS
# ──────────────────────────────────────────────────────────────────────

class ErrorHTTP(Exception):
    def __init__(self, status: int, retry_after: int | None = None):
        self.status = status
        self.retry_after = retry_after


async def obtener_feed(
    cliente: httpx.AsyncClient,
) -> tuple[list[dict], list[dict], dict]:
    """
    Descarga el feed USGS.
    Devuelve (terremotos, excluidos, meta) donde meta contiene latencia y status.
    Lanza ErrorHTTP para 429/5xx; deja que el caller decida cómo esperar.
    Lanza httpx.TimeoutException para timeout.
    Lanza ValueError para JSON inválido.
    """
    t0 = time.monotonic()
    respuesta = await cliente.get(USGS_URL, timeout=TIMEOUT_HTTP)
    latencia_ms = int((time.monotonic() - t0) * 1000)

    if respuesta.status_code == 429:
        retry_after = int(respuesta.headers.get("Retry-After", 0)) or None
        raise ErrorHTTP(429, retry_after)

    if respuesta.status_code >= 500:
        raise ErrorHTTP(respuesta.status_code)

    respuesta.raise_for_status()

    try:
        datos = respuesta.json()
    except Exception as exc:
        raise ValueError(f"JSON inválido: {exc}") from exc

    features = datos.get("features", [])
    terremotos, excluidos = [], []
    for f in features:
        tipo = (f.get("properties") or {}).get("type")
        if tipo == "earthquake":
            terremotos.append(f)
        else:
            excluidos.append({
                "id": f.get("id"),
                "tipo_excluido": tipo,
                "motivo": "properties.type != 'earthquake'",
            })

    meta = {
        "http_status": respuesta.status_code,
        "latencia_ms": latencia_ms,
        "total_features": len(features),
        "total_terremotos": len(terremotos),
        "total_excluidos": len(excluidos),
    }
    log.info(
        "USGS · %d features · %d terremotos · %d excluidos · %dms",
        len(features), len(terremotos), len(excluidos), latencia_ms,
    )
    return terremotos, excluidos, meta


def extraer_evento(feature: dict) -> dict | None:
    """
    Normaliza un feature USGS al esquema del mensaje Kafka.

    Tipos y unidades:
      id        str         — identificador único USGS
      time      int (ms)    — tiempo del terremoto, epoch UTC
      updated   int (ms)    — última revisión en USGS, epoch UTC
      mag       float|None  — magnitud; None si no disponible
      magType   str|None    — tipo de magnitud (ml, mb, mww…)
      type      str         — siempre "earthquake" tras el filtro
      place     str|None    — descripción textual de ubicación
      lon       float       — longitud WGS84 (-180..180)
      lat       float       — latitud WGS84 (-90..90)
      depth_km  float|None  — profundidad en kilómetros
    """
    try:
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or []
        return {
            "id":       feature["id"],
            "time":     props.get("time"),
            "updated":  props.get("updated"),
            "mag":      props.get("mag"),
            "magType":  props.get("magType"),
            "type":     props.get("type"),
            "place":    props.get("place"),
            "lon":      coords[0] if len(coords) > 0 else None,
            "lat":      coords[1] if len(coords) > 1 else None,
            "depth_km": coords[2] if len(coords) > 2 else None,
        }
    except Exception as exc:
        log.warning("Error extrayendo evento %s: %s", feature.get("id"), exc)
        return None


# ──────────────────────────────────────────────────────────────────────
# Bucle principal
# ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    LISTO.unlink(missing_ok=True)

    # 1. Conectar a Kafka y PostgreSQL (con reintentos)
    await esperar("Kafka (topic)", asegurar_topic)

    async def crear_pool():
        return await asyncpg.create_pool(ajustes.postgres_dsn, min_size=1, max_size=3)

    pool: asyncpg.Pool = await esperar("PostgreSQL", crear_pool)

    productor = AIOKafkaProducer(
        bootstrap_servers=ajustes.kafka_servers,
        acks="all",
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    await esperar("Kafka (productor)", productor.start)

    # 2. Inicializar esquema propio y cargar estado persistente
    await inicializar_tablas(pool)
    await limpiar_raw(pool)
    vistos: dict[str, int] = await cargar_estado(pool)

    LISTO.write_text("ok", encoding="utf-8")
    log.info(
        "Productor listo · topic=%s · intervalo=%ss · %d eventos en estado",
        ajustes.kafka_topic, ajustes.poll_seconds, len(vistos),
    )

    extra_espera = 0  # segundos adicionales tras un 429

    try:
        async with httpx.AsyncClient() as cliente:
            while True:
                if extra_espera > 0:
                    log.info("Esperando %ss extra (backoff 429)…", extra_espera)
                    await asyncio.sleep(extra_espera)
                    extra_espera = 0

                # ── Extracción ─────────────────────────────────────
                terremotos = []
                meta: dict = {"url": USGS_URL}
                try:
                    terremotos, _, meta_feed = await obtener_feed(cliente)
                    meta = {**meta_feed, "url": USGS_URL}
                except ErrorHTTP as exc:
                    if exc.status == 429:
                        espera_429 = exc.retry_after or (ajustes.poll_seconds * 2)
                        log.warning("USGS 429 · esperando %ss", espera_429)
                        extra_espera = espera_429
                        meta["http_status"] = 429
                        meta["error"] = "Too Many Requests"
                    else:
                        log.error("USGS HTTP %d · omitiendo ciclo", exc.status)
                        meta["http_status"] = exc.status
                        meta["error"] = f"HTTP {exc.status}"
                except httpx.TimeoutException:
                    log.error("USGS timeout · omitiendo ciclo")
                    meta["error"] = "Timeout"
                except ValueError as exc:
                    log.error("USGS respuesta inválida: %s · omitiendo ciclo", exc)
                    meta["error"] = str(exc)
                except Exception as exc:
                    log.error("Error inesperado en extracción: %s · omitiendo ciclo", exc)
                    meta["error"] = str(exc)

                # ── Publicación ────────────────────────────────────
                publicados = 0
                for feature in terremotos:
                    evento = extraer_evento(feature)
                    if evento is None:
                        continue

                    eid = evento["id"]
                    updated = evento.get("updated") or 0

                    # Comparar con estado persistente: solo publicar novedades
                    if vistos.get(eid, -1) >= updated:
                        continue  # versión ya publicada o más reciente en estado

                    # Guardar GeoJSON original ANTES de publicar
                    await guardar_raw(pool, feature)

                    # Construir mensaje Kafka
                    mensaje = {
                        "source":      "usgs_live",
                        "ingested_at": int(time.time() * 1000),
                        "mode":        "live",
                        "event":       evento,
                        "raw":         feature,
                    }

                    # Publicar usando id del evento como clave (at-least-once)
                    # La clave garantiza que revisiones del mismo evento van
                    # a la misma partición (relevante si se amplían particiones).
                    await productor.send(
                        ajustes.kafka_topic,
                        key=eid,
                        value=mensaje,
                    )
                    # ack recibido (acks="all") → actualizar estado persistente
                    await marcar_publicado(pool, eid, updated)
                    vistos[eid] = updated
                    publicados += 1

                # ── Registro de la consulta ────────────────────────
                await registrar_consulta(
                    pool,
                    publicados=publicados,
                    **meta,
                )
                log.info(
                    "Ciclo completado · publicados=%d de %d · siguiente en %ss",
                    publicados, len(terremotos), ajustes.poll_seconds,
                )
                await asyncio.sleep(ajustes.poll_seconds)

    finally:
        await productor.stop()
        await pool.close()
        LISTO.unlink(missing_ok=True)


if __name__ == "__main__":
    asyncio.run(main())
