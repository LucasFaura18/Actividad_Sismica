"""
Productor de Reproducción Histórica Acelerada (Punto 11).

Características:
- Publica en el topic aislado 'terremotos.replay'.
- Todos los mensajes están claramente etiquetados con:
    "mode": "replay"
    "source": "usgs_replay_2025"
- Mantiene los timestamps originales del evento (time, updated) y añade
  el timestamp de reproducción en 'ingested_at'.
- Incluye casos controlados de:
    * Evento nuevo
    * Reenvío idéntico (duplicado)
    * Versión posterior corregida (revisión de magnitud)
    * Versión antigua retrasada (regresión)
    * Magnitud nula
    * Coordenadas inválidas (cuarentena)
    * Evento marítimo (sin país asignado)
"""

import asyncio
import json
import logging
import time
from pathlib import Path

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from src.config import ajustes, esperar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [REPRODUCCIÓN] %(message)s",
)
log = logging.getLogger("productor_replay")

TOPIC_REPLAY = "terremotos.replay"
MUESTRA_FILE = Path(__file__).parent.parent / "datos" / "historico" / "muestra_replay.geojson"


async def asegurar_topic_replay():
    admin = AIOKafkaAdminClient(bootstrap_servers=ajustes.kafka_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(
                    name=TOPIC_REPLAY,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"retention.ms": "259200000"},
                )
            ]
        )
        log.info("Topic aislado %s creado", TOPIC_REPLAY)
    except TopicAlreadyExistsError:
        log.info("Topic %s ya existía", TOPIC_REPLAY)
    finally:
        await admin.close()


def crear_mensaje(eid: str, time_ms: int, updated_ms: int, lon: float, lat: float,
                   mag: float | None, depth_km: float | None, place: str, mag_type: str = "mww") -> dict:
    evento = {
        "id": eid,
        "time": time_ms,
        "updated": updated_ms,
        "mag": mag,
        "magType": mag_type,
        "type": "earthquake",
        "place": place,
        "lon": lon,
        "lat": lat,
        "depth_km": depth_km,
    }
    return {
        "source": "usgs_replay_2025",
        "ingested_at": int(time.time() * 1000),
        "mode": "replay",
        "event": evento,
        "raw": {
            "type": "Feature",
            "id": eid,
            "properties": {
                "mag": mag,
                "place": place,
                "time": time_ms,
                "updated": updated_ms,
                "type": "earthquake",
                "magType": mag_type,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat, depth_km],
            },
        },
    }


async def main():
    await esperar("Kafka Admin", asegurar_topic_replay)

    productor = AIOKafkaProducer(
        bootstrap_servers=ajustes.kafka_servers,
        acks="all",
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    await esperar("Kafka Producer", productor.start)
    log.info("Productor de REPRODUCCIÓN iniciado sobre topic '%s'", TOPIC_REPLAY)

    # Definir secuencia controlada de prueba de aceptación
    secuencia = [
        # 1. Evento nuevo normal
        ("1. Evento nuevo normal",
         crear_mensaje("replay_001_normal", 1735700000000, 1735700600000, -124.16, 40.80, 4.5, 22.5, "12 km S of Eureka, California")),

        # 2. Reenvío idéntico (Duplicado)
        ("2. Reenvío idéntico (Caso de duplicado)",
         crear_mensaje("replay_001_normal", 1735700000000, 1735700600000, -124.16, 40.80, 4.5, 22.5, "12 km S of Eureka, California")),

        # 3. Evento para posterior revisión
        ("3. Evento inicial (pre-revisión)",
         crear_mensaje("replay_002_revision", 1735705000000, 1735706000000, -71.50, -32.50, 5.1, 35.0, "Central Chile")),

        # 4. Versión posterior con magnitud corregida (updated mayor)
        ("4. Versión posterior (Magnitud revisada de 5.1 a 5.4)",
         crear_mensaje("replay_002_revision", 1735705000000, 1735709000000, -71.50, -32.50, 5.4, 33.0, "Central Chile (Revisado)")),

        # 5. Versión antigua recibida tarde (regresión: updated menor)
        ("5. Versión antigua que llega tarde (debe ignorarse sin sobreescribir la 5.4)",
         crear_mensaje("replay_002_revision", 1735705000000, 1735704000000, -71.50, -32.50, 4.9, 35.0, "Central Chile (Antiguo)")),

        # 6. Evento con Magnitud Nula (ND)
        ("6. Evento con magnitud nula (NULL)",
         crear_mensaje("replay_003_mag_nula", 1735710000000, 1735710500000, -150.00, 61.50, None, 15.0, "Southern Alaska (Sin Mag)")),

        # 7. Evento con Coordenadas Inválidas (Cuarentena)
        ("7. Coordenadas inválidas [lon 250, lat 125] (debe ir a cuarentena)",
         crear_mensaje("replay_004_coords_invalidas", 1735715000000, 1735715200000, 250.00, 125.00, 4.2, 10.0, "Punto Erróneo")),

        # 8. Evento marítimo (Sin país asignado)
        ("8. Evento marítimo en océano Pacífico (sin país asignado)",
         crear_mensaje("replay_005_maritimo", 1735720000000, 1735721000000, -135.50, -55.20, 5.8, 10.0, "Pacific-Antarctic Ridge")),
    ]

    try:
        log.info("Iniciando envío acelerado de %d eventos de reproducción...", len(secuencia))
        for etiqueta, msg in secuencia:
            eid = msg["event"]["id"]
            await productor.send(TOPIC_REPLAY, key=eid, value=msg)
            log.info(">> ENVIADO [%s]: %s (updated=%s)", eid, etiqueta, msg["event"]["updated"])
            await asyncio.sleep(0.3)

        log.info("Reproducción finalizada con éxito en topic '%s'.", TOPIC_REPLAY)

    finally:
        await productor.stop()


if __name__ == "__main__":
    asyncio.run(main())
