"""
Script de simulación para reproducir casos de prueba (duplicados, revisiones, cuarentena).
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
    format="%(asctime)s %(levelname)s [REPLAY] %(message)s",
)
log = logging.getLogger("productor_replay")

TOPIC_REPLAY = "terremotos.replay"


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
        log.info("Topic %s creado", TOPIC_REPLAY)
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
    log.info("Productor replay iniciado en '%s'", TOPIC_REPLAY)

    # Casos de prueba controlados
    secuencia = [
        # 1. Evento normal
        ("Evento nuevo normal",
         crear_mensaje("replay_001_normal", 1735700000000, 1735700600000, -124.16, 40.80, 4.5, 22.5, "12 km S of Eureka, California")),

        # 2. Reenvío idéntico (duplicado)
        ("Reenvío idéntico (duplicado)",
         crear_mensaje("replay_001_normal", 1735700000000, 1735700600000, -124.16, 40.80, 4.5, 22.5, "12 km S of Eureka, California")),

        # 3. Evento antes de su revisión
        ("Evento inicial",
         crear_mensaje("replay_002_revision", 1735705000000, 1735706000000, -71.50, -32.50, 5.1, 35.0, "Central Chile")),

        # 4. Revisión posterior con magnitud corregida
        ("Magnitud revisada (de 5.1 a 5.4)",
         crear_mensaje("replay_002_revision", 1735705000000, 1735709000000, -71.50, -32.50, 5.4, 33.0, "Central Chile (Revisado)")),

        # 5. Versión atrasada (regresión)
        ("Versión antigua que llega tarde (no debe sobreescribir)",
         crear_mensaje("replay_002_revision", 1735705000000, 1735704000000, -71.50, -32.50, 4.9, 35.0, "Central Chile (Antiguo)")),

        # 6. Evento con magnitud nula
        ("Evento con magnitud nula",
         crear_mensaje("replay_003_mag_nula", 1735710000000, 1735710500000, -150.00, 61.50, None, 15.0, "Southern Alaska (Sin Mag)")),

        # 7. Evento con coordenadas fuera de rango
        ("Coordenadas fuera de rango (cuarentena)",
         crear_mensaje("replay_004_coords_invalidas", 1735715000000, 1735715200000, 250.00, 125.00, 4.2, 10.0, "Punto Erróneo")),

        # 8. Evento marítimo sin país
        ("Evento marítimo (sin país)",
         crear_mensaje("replay_005_maritimo", 1735720000000, 1735721000000, -135.50, -55.20, 5.8, 10.0, "Pacific-Antarctic Ridge")),
    ]

    try:
        log.info("Enviando %d eventos de prueba...", len(secuencia))
        for etiqueta, msg in secuencia:
            eid = msg["event"]["id"]
            await productor.send(TOPIC_REPLAY, key=eid, value=msg)
            log.info(">> Enviado [%s]: %s (updated=%s)", eid, etiqueta, msg["event"]["updated"])
            await asyncio.sleep(0.3)

        log.info("Prueba de replay finalizada.")

    finally:
        await productor.stop()


if __name__ == "__main__":
    asyncio.run(main())
