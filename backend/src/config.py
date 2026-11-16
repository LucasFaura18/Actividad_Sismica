import asyncio
import logging
import os
from dataclasses import dataclass

log = logging.getLogger("observatorio")


def _entero(nombre: str, defecto: int) -> int:
    bruto = os.environ.get(nombre)
    if bruto is None or bruto == "":
        return defecto
    return int(bruto)


@dataclass(frozen=True)
class Ajustes:
    kafka_servers: str
    kafka_topic: str
    kafka_group: str
    kafka_retention_ms: int
    poll_seconds: int
    postgres_dsn: str
    cors_origins: tuple[str, ...]


def cargar() -> Ajustes:
    usuario = os.environ.get("POSTGRES_USER", "observatorio")
    clave = os.environ.get("POSTGRES_PASSWORD", "observatorio_local")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    puerto = os.environ.get("POSTGRES_PORT", "5432")
    base = os.environ.get("POSTGRES_DB", "observatorio")
    dsn = os.environ.get("POSTGRES_DSN") or f"postgresql://{usuario}:{clave}@{host}:{puerto}/{base}"
    origenes = tuple(
        parte.strip()
        for parte in os.environ.get("CORS_ORIGINS", "http://localhost:4200,http://127.0.0.1:4200").split(",")
        if parte.strip()
    )
    return Ajustes(
        kafka_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        kafka_topic=os.environ.get("KAFKA_TOPIC", "terremotos.live"),
        kafka_group=os.environ.get("KAFKA_GROUP", "observatorio-consumidor"),
        kafka_retention_ms=_entero("KAFKA_TOPIC_RETENTION_MS", 72 * 60 * 60 * 1000),
        poll_seconds=_entero("POLL_SECONDS", 60),
        postgres_dsn=dsn,
        cors_origins=origenes,
    )


ajustes = cargar()


async def esperar(etiqueta: str, accion, intentos: int = 30):
    """Reintenta una corrutina con espera progresiva. El orden de Compose no basta."""
    espera = 2
    ultimo = None
    for numero in range(1, intentos + 1):
        try:
            return await accion()
        except Exception as exc:
            ultimo = exc
            log.warning(
                "%s no responde (%s/%s): %s. Reintento en %ss",
                etiqueta,
                numero,
                intentos,
                exc,
                espera,
            )
            await asyncio.sleep(espera)
            espera = min(espera * 2, 30)
    raise RuntimeError(f"{etiqueta} no estuvo disponible") from ultimo
