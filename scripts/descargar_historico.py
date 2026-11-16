"""
Descarga del histórico USGS — año 2025.

Parámetros fijados:
  - Ámbito:      mundial
  - Magnitud:    >= 4.0
  - Tipo:        earthquake
  - Período:     2025-01-01 → 2026-01-01 (12 meses)
  - Orden:       time-asc

Ejecutar UNA VEZ en el equipo anfitrión (no dentro de Docker):

    pip install httpx
    python scripts/descargar_historico.py

Los archivos se guardan en datos/historico/<YYYY-MM>.geojson.
Si ya existen se reutilizan sin volver a consultar la API.
El manifiesto datos/historico/manifest.json registra:
  - Fecha de extracción
  - URL y parámetros de cada consulta
  - Número de eventos por mes y total
  - Eventos deduplicados entre intervalos
"""

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [historico] %(message)s",
)
log = logging.getLogger("historico")

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
DIRECTORIO = Path(__file__).parent.parent / "datos" / "historico"
MANIFEST = DIRECTORIO / "manifest.json"
MAX_RESULTADOS = 20_000  # Límite documentado de la API USGS; si se alcanza, se subdivide

FILTROS_BASE = {
    "format": "geojson",
    "minmagnitude": "4",
    "eventtype": "earthquake",
    "orderby": "time-asc",
}

# 12 intervalos mensuales del año 2025.
# Los extremos de cada intervalo son [starttime, endtime) para evitar
# solapamientos; la API puede incluir ambos extremos, por lo que se
# deduplica por id antes de guardar.
MESES_2025 = [
    ("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z", "2025-01"),
    ("2025-02-01T00:00:00Z", "2025-03-01T00:00:00Z", "2025-02"),
    ("2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z", "2025-03"),
    ("2025-04-01T00:00:00Z", "2025-05-01T00:00:00Z", "2025-04"),
    ("2025-05-01T00:00:00Z", "2025-06-01T00:00:00Z", "2025-05"),
    ("2025-06-01T00:00:00Z", "2025-07-01T00:00:00Z", "2025-06"),
    ("2025-07-01T00:00:00Z", "2025-08-01T00:00:00Z", "2025-07"),
    ("2025-08-01T00:00:00Z", "2025-09-01T00:00:00Z", "2025-08"),
    ("2025-09-01T00:00:00Z", "2025-10-01T00:00:00Z", "2025-09"),
    ("2025-10-01T00:00:00Z", "2025-11-01T00:00:00Z", "2025-10"),
    ("2025-11-01T00:00:00Z", "2025-12-01T00:00:00Z", "2025-11"),
    ("2025-12-01T00:00:00Z", "2026-01-01T00:00:00Z", "2025-12"),
]


async def descargar_intervalo(
    cliente: httpx.AsyncClient,
    inicio: str,
    fin: str,
    etiqueta: str,
) -> tuple[list[dict], list[dict]]:
    """
    Descarga un intervalo temporal de la API USGS.
    Devuelve (features, registros_consulta).

    Si la respuesta supera MAX_RESULTADOS, subdivide el intervalo en dos
    mitades y recursa. Nunca trunca silenciosamente.
    """
    params = {**FILTROS_BASE, "starttime": inicio, "endtime": fin}
    url_completa = f"{BASE_URL}?" + "&".join(f"{k}={v}" for k, v in params.items())
    log.info("GET %s → %s  [%s]", inicio, fin, etiqueta)

    t0 = asyncio.get_event_loop().time()
    respuesta = await cliente.get(BASE_URL, params=params, timeout=120)
    respuesta.raise_for_status()
    latencia = asyncio.get_event_loop().time() - t0

    datos = respuesta.json()
    features = datos.get("features", [])
    cuenta_api = datos.get("metadata", {}).get("count", len(features))

    registro = {
        "etiqueta": etiqueta,
        "inicio": inicio,
        "fin": fin,
        "url": url_completa,
        "cuenta_api": cuenta_api,
        "eventos_guardados": 0,  # se actualiza al final
        "latencia_s": round(latencia, 2),
        "subdividido": False,
    }

    if cuenta_api >= MAX_RESULTADOS:
        log.warning(
            "%s: %d >= %d (límite). Subdividiendo intervalo para no truncar…",
            etiqueta, cuenta_api, MAX_RESULTADOS,
        )
        registro["subdividido"] = True
        # Partir el intervalo exactamente por la mitad
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        dt_ini = datetime.strptime(inicio, fmt).replace(tzinfo=timezone.utc)
        dt_fin = datetime.strptime(fin, fmt).replace(tzinfo=timezone.utc)
        mitad = dt_ini + (dt_fin - dt_ini) / 2
        mitad_str = mitad.strftime(fmt)

        features_a, regs_a = await descargar_intervalo(cliente, inicio, mitad_str, f"{etiqueta}a")
        features_b, regs_b = await descargar_intervalo(cliente, mitad_str, fin, f"{etiqueta}b")
        return features_a + features_b, [registro] + regs_a + regs_b

    log.info("%s: %d eventos (%.2fs)", etiqueta, len(features), latencia)
    return features, [registro]


async def main() -> None:
    DIRECTORIO.mkdir(parents=True, exist_ok=True)

    fecha_extraccion = datetime.now(timezone.utc).isoformat()
    todos_ids: set[str] = set()
    total_eventos = 0
    todos_registros: list[dict] = []
    resumen_meses: list[dict] = []

    async with httpx.AsyncClient() as cliente:
        for inicio, fin, etiqueta in MESES_2025:
            archivo = DIRECTORIO / f"{etiqueta}.geojson"

            if archivo.exists():
                log.info("%s ya existe — reutilizando", archivo.name)
                with open(archivo, encoding="utf-8") as f:
                    coleccion = json.load(f)
                features = coleccion.get("features", [])
                # Registrar en ids para deduplicar con meses futuros
                todos_ids |= {f["id"] for f in features}
                total_eventos += len(features)
                resumen_meses.append({"mes": etiqueta, "eventos": len(features), "archivo": str(archivo), "reutilizado": True})
                continue

            # Descargar
            features_brutos, registros = await descargar_intervalo(cliente, inicio, fin, etiqueta)

            # Deduplicar respecto a meses ya procesados (extremos compartidos)
            features_unicos = [f for f in features_brutos if f["id"] not in todos_ids]
            deduplicados = len(features_brutos) - len(features_unicos)
            if deduplicados:
                log.info("%s: %d eventos deduplicados entre intervalos", etiqueta, deduplicados)

            # Actualizar estado global
            todos_ids |= {f["id"] for f in features_unicos}
            total_eventos += len(features_unicos)

            # Actualizar cuenta en los registros de esta descarga
            for reg in registros:
                if not reg.get("subdividido"):
                    reg["eventos_guardados"] = len(features_unicos)
            todos_registros.extend(registros)

            # Guardar GeoJSON del mes
            coleccion = {
                "type": "FeatureCollection",
                "metadata": {
                    "mes": etiqueta,
                    "filtros": FILTROS_BASE,
                    "fecha_extraccion": fecha_extraccion,
                    "eventos": len(features_unicos),
                    "deduplicados": deduplicados,
                },
                "features": features_unicos,
            }
            with open(archivo, "w", encoding="utf-8") as f:
                json.dump(coleccion, f, ensure_ascii=False)

            log.info("%s guardado: %d eventos en %s", etiqueta, len(features_unicos), archivo.name)
            resumen_meses.append({"mes": etiqueta, "eventos": len(features_unicos), "archivo": str(archivo), "reutilizado": False})

            # Pausa cortés entre peticiones a la API pública
            await asyncio.sleep(1.5)

    # Manifiesto global
    manifest = {
        "descripcion": "Histórico USGS 2025 · mundial · magnitud >= 4 · tipo earthquake",
        "fecha_extraccion": fecha_extraccion,
        "filtros_base": FILTROS_BASE,
        "total_eventos": total_eventos,
        "total_ids_unicos": len(todos_ids),
        "meses": resumen_meses,
        "consultas_detalle": todos_registros,
    }
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    log.info("=" * 60)
    log.info("Descarga completa: %d eventos únicos", total_eventos)
    log.info("Manifiesto: %s", MANIFEST)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Cancelado por el usuario")
        sys.exit(0)
