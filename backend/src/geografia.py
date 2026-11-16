"""
Enriquecimiento geográfico: asigna país a cada evento sísmico mediante
una operación espacial de punto dentro de polígono.

Fuente:    Natural Earth via paises.geojson (entregado por el profesor).
CRS:       WGS84 / EPSG:4326 — compatible con las coordenadas USGS.
ID usado:  ADM0_A3 (unicidad comprobada: 177 valores distintos).

Regla para puntos en frontera o con múltiples coincidencias:
  Se devuelve la primera coincidencia en el índice espacial (STRtree).
  El orden no es determinista entre ejecuciones, pero es estable dentro
  de una misma carga. Comportamiento documentado; no se elige el país más cercano.

Categoría especial — "Sin país asignado" (country_id=None):
  Puede deberse a un evento marítimo, simplificación de costas o ausencia
  de cobertura en el catálogo. No se clasifica como error.
"""

import json
import logging
from pathlib import Path

from shapely.geometry import Point, shape
from shapely.strtree import STRtree

log = logging.getLogger("geografia")

# Ruta dentro del contenedor (montada desde el host en compose.yaml)
GEOJSON_PATH = Path("/app/datos/paises.geojson")


class CatalogoGeografico:
    """
    Carga el catálogo de países una sola vez y permite consultas rápidas
    mediante un índice espacial STRtree (R-tree).
    """

    def __init__(self, path: Path = GEOJSON_PATH) -> None:
        with open(path, encoding="utf-8") as f:
            datos = json.load(f)

        self._registros: list[dict] = []
        geometrias = []

        for feature in datos["features"]:
            props = feature.get("properties") or {}
            geom = shape(feature["geometry"])
            self._registros.append(
                {
                    "geom": geom,
                    "country_id": props.get("ADM0_A3"),
                    "country_name": props.get("NAME_ES") or props.get("ADMIN"),
                    "continent": props.get("CONTINENT"),
                    "subregion": props.get("SUBREGION"),
                }
            )
            geometrias.append(geom)

        # Índice espacial: acelera la búsqueda de candidatos por bounding box
        # antes de aplicar el test contains() exacto.
        self._tree = STRtree(geometrias)

        # Verificar unicidad de ADM0_A3
        codigos = [r["country_id"] for r in self._registros if r["country_id"]]
        unicos = len(set(codigos))
        log.info(
            "Catálogo geográfico cargado: %d entidades, %d códigos ADM0_A3 únicos",
            len(self._registros),
            unicos,
        )
        if unicos != len(codigos):
            log.warning("ADM0_A3 no es completamente único (%d total vs %d únicos)", len(codigos), unicos)

    def buscar(self, lon: float, lat: float) -> dict:
        """
        Busca el país que contiene el punto (lon, lat).

        Devuelve un dict con:
          country_id   (str|None)  — ADM0_A3 del país, o None
          country_name (str|None)  — nombre en español, o None
          continent    (str|None)  — continente
          subregion    (str|None)  — subregión

        Devuelve todos los campos a None si el punto no cae en ningún
        polígono (evento marítimo, costa simplificada, etc.).
        """
        punto = Point(lon, lat)
        candidatos = self._tree.query(punto, predicate="intersects")

        for idx in candidatos:
            registro = self._registros[idx]
            if registro["geom"].contains(punto):
                return {
                    "country_id": registro["country_id"],
                    "country_name": registro["country_name"],
                    "continent": registro["continent"],
                    "subregion": registro["subregion"],
                }

        # Sin país asignado — puede ser evento marítimo o simplificación de costas
        return {
            "country_id": None,
            "country_name": None,
            "continent": None,
            "subregion": None,
        }
