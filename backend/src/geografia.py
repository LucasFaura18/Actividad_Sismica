"""
Enriquecimiento geográfico: asigna país a cada sismo mediante un índice espacial.
"""

import json
import logging
from pathlib import Path

from shapely.geometry import Point, shape
from shapely.strtree import STRtree

log = logging.getLogger("geografia")

GEOJSON_PATH = Path("/app/datos/paises.geojson")


class CatalogoGeografico:
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

        # Índice espacial STRtree para agilizar la búsqueda punto en polígono
        self._tree = STRtree(geometrias)
        log.info("Catálogo geográfico cargado: %d países", len(self._registros))

    def buscar(self, lon: float, lat: float) -> dict:
        punto = Point(lon, lat)

        # Consultar candidatos en el árbol espacial
        indices = self._tree.query(punto, predicate="contains")
        if len(indices) == 0:
            return {
                "country_id": None,
                "country_name": None,
                "continent": None,
                "subregion": None,
            }

        # Si coincide con un país devolvemos sus datos
        primer_match = self._registros[indices[0]]
        return {
            "country_id": primer_match["country_id"],
            "country_name": primer_match["country_name"],
            "continent": primer_match["continent"],
            "subregion": primer_match["subregion"],
        }
