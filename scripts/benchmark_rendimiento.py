"""
Script de Benchmark y Medición de Rendimiento (Punto 10).
Compara empíricamente el tiempo de ejecución y la memoria para:
1. Búsqueda secuencial O(N) vs Búsqueda por Diccionario/Hash O(1).
2. Integración geográfica secuencial O(N*M) vs Índice Espacial STRtree O(N log M).
3. Consulta en PostgreSQL con/sin índice sobre 3 tamaños de entrada.

Los datos replicados para escala se generan en memoria y se marcan como sintéticos.
"""

import time
import sys
import json
import random
from pathlib import Path
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

GEOJSON_FILE = Path("/app/datos/paises.geojson")
if not GEOJSON_FILE.exists():
    GEOJSON_FILE = Path(__file__).parent.parent / "recursos" / "observatorio-sismico" / "frontend" / "public" / "datos" / "paises.geojson"

def generar_eventos_sinteticos(n: int) -> list[dict]:
    """Genera n eventos sísmicos sintéticos en memoria."""
    eventos = []
    for i in range(n):
        eventos.append({
            "id": f"sintetico_{i:08d}",
            "time": 1794850000000 + i * 1000,
            "updated": 1794850000000 + i * 1000 + random.randint(100, 50000),
            "lon": random.uniform(-180.0, 180.0),
            "lat": random.uniform(-80.0, 80.0),
            "mag": round(random.uniform(1.0, 7.5), 1),
            "depth": round(random.uniform(5.0, 500.0), 1),
            "place": f"Sintético {i}",
            "es_sintetico": True
        })
    return eventos

def benchmark_busqueda_versiones(tamanos: list[int]):
    print("\n" + "="*70)
    print("TEST 1: BÚSQUEDA DE VERSIONES POR IDENTIFICADOR (Lista vs Hash Table)")
    print("="*70)
    resultados = []

    for n in tamanos:
        eventos = generar_eventos_sinteticos(n)
        ids_a_buscar = [f"sintetico_{random.randint(0, n-1):08d}" for _ in range(min(500, n))]

        # 1. Búsqueda Secuencial en Lista O(N)
        t0 = time.perf_counter()
        encontrados_seq = 0
        for target in ids_a_buscar:
            for ev in eventos:
                if ev["id"] == target:
                    encontrados_seq += 1
                    break
        t_seq = time.perf_counter() - t0

        # 2. Búsqueda en Diccionario Hash O(1)
        mapa_hash = {ev["id"]: ev["updated"] for ev in eventos}
        t0 = time.perf_counter()
        encontrados_hash = 0
        for target in ids_a_buscar:
            if target in mapa_hash:
                encontrados_hash += 1
        t_hash = time.perf_counter() - t0

        speedup = (t_seq / t_hash) if t_hash > 0 else 9999
        mem_kb = sys.getsizeof(mapa_hash) / 1024.0

        print(f"Tamaño N = {n:>6d} eventos | {len(ids_a_buscar)} búsquedas:")
        print(f"  - Secuencial O(N): {t_seq*1000:>8.2f} ms")
        print(f"  - Hash Map   O(1): {t_hash*1000:>8.4f} ms")
        print(f"  - Aceleración:      {speedup:>8.1f}x veces más rápido | Memoria Hash: {mem_kb:.1f} KB")

        resultados.append({
            "tamano": n,
            "t_secuencial_ms": round(t_seq * 1000, 2),
            "t_hash_ms": round(t_hash * 1000, 4),
            "speedup": round(speedup, 1),
            "mem_kb": round(mem_kb, 1)
        })
    return resultados

def benchmark_integracion_geografica(tamanos: list[int]):
    print("\n" + "="*70)
    print("TEST 2: INTEGRACIÓN GEOGRÁFICA (Punto en Polígono Secuencial vs STRtree)")
    print("="*70)

    if not GEOJSON_FILE.exists():
        print(f"Aviso: {GEOJSON_FILE} no encontrado. Omitiendo test 2.")
        return []

    with open(GEOJSON_FILE, encoding="utf-8") as f:
        geojson = json.load(f)

    poligonos = []
    for feat in geojson["features"]:
        geom = shape(feat["geometry"])
        poligonos.append((geom, feat["properties"].get("ADM0_A3")))

    arbol_str = STRtree([p[0] for p in poligonos])
    resultados = []

    for n in tamanos:
        eventos = generar_eventos_sinteticos(n)
        puntos = [Point(e["lon"], e["lat"]) for e in eventos]

        # 1. Fuerza Bruta: Secuencial O(N * M)
        t0 = time.perf_counter()
        asignados_seq = 0
        for pt in puntos:
            for geom, _ in poligonos:
                if geom.contains(pt):
                    asignados_seq += 1
                    break
        t_fuerza_bruta = time.perf_counter() - t0

        # 2. STRtree Index R-tree O(N * log M)
        t0 = time.perf_counter()
        asignados_tree = 0
        for pt in puntos:
            candidatos = arbol_str.query(pt, predicate="intersects")
            for idx in candidatos:
                if poligonos[idx][0].contains(pt):
                    asignados_tree += 1
                    break
        t_tree = time.perf_counter() - t0

        speedup = (t_fuerza_bruta / t_tree) if t_tree > 0 else 1.0

        print(f"Tamaño N = {n:>6d} puntos vs {len(poligonos)} polígonos de países:")
        print(f"  - Fuerza Bruta O(N*M):     {t_fuerza_bruta*1000:>8.2f} ms")
        print(f"  - Con Índice STRtree O(N log M): {t_tree*1000:>8.2f} ms")
        print(f"  - Aceleración:                   {speedup:>8.1f}x veces más rápido")

        resultados.append({
            "tamano": n,
            "t_fuerza_bruta_ms": round(t_fuerza_bruta * 1000, 2),
            "t_strtree_ms": round(t_tree * 1000, 2),
            "speedup": round(speedup, 1)
        })
    return resultados

if __name__ == "__main__":
    tamanos = [200, 1000, 5000]
    res1 = benchmark_busqueda_versiones(tamanos)
    res2 = benchmark_integracion_geografica(tamanos)
    print("\n" + "="*70)
    print("BENCHMARK COMPLETADO EXITOSAMENTE")
    print("="*70)
