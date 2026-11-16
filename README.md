# 🌍 Proyecto: Observatorio de Actividad Sísmica

**Sistemas de Big Data · Consolidación de las sesiones 1–8 · RA1**  
- **Panel Web Desplegado (Netlify):** https://actividad-sismica.netlify.app
- **Sistema distribuido en contenedores Docker** para la ingesta en tiempo real, transporte en streaming, enriquecimiento geográfico, persistencia relacional y análisis comparativo de terremotos a nivel global.

---

## 🏛️ Arquitectura del Sistema

```
                      ┌──────────────────────┐
                      │ USGS Earthquake Feed │ (HTTP GET / 60s)
                      └──────────┬───────────┘
                                 │
                                 ▼
┌── Docker Compose ────────────────────────────────────────────────────────┐
│                                                                          │
│  [ productor ] Python 3.12 + aiokafka + httpx                           │
│        │                                                                 │
│        ▼ (topic: terremotos.live / terremotos.replay)                    │
│  [ kafka ] Apache Kafka 4.1.0 (KRaft sin ZooKeeper)                      │
│        │                                                                 │
│        ▼ (at-least-once con commit manual)                               │
│  [ consumidor ] Python 3.12 + Shapely (STRtree R-Tree)                   │
│        │                                                                 │
│        ▼ (transacción ACID / upsert idempotente)                         │
│  [ postgres ] PostgreSQL 16.10 (eventos_actuales, dim_pais, cuarentena)  │
│        ▲                                                                 │
│        │ (consultas preparadas / pool asyncpg)                           │
│  [ api ] Python 3.12 + FastAPI + Uvicorn (0.0.0.0:8000)                  │
│                                                                          │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 │ (127.0.0.1:8000)
                                 ▼
                        [ Túnel ngrok ] (HTTPS público con TLS)
                                 ▲
                                 │ (Fetch cada 60s / CORS)
                        [ Panel Netlify ] (SPA Angular 20 + Leaflet)
```

---

## 🚀 Puesta en Marcha Rápida (Entorno Limpio)

### 1. Requisitos Previos
- Docker Engine y Docker Compose v2.
- Puertos libres en `127.0.0.1`: `8000` (API), `5432` (PostgreSQL) y `9092` (Kafka).

### 2. Configuración y Arranque
```bash
# 1. Copiar la plantilla de variables de entorno
copy .env.example .env

# 2. Levantar los 5 servicios obligatorios
docker compose up --build -d

# 3. Comprobar que los 5 contenedores estén en estado healthy
docker compose ps
```

### 3. Verificación de Endpoints
```bash
# Comprobación de salud e ingesta
curl http://127.0.0.1:8000/api/salud

# Consulta GeoJSON FeatureCollection
curl http://127.0.0.1:8000/api/eventos

# Métricas estadísticas y comparación con 2025
curl http://127.0.0.1:8000/api/estadisticas
```

### 4. Lanzar el Panel Angular en Local (Desarrollo)
```bash
cd recursos/observatorio-sismico/frontend
npm install
npm start
# Abrir en el navegador: http://localhost:4200
```

---

## 📚 Índice de Entregables y Documentación (Punto 12)

| Documento / Entregable | Ubicación | Descripción |
|---|---|---|
| **Modelo de Datos** | [docs/modelo_datos.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/modelo_datos.md) | Modelo conceptual (Mermaid), lógico y físico, grano, claves e índices B-Tree. |
| **Diccionario de Datos** | [docs/diccionario_datos.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/diccionario_datos.md) | Registro de procedencia, tipos, rangos y nulos de cada campo y tabla. |
| **Calidad de Datos** | [sql/consultas_calidad.sql](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/sql/consultas_calidad.sql) | Consultas SQL de las 6 dimensiones de calidad, análisis antes/después y cuarentena. |
| **Análisis y Conclusiones** | [docs/analisis_conclusiones.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/analisis_conclusiones.md) | 3 conclusiones fundamentadas con cifras, filtros, retrasos de reporte y sesgos. |
| **Complejidad y Costes** | [docs/informe_complejidad_costes.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/informe_complejidad_costes.md) | Benchmarks empíricos (3 tamaños), análisis a $10^8$ registros y TCO anual. |
| **Pruebas de Aceptación** | [docs/evidencias_pruebas_aceptacion.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/evidencias_pruebas_aceptacion.md) | Matriz 11/11 pruebas superadas (duplicados, regresión, cuarentena, túnel). |
| **Guía de Despliegue** | [docs/despliegue_ngrok_netlify.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/despliegue_ngrok_netlify.md) | Instrucciones paso a paso para ngrok y Netlify en la defensa. |
| **Guión Defensa Oral** | [docs/guion_defensa_oral.md](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/docs/guion_defensa_oral.md) | Resumen del flujo completo y explicación de una decisión técnica propia. |

---

## 🎮 Demostración en Vivo para la Defensa

### 1. Activar el Túnel HTTPS
```bash
ngrok http 8000
```
Indica la URL generada en `recursos/observatorio-sismico/frontend/public/config.json` y compila con `npm run build`.

### 2. Ejecutar la Reproducción Histórica Acelerada
Para demostrar actividad en vivo sin esperar eventos naturales:
```bash
docker exec actividad_sismica-productor-1 python -m src.productor_replay
```
Los eventos históricos se inyectan por Kafka aislados en el topic `terremotos.replay` bajo la etiqueta `[REPRODUCCIÓN]`, ejecutando casos controlados de duplicado, revisión de magnitud y cuarentena.
