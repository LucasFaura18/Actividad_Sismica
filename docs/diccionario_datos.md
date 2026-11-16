# Diccionario de Datos y Registro de Consultas (Punto 12)

> **Observatorio de Actividad Sísmica**  
> **Sistema de Coordenadas:** WGS84 (EPSG:4326) · **Tiempos:** Milisegundos desde Unix Epoch (UTC)

---

## 1. Procedencia y Registro de Consultas de Extracción

### 1.1 Feed en Tiempo Real (USGS)
- **URL Consultada:** `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson`
- **Frecuencia:** Cada 60 segundos por HTTP GET mediante cliente asíncrono `httpx`.
- **Naturaleza del Feed:** Ventana móvil de las últimas 24 horas. Cada respuesta devuelve la ventana completa; no es push.
- **Filtro de Ingesta:** `properties.type == 'earthquake'`. Cualquier otro tipo (explosiones de cantera, colapsos acústicos, etc.) se excluye registrando el motivo.

### 1.2 Catálogo Histórico (USGS FDSNWS)
- **URL Base:** `https://earthquake.usgs.gov/fdsnws/event/1/query`
- **Parámetros:** `format=geojson&starttime=YYYY-MM-01T00:00:00Z&endtime=YYYY-MM-01T00:00:00Z&minmagnitude=4&eventtype=earthquake&orderby=time-asc`
- **Límite de API:** 20.000 eventos por petición; particionado recursivo en dos mitades si se alcanza el umbral.

### 1.3 Catálogo Geográfico (Natural Earth)
- **Fuente:** `paises.geojson` (Cultural 1:110m).
- **Entidades:** 177 polígonos territoriales identificados unívocamente por código `ADM0_A3`.

---

## 2. Diccionario de Datos por Tabla

### Tabla: `eventos_actuales` (Hechos - Estado Vigente)
Representa la versión más reciente de cada terremoto válido conocido.

| Campo | Tipo SQL | Nulo | Origen / Proveniencia | Descripción y Rango |
|---|---|:---:|---|---|
| `id` | `TEXT` | **NO** | `feature.id` (USGS) | Clave primaria. Identificador alfanumérico único asignado por USGS. |
| `time_ms` | `BIGINT` | **NO** | `properties.time` | Momento de ocurrencia del sismo en milisegundos Unix epoch UTC. |
| `updated_ms` | `BIGINT` | **NO** | `properties.updated` | Momento de la última revisión del evento por USGS en milisegundos UTC. |
| `ingested_at_ms` | `BIGINT` | **NO** | Productor Python | Timestamp en que el productor local extrajo y publicó el mensaje. |
| `procesado_en` | `TIMESTAMPTZ`| **NO** | Consumidor Python | Fecha y hora del sistema en que el consumidor completó la persistencia. |
| `mag` | `NUMERIC(5,2)`| SÍ | `properties.mag` | Magnitud sismológica. `NULL` si no está disponible (no rellenado con 0). |
| `mag_type` | `TEXT` | SÍ | `properties.magType` | Método de cálculo de magnitud (`ml`, `mb`, `mww`, etc.). |
| `place` | `TEXT` | SÍ | `properties.place` | Descripción textual aproximada del epicentro respecto a una población. |
| `lon` | `DOUBLE PRECISION`| **NO** | `geometry.coordinates[0]`| Longitud geográfica en grados decimales WGS84 $[-180.0, 180.0]$. |
| `lat` | `DOUBLE PRECISION`| **NO** | `geometry.coordinates[1]`| Latitud geográfica en grados decimales WGS84 $[-90.0, 90.0]$. |
| `depth_km` | `DOUBLE PRECISION`| SÍ | `geometry.coordinates[2]`| Profundidad del hipocentro en km. Valores negativos indican cotas sobre nivel del mar. |
| `country_id` | `TEXT` | SÍ | `dim_pais.country_id` | Código ADM0_A3 del país tras cruce espacial. `NULL` si es evento marítimo. |
| `country_name` | `TEXT` | SÍ | `dim_pais.country_name` | Nombre común del país en español. |
| `continent` | `TEXT` | SÍ | `dim_pais.continent` | Continente asignado. |
| `subregion` | `TEXT` | SÍ | `dim_pais.subregion` | Subregión geográfica asignada. |
| `source` | `TEXT` | **NO** | Mensaje Kafka | Fuente de extracción (`usgs_live` o `usgs_replay_2025`). |
| `mode` | `TEXT` | **NO** | Mensaje Kafka | Modo de operación (`live` para tiempo real, `replay` para simulación). |
| `mag_nula` | `BOOLEAN` | **NO** | Consumidor Python | Flag de calidad: indica si la magnitud llegó nula. |
| `mag_negativa` | `BOOLEAN` | **NO** | Consumidor Python | Flag de calidad: indica si la magnitud es negativa ($< 0.0$). |
| `depth_nula` | `BOOLEAN` | **NO** | Consumidor Python | Flag de calidad: indica si la profundidad no estaba informada. |
| `depth_negativa` | `BOOLEAN` | **NO** | Consumidor Python | Flag de calidad: profundidad sobre el nivel del mar ($< 0.0\text{ km}$). |
| `place_nulo` | `BOOLEAN` | **NO** | Consumidor Python | Flag de calidad: descripción de lugar ausente o vacía. |
| `sin_pais` | `BOOLEAN` | **NO** | Consumidor Python | Flag: sismo en océano o costa simplificada sin país asignado. |

---

### Tabla: `dim_pais` (Dimensión Geográfica)
Catálogo vectorial de referencia territorial cargado desde Natural Earth.

| Campo | Tipo SQL | Nulo | Descripción |
|---|---|:---:|---|
| `country_id` | `TEXT` | **NO** | Clave primaria. Código de tres caracteres `ADM0_A3` (177 valores únicos). |
| `country_name` | `TEXT` | **NO** | Nombre del país en español (`NAME_ES` o `ADMIN`). |
| `continent` | `TEXT` | SÍ | Continente al que pertenece la entidad territorial. |
| `subregion` | `TEXT` | SÍ | Subregión geopolítica (ej. *South America*, *Southern Europe*). |

---

### Tabla: `eventos_versiones` (Historial de Revisiones)
Trazabilidad completa de cada versión recibida de un evento.

| Campo | Tipo SQL | Nulo | Descripción |
|---|---|:---:|---|
| `id` | `TEXT` | **NO** | Clave primaria compuesta (con `updated_ms`). ID del terremoto. |
| `updated_ms` | `BIGINT` | **NO** | Clave primaria compuesta. Timestamp de la revisión observada. |
| `procesado_en`| `TIMESTAMPTZ`| **NO** | Fecha de auditoría en la que el consumidor guardó la versión. |
| `mag` | `NUMERIC(5,2)`| SÍ | Magnitud asignada en dicha revisión específica. |
| `mag_type` | `TEXT` | SÍ | Tipo de magnitud en dicha revisión. |
| `place` | `TEXT` | SÍ | Descripción de lugar en dicha revisión. |
| `lon`, `lat` | `DOUBLE PRECISION`| **NO** | Coordenadas WGS84 observadas en dicha revisión. |
| `depth_km` | `DOUBLE PRECISION`| SÍ | Profundidad en kilómetros. |

---

### Tabla: `eventos_cuarentena` (Aislamiento de Anomalías)
Eventos descartados por incumplir restricciones de calidad e integridad.

| Campo | Tipo SQL | Nulo | Descripción |
|---|---|:---:|---|
| `cuarentena_id` | `BIGSERIAL` | **NO** | Clave primaria autonumérica. |
| `id` | `TEXT` | SÍ | Identificador del evento (si estaba presente). |
| `updated_ms` | `BIGINT` | SÍ | Timestamp de revisión (si era válido). |
| `motivo` | `TEXT` | **NO** | Código de error (ej. `LONGITUD_FUERA_DE_RANGO`, `JSON_INVALIDO`). |
| `detalle` | `TEXT` | SÍ | Valores numéricos o descripción de la anomalía observada. |
| `recibido_en` | `TIMESTAMPTZ`| **NO** | Fecha del sistema en que se interceptó el registro erróneo. |
| `payload_raw` | `JSONB` | **NO** | JSON original íntegro para auditoría y reproducción técnica. |
