# Registro de Evidencias: Pruebas de Aceptación y Demostración (Punto 11)

> **Sistemas de Big Data · Observatorio de Actividad Sísmica**  
> **Fecha de ejecución y validación:** 16 de noviembre de 2026  
> **Resultado global:** 11 / 11 Pruebas Superadas con Éxito (**100 % PASS**)

---

## 1. Matriz de Pruebas de Aceptación

| # | Prueba Obligatoria | Resultado Esperado | Resultado Real Observado | Evidencia Verificada | Estado |
|---|---|---|---|---|:---:|
| **1** | **Recibir un evento nuevo** | Una fila actual, visible en API, mapa y análisis. | Evento insertado en `eventos_actuales` con país asignado y retornado en `/api/eventos`. | `replay_001_normal` insertado; visible en BD y GeoJSON. | **SUPERADA** |
| **2** | **Reenviar la misma versión** | No aumenta el número de terremotos. | Detectado duplicado por `updated_ms <= actual`. Se omite sin aumentar el recuento. | Log: `omitidos_dup=1`. Recuento de filas se mantuvo constante en 1. | **SUPERADA** |
| **3** | **Recibir una versión posterior** | Cambian los datos actuales y las agregaciones afectadas. | `updated_ms > actual`: actualiza campos (mag 5.10 → 5.40) y guarda en `eventos_versiones`. | Fila actualizada en `eventos_actuales`; 2 versiones registradas. | **SUPERADA** |
| **4** | **Recibir después una versión antigua** | No sustituye a la más reciente (protección contra regresión). | Versión con `updated` inferior descartada; la magnitud 5.40 se mantiene intacta. | `mag` permanece en 5.40; versión vieja (4.90) no la sobrescribió. | **SUPERADA** |
| **5** | **Magnitud nula** | Se conserva con tratamiento documentado y se visualiza como ND. | Campo `mag = NULL`, flag `mag_nula = true`. En API/mapa se muestra como `null` / `ND`. | `replay_003_mag_nula` almacenado con `mag IS NULL` (no rellenado con 0). | **SUPERADA** |
| **6** | **Coordenadas inválidas** | Cuarentena y motivo; no aparecen puntos inventados. | Enviado a `eventos_cuarentena`. No se inserta en `eventos_actuales`. | `replay_004_coords_invalidas` [lon 250, lat 125] aislado con motivo exacto. | **SUPERADA** |
| **7** | **Evento marítimo** | Sin país asignado; permanece en el análisis. | `country_id = NULL`, `sin_pais = true`. Se conserva en API y estadísticas. | `replay_005_maritimo` categorizado como marítimo en Pacífico. | **SUPERADA** |
| **8** | **Reiniciar consumidor** | Reanuda sin duplicar los efectos ya persistidos. | Contenedor reiniciado (`docker compose restart consumidor`). Offsets respetados. | Recuento total intacto tras reinicio; 0 duplicados generados. | **SUPERADA** |
| **9** | **Cerrar el túnel ngrok** | El panel muestra el fallo y conserva/identifica la última lectura. | Frontend detecta HTTP timeout/error, enciende alerta visual y mantiene la capa previa. | Función `refresh()` captura excepción y conserva las capas de Leaflet. | **SUPERADA** |
| **10** | **Volver a abrir el túnel** | Se recuperan las consultas automáticamente. | El temporizador periódico reintenta; al volver ngrok, recupera estado verde. | Sincronización restablecida automáticamente en el siguiente ciclo. | **SUPERADA** |
| **11** | **Reproducción histórica** | Procesa por Kafka sin mezclarla con datos live. | Topic `terremotos.replay` y `mode='replay'`. Filtrado estricto en API. | Consultas `/api/eventos` normales solo leen `mode='live'`. | **SUPERADA** |

---

## 2. Evidencias Detalladas por Prueba

### Prueba 1: Evento Nuevo
- **Entrada enviada:** Evento `replay_001_normal`, magnitud 4.5, coordenadas `[-124.16, 40.80]`, California.
- **Salida en base de datos:**
  ```sql
  SELECT id, mag, place, country_id, updated_ms FROM eventos_actuales WHERE id = 'replay_001_normal';
  ```
  ```
          id         | mag  |             place             | country_id |  updated_ms   
  -------------------+------+-------------------------------+------------+---------------
   replay_001_normal | 4.50 | 12 km S of Eureka, California | USA        | 1735700600000
  ```

---

### Pruebas 2, 3 y 4: Idempotencia y Manejo de Revisiones
- **Secuencia:**
  1. Se envió `replay_001_normal` duplicado (mismo timestamp `updated_ms = 1735700600000`).
     - *Log del Consumidor:* `Lote procesado: 1 msgs (insertados=0, actualizados=0, omitidos_dup=1)`
     - *Efecto:* El recuento no aumentó.
  2. Se envió `replay_002_revision` inicial con `mag = 5.10` (`updated = 1735706000000`).
  3. Se envió revisión con `mag = 5.40` (`updated = 1735709000000`).
     - *Log del Consumidor:* `Lote procesado: 1 msgs (insertados=0, actualizados=1)`
  4. Se envió versión obsoleta con `mag = 4.90` (`updated = 1735704000000`).
     - *Log del Consumidor:* `Lote procesado: 1 msgs (insertados=0, actualizados=0, omitidos_dup=1)`
- **Auditoría en `eventos_versiones`:**
  ```
           id          |  updated_ms   | mag  |          place           
  ---------------------+---------------+------+--------------------------
   replay_002_revision | 1735706000000 | 5.10 | Central Chile
   replay_002_revision | 1735709000000 | 5.40 | Central Chile (Revisado)
  ```
  *Conclusión:* La versión de menor timestamp fue ignorada; la tabla actual retiene la versión más reciente (5.40).

---

### Prueba 5: Magnitud Nula (Tratamiento Documentado)
- **Entrada:** `replay_003_mag_nula` con `properties.mag = null`.
- **Registro en PostgreSQL:**
  ```sql
  SELECT id, mag, mag_nula FROM eventos_actuales WHERE id = 'replay_003_mag_nula';
  ```
  ```
           id          | mag  | mag_nula 
  ---------------------+------+----------
   replay_003_mag_nula |      | t
  ```
  *Conclusión:* No se rellena con 0; se almacena `NULL` con flag `mag_nula = true` y se visualiza en la interfaz como `ND`.

---

### Prueba 6: Coordenadas Inválidas y Cuarentena
- **Entrada:** `replay_004_coords_invalidas` con `lon = 250.0`, `lat = 125.0` (fuera del rango WGS84 $[-180, 180]$ y $[-90, 90]$).
- **Log del Consumidor:**
  ```
  WARNING [consumidor] Evento en cuarentena (replay_004_coords_invalidas): LONGITUD_FUERA_DE_RANGO; LATITUD_FUERA_DE_RANGO
  ```
- **Registro en `eventos_cuarentena`:**
  ```
               id              |                     motivo                      |                 detalle                  
  -----------------------------+-------------------------------------------------+------------------------------------------
   replay_004_coords_invalidas | LONGITUD_FUERA_DE_RANGO; LATITUD_FUERA_DE_RANGO | lon=250.0, lat=125.0, time=1735715000000
  ```
  *Conclusión:* El evento erróneo no contamina la tabla de análisis ni genera puntos falsos en el mapa.

---

### Prueba 7: Evento Marítimo
- **Entrada:** `replay_005_maritimo` en coordenadas oceánicas de la dorsal Pacífico-Antártica `[-135.50, -55.20]`.
- **Resultado:**
  ```
           id          | mag  | country_id | sin_pais 
  ---------------------+------+------------+----------
   replay_005_maritimo | 5.80 |            | t
  ```
  *Conclusión:* Se integra en la base de datos con `country_id = NULL` y `sin_pais = true`. No se inventa un país cercano.

---

### Prueba 8: Tolerancia a Fallos y Reinicio del Consumidor
- Se reinició el contenedor mediante: `docker compose restart consumidor`.
- Los offsets confirmados previamente en Kafka impidieron el reprocesamiento innecesario de mensajes antiguos: el recuento de filas permaneció exactamente constante.

---

### Pruebas 9 y 10: Resiliencia ante Caída y Recuperación de Red / Túnel
- **Al cortar el túnel ngrok:**
  - El frontend captura la pérdida de conexión mediante `AbortSignal.timeout(15000)`.
  - La interfaz conmuta el indicador LED a rojo y muestra:
    > *"Aviso: el backend no responde o el túnel está cerrado. Se conserva la última lectura."*
  - Los marcadores de terremotos existentes en el mapa de Leaflet **no se borran**.
- **Al restaurar el túnel:**
  - En el siguiente sondeo automático (`refresh()`), el frontend recibe HTTP 200, conmuta el indicador a verde y sincroniza los nuevos datos sin requerir recargar la página.

---

## 3. Demostración en Vivo: Reproducción Histórica Acelerada

Para ejecutar la demostración de reproducción histórica en la defensa ante el profesor:

```bash
docker exec actividad_sismica-productor-1 python -m src.productor_replay
```

El productor publicará los eventos históricos acelerados con la etiqueta pública `[REPRODUCCIÓN]`, garantizando total aislamiento de los datos en tiempo real mediante el topic `terremotos.replay` y el discriminador `mode = 'replay'`.
