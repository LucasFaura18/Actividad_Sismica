# Informe de Complejidad Algorítmica, Rendimiento y Estimación de Costes (Punto 10)

> **Sistemas de Big Data · Observatorio de Actividad Sísmica**  
> **Fecha:** Noviembre de 2026  
> **Entorno de pruebas:** Intel/AMD x86_64, 16 GB RAM, almacenamiento NVMe, Docker Desktop sobre Windows.

---

## 1. Justificación Algorítmica de Estructuras y Operaciones

Se analizan las tres operaciones fundamentales del pipeline sismológico, contrastando su rendimiento asintótico (notación Big-O) en caso medio y peor caso:

| Operación / Estructura | Estructura Utilizada | Alternativa Ingenua | Coste Medio | Peor Caso | Justificación Técnica |
|---|---|---|---|---|---|
| **1. Búsqueda de versiones por ID** | Tabla Hash en memoria (`dict`) e índice **B-Tree** en PostgreSQL | Búsqueda secuencial en lista / `Seq Scan` en SQL | **$O(1)$** en memoria / **$O(\log N)$** en BD | **$O(N)$** (colisiones hash extremas) / **$O(\log N)$** en BD | Permite comprobar de forma instantánea si una revisión de un terremoto ya fue persistida antes de ejecutar el upsert, evitando escaneos lineales continuos de toda la tabla. |
| **2. Integración geográfica (Punto en Polígono)** | **STRtree (R-Tree)** con filtro de bounding box espacial en Shapely | Fuerza bruta: probar punto contra los 177 polígonos de países | **$O(N \log M)$** ($N$ eventos, $M$ países) | **$O(N \cdot M)$** (cuando todos los polígonos intersectan el mismo punto) | El árbol R-Tree filtra los polígonos candidatos en $O(\log M)$ mediante sus rectángulos envolventes (AABB) antes de evaluar el algoritmo exacto *Ray-Casting* / *PIP*. |
| **3. Agregación temporal por ventanas de 1 hora** | **Index Scan** sobre B-Tree (`time_ms DESC`) + `HashAggregate` | Escaneo completo de tabla con ordenación externa en disco (`Sort`) | **$O(K \log K)$** ($K$ eventos en la ventana) | **$O(N \log N)$** ($N$ total histórico) | Al existir un índice sobre `time_ms`, el motor extrae únicamente las tuplas de la ventana solicitada en tiempo proporcional al resultado y no al tamaño histórico acumulado. |

---

## 2. Mediciones Empíricas de Rendimiento (Benchmark)

Se ejecutó el banco de pruebas empírico [`scripts/benchmark_rendimiento.py`](file:///c:/Users/Lucas/Desktop/Actividad_Sismica/scripts/benchmark_rendimiento.py) sobre el entorno del contenedor, evaluando tres tamaños de entrada con datos sintéticos generados en memoria (sin alterar las tablas analíticas reales):

### Prueba 1: Búsqueda de Versiones (Lista Secuencial vs Tabla Hash)
*Procedimiento:* Se cargaron $N$ eventos y se ejecutaron 500 búsquedas aleatorias de identificadores.

| Tamaño Entrada ($N$) | Búsqueda Secuencial $O(N)$ | Búsqueda Hash Map $O(1)$ | Factor de Aceleración (*Speedup*) | Memoria Estructura Hash |
|:---:|:---:|:---:|:---:|:---:|
| **200 eventos** | 0,48 ms | 0,010 ms | **48,5×** | 6,4 KB |
| **1.000 eventos** | 5,84 ms | 0,029 ms | **195,8×** | 25,4 KB |
| **5.000 eventos** | 29,68 ms | 0,037 ms | **797,9×** | 101,4 KB |

*Conclusión empírica:* A medida que el volumen crece, el coste secuencial escala linealmente ($29,68\text{ ms}$ para 5.000 registros), mientras que la estructura Hash mantiene un tiempo de respuesta prácticamente plano ($< 0,04\text{ ms}$), logrando una aceleración de casi **800 veces** con un consumo de memoria insignificante (~101 KB).

---

### Prueba 2: Integración Geográfica Espacial (Punto en Polígono)
*Procedimiento:* Evaluación de $N$ coordenadas aleatorias frente a los 177 polígonos territoriales de Natural Earth.

| Tamaño Entrada ($N$) | Fuerza Bruta Secuencial $O(N \cdot M)$ | Índice Espacial STRtree $O(N \log M)$ | Factor de Aceleración (*Speedup*) |
|:---:|:---:|:---:|:---:|
| **200 puntos** | 114,25 ms | 1,43 ms | **80,0×** |
| **1.000 puntos** | 583,54 ms | 7,69 ms | **75,9×** |
| **5.000 puntos** | 2.358,11 ms | 35,76 ms | **65,9×** |

*Conclusión empírica:* La fuerza bruta requiere más de **2,3 segundos** para enriquecer un lote de 5.000 eventos, bloqueando el bucle de eventos del consumidor. Con el índice espacial R-Tree STRtree, el tiempo se reduce a **35 ms** (más de 65 veces más rápido), garantizando que el consumidor procese los lotes de Kafka holgadamente antes del siguiente sondeo.

---

### Prueba 3: Consulta en PostgreSQL con Índice B-Tree (`EXPLAIN ANALYZE`)
*Consulta evaluada:* `SELECT * FROM eventos_actuales WHERE time_ms >= $start ORDER BY time_ms DESC LIMIT 100;`
* **Plan de ejecución:** `Index Scan using idx_ev_act_time on eventos_actuales`
* **Tiempo de planificación:** 1,40 ms
* **Tiempo de ejecución real:** **0,155 ms**
* **Evaluación:** Gracias al índice B-Tree ordenado descendentemente, PostgreSQL no requiere realizar operaciones de ordenación en memoria (`Sort`) ni escanear tuplas irrelevantes, resolviendo la consulta en una fracción de milisegundo.

---

## 3. Razonamiento a Escala Extrema: ¿Qué ocurriría con 100 Millones de Registros?

Si el observatorio ampliara su alcance histórico mundial a décadas completas acumulando **$10^8$ registros sísmicos**:

1. **Cuello de Botella de Almacenamiento e I/O:**
   - A un tamaño medio de ~300 bytes por fila más índices (~150 bytes), la tabla principal ocuparía aproximadamente **45 GB**.
   - Un escaneo secuencial (`Seq Scan`) obligaría a leer decenas de gigabytes de disco, saturando el ancho de banda del canal I/O y congelando la base de datos por decenas de segundos por consulta.
2. **Degeneración de Índices B-Tree Tradicionales:**
   - Un índice B-Tree sobre 100 millones de claves requeriría ~3 a 4 niveles de profundidad y varios gigabytes de RAM. Si el *working set* supera la memoria caché de PostgreSQL (`shared_buffers`), cada consulta generaría lecturas físicas aleatorias en disco (*cache miss*).
3. **Estrategias Arquitectónicas de Solución:**
   - **Particionamiento Declarativo por Tiempo:** Dividir `eventos_actuales` en particiones mensuales (`PARTITION BY RANGE (time_ms)`). Esto permite aplicar *Partition Pruning*: una consulta de las últimas 24 horas solo lee la partición del mes en curso, ignorando el 99% de los datos históricos.
   - **Índices BRIN (Block Range Index):** Dado que los eventos sísmicos se insertan de forma casi estrictamente creciente en el tiempo, un índice BRIN sobre `time_ms` ocupa menos del 1 % del espacio de un B-Tree tradicional y ofrece rendimientos comparables para consultas por rangos temporales.
   - **Migración a Base de Datos Columnar / OLAP:** Para agregaciones masivas y análisis exploratorios sobre cientos de millones de eventos, desacoplar el almacenamiento operacional de PostgreSQL y volcar los eventos a **ClickHouse** o **DuckDB**, permitiendo agregaciones vectorizadas SIMD a velocidades de gigabytes por segundo.

---

## 4. Estimación Realista de Costes (TCO - Total Cost of Ownership)

> **Principio de evaluación:** *Una cuota gratuita de proveedor cloud no equivale a coste cero.* El coste total de propiedad contempla el consumo energético, la amortización de infraestructura física, el almacenamiento persistente y el tiempo de ingeniería.

### Desglose de Costes Estimados (Operación Anual de un Nodo Productivo)

| Concepto de Gasto | Supuestos y Dimensionamiento | Coste Mensual Estimado | Coste Anual Estimado |
|---|---|:---:|:---:|
| **Hardware y Amortización** | Servidor o instancia 4 vCPU, 16 GB RAM, 256 GB NVMe (amortización 3 años) | 35,00 € | 420,00 € |
| **Consumo Eléctrico** | Equipo con consumo continuo de ~85 W (24/7 = 61,2 kWh/mes a 0,18 €/kWh) | 11,00 € | 132,00 € |
| **Almacenamiento y Backup** | 50 GB almacenamiento caliente (SSD) + snapshots diarios en S3/B2 | 2,50 € | 30,00 € |
| **Transferencia y Red** | Feed USGS cada 60s (~15 MB/día = 450 MB/mes) + consultas de panel (~5 GB/mes) | 1,00 € | 12,00 € |
| **Licencias y Túneles** | ngrok plan Personal (o túnel Cloudflare Zero Trust autohospedado) | 8,00 € | 96,00 € |
| **Mantenimiento y DevOps** | 3 horas mensuales de supervisión, actualización de parches y auditoría (a 30 €/h) | 90,00 € | 1.080,00 € |
| **Inversión Inicial de Desarrollo** | 60 horas de ingeniería de software y Big Data (diseño, ETL, pruebas, frontend) | — | 1.800,00 € *(pago único)* |
| **TOTAL TCO (Año 1)** | *(Infraestructura + Mantenimiento + Desarrollo Inicial)* | **147,50 € / mes** | **3.570,00 €** |

---

### Propuesta de Escalado y Retorno de Inversión (ROI)

* **¿Cómo justificar el retorno (ROI) sin inventar beneficios económicos ficticios?**
  El observatorio no genera ingresos directos por venta de datos sísmicos (los datos de USGS son de dominio público). El retorno de inversión se justifica técnica y operativamente en:
  1. **Ahorro por automatización de observabilidad:** Sustituye el análisis manual de catálogos y boletines de emergencia por un panel de consolidación automática en tiempo real, ahorrando horas de trabajo a equipos de protección civil o investigación académica.
  2. **Continuidad operativa y cumplimiento normativo:** Permite disponer de una base de datos histórica propia, enriquecida geográficamente y desduplicada, protegiendo a la organización frente a caídas temporales de la API internacional de USGS o cortes en la red exterior.
* **Propuesta de Escalado Gradual:**
  - *Fase 1 (Actual - Local):* Docker Compose en equipo local, ngrok y Netlify. Ideal para validación, coste marginal nulo en cuota gratuita y desarrollo.
  - *Fase 2 (Producción Media):* Despliegue en VPS Linux (ej. Hetzner / OVH) con túnel Cloudflare Zero Trust (elimina los límites de ngrok) y volumen NVMe gestionado.
  - *Fase 3 (Alta Escala):* Separar el broker de Kafka en un clúster gestionado de 3 nodos (Strimzi / Confluent) y mover las agregaciones históricas a ClickHouse.
