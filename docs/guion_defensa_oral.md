# Guión de Defensa Oral Individual (Punto 12)

> Este guión está preparado para que puedas explicar con soltura y seguridad el proyecto ante el profesor en 2 o 3 minutos.

---

## 1. Explicación del Flujo Completo de Datos (60 segundos)

> *"El sistema que he construido es un observatorio distribuido en tiempo real que captura, limpia, almacena y visualiza actividad sísmica mundial mediante un pipeline desacoplado:*
> 
> 1. **Extracción (Productor Python):** Cada 60 segundos realiza una petición asíncrona por HTTP GET al feed de 24 horas del USGS. Filtra únicamente los terremotos (`type == 'earthquake'`), guarda el GeoJSON crudo en PostgreSQL para auditoría y compara la fecha de actualización (`updated`) con su estado en memoria. Solo publica novedades en Kafka usando el ID del evento como clave.
> 2. **Transporte (Apache Kafka):** Actúa como buffer desacoplador y tolerante a fallos. Retiene los mensajes hasta 72 horas con semántica de partición por clave.
> 3. **Transformación y Calidad (Consumidor Python):** Lee los mensajes con confirmación manual de offsets (`auto_commit=False`). Valida la integridad de coordenadas en WGS84 (enviando a una tabla de cuarentena cualquier punto fuera de rango). Realiza un cruce espacial de punto en polígono con el catálogo de Natural Earth mediante un índice espacial STRtree (R-Tree) para asignar país de forma instantánea.
> 4. **Almacenamiento (PostgreSQL):** Aplica un upsert idempotente en una transacción ACID: si es un evento nuevo lo inserta; si es una revisión posterior lo actualiza y audita en un histórico de versiones; si es un mensaje duplicado o retrasado, lo ignora. **Solo tras hacer COMMIT en la base de datos, el consumidor confirma el offset en Kafka.**
> 5. **Consulta y Visualización (FastAPI + ngrok + Angular en Netlify):** FastAPI expone los datos procesados en `/api/eventos` y `/api/estadisticas`. ngrok proporciona el túnel seguro con terminación HTTPS y el panel Angular en Netlify consulta la API cada 60 segundos, mostrando el mapa de calor, gráficos de distribución y comparación histórica frente a la media de 2025.*"

---

## 2. Explicación de una Decisión Técnica Propia Justificada (60 segundos)

Elige **UNA** de estas dos decisiones técnicas para defenderla:

### Opción A (Recomendada): Semántica At-Least-Once e Idempotencia Transaccional
> *"Una decisión técnica crítica que tomé fue **desactivar el auto-commit de Kafka y confirmar los offsets únicamente después de persistir en PostgreSQL**.  
> En arquitecturas de Big Data, coordinar Kafka y una base de datos relacional sin usar transacciones distribuidas XA de dos fases (que penalizan drásticamente el rendimiento) genera el riesgo de duplicar datos si un contenedor cae.  
> Mi solución implementa **procesamiento al menos una vez con escritura idempotente**:  
> - Si el consumidor cae antes de persistir, la base de datos hace rollback automático y Kafka vuelve a entregar el mensaje al arrancar.  
> - Si cae después de persistir pero antes de confirmar en Kafka, el mensaje se reenvía, pero el consumidor comprueba la versión `updated_ms <= actual`: la transacción detecta que ya fue procesado y no incrementa el recuento ni corrompe el histórico.  
> Además, si por latencia de red una revisión antigua llega después de una nueva, la regla `updated_ms > actual` protege el estado vigente impidiendo regresiones.*"

### Opción B: Integración Geográfica con STRtree en memoria vs Fuerza Bruta
> *"Al integrar cada terremoto con el catálogo de Natural Earth de 177 países, una búsqueda ingenua recorriendo cada polígono tiene un coste de $O(N \cdot M)$. En un lote de 5.000 eventos, esto tardaba más de 2,3 segundos en CPU y bloqueaba el bucle de eventos.  
> Decidí cargar el GeoJSON una sola vez al arrancar el contenedor e indexarlo con un **árbol espacial STRtree (R-Tree)** en Shapely. El árbol descarta en $O(\log M)$ el 99% de los países evaluando solo los rectángulos envolventes antes de ejecutar el algoritmo exacto de punto en polígono.  
> Medí empíricamente el resultado: el tiempo bajó de 2.358 ms a 35 ms, logrando una **aceleración de 66 veces** y permitiendo que el consumidor procese los lotes de Kafka prácticamente en tiempo real.*"
