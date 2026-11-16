# Guión y Preparación de la Defensa Oral en Clase

> **¡IMPORTANTE PARA CLASE!**  
> Si usas el portátil de clase desde cero, sigue primero la **Sección 0 (Preparación desde cero en 5 minutos)** antes de que empiece la evaluación. Luego usa la **Sección 1 y 2** durante la defensa oral ante el profesor.

---

## 🛠️ SECCIÓN 0: Puesta en marcha desde cero en el portátil de clase (5 minutos)

### Paso 1: Instalar requisitos previos (si el portátil no los tiene)
1. **Docker Desktop:** Descárgalo e instálalo desde [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop).
   - Abre Docker Desktop y asegúrate de que el icono en la barra de tareas esté en verde (*"Engine running"*).
2. **Git:** Descárgalo desde [git-scm.com](https://git-scm.com/) (o usa GitHub Desktop / terminal).
3. **ngrok:** Descárgalo desde [ngrok.com/download](https://ngrok.com/download) (o ejecuta `winget install ngrok` en PowerShell).

---

### Paso 2: Clonar el repositorio y levantar el backend
Abre la terminal de PowerShell en tu portátil y escribe:

```bash
# 1. Clonar tu repositorio de GitHub
git clone https://github.com/TU_USUARIO/TU_REPOSITORIO.git
cd TU_REPOSITORIO

# 2. Crear el archivo de entorno a partir de la plantilla
copy .env.example .env

# 3. Levantar los 5 contenedores Docker (construye la imagen Python y arranca todo)
docker compose up --build -d

# 4. Comprobar que los 5 contenedores estén en verde (healthy)
docker compose ps
```

---

### Paso 3: Abrir el túnel ngrok y conectar Netlify
Como Netlify ya está desplegado en internet (`https://actividad-sismica.netlify.app`), solo necesitas conectar tu nuevo portátil a internet con ngrok:

```bash
# 1. Iniciar ngrok en el puerto 8000
ngrok http 8000
```

1. ngrok te mostrará una URL pública tipo: `https://xxxx-xx-xx.ngrok-free.app`.
2. **Copia esa URL de ngrok**.
3. Ve al archivo `recursos/observatorio-sismico/frontend/public/config.json`:
   ```json
   {
     "mode": "api",
     "apiUrl": "https://xxxx-xx-xx.ngrok-free.app",
     "refreshMs": 60000,
     "skipNgrokWarning": true
   }
   ```
4. Guarda y sube ese pequeño cambio a GitHub:
   ```bash
   git add recursos/observatorio-sismico/frontend/public/config.json
   git commit -m "chore: actualizar url ngrok para defensa en clase"
   git push origin main
   ```
   *(Netlify se actualizará solo en 30 segundos con la nueva URL).*

5. Abre en el navegador **https://actividad-sismica.netlify.app** y verás el panel conectado con el indicador LED en verde.

---

### Paso 4: (Opcional) Si quieres ver el panel en local sin ngrok
Si por cualquier motivo no quieres usar ngrok en ese momento, puedes abrir el frontend en local directamente:
```bash
cd recursos/observatorio-sismico/frontend
npm install
npm start
```
Y abrir `http://localhost:4200` en tu navegador.

---

## 🎤 SECCIÓN 1: Explicación del Flujo Completo (60 segundos)

Cuando el profesor se acerque a tu mesa, le dices con seguridad:

> *"El sistema que he construido es un observatorio distribuido en tiempo real que captura, limpia, enriquece, almacena y visualiza actividad sísmica mundial mediante un pipeline desacoplado en 5 servicios Docker:*
> 
> 1. **Extracción (Productor Python):** Cada 60 segundos realiza una petición asíncrona HTTP GET al feed de 24 horas del USGS. Filtra únicamente terremotos (`type == 'earthquake'`), guarda el GeoJSON crudo en PostgreSQL para auditoría y compara la fecha de actualización (`updated`) con su estado en memoria para publicar únicamente novedades en Kafka usando el ID del evento como clave de partición.
> 2. **Transporte (Apache Kafka):** Actúa como buffer desacoplador y tolerante a fallos en modo KRaft (sin ZooKeeper). Retiene los mensajes hasta 72 horas.
> 3. **Transformación y Calidad (Consumidor Python):** Lee los mensajes con confirmación manual de offsets (`auto_commit=False`). Valida la integridad de coordenadas en WGS84 (enviando a una tabla de cuarentena cualquier punto fuera de rango). Realiza un cruce espacial de punto en polígono con el catálogo de Natural Earth mediante un índice espacial STRtree (R-Tree) en memoria para asignar país de forma instantánea.
> 4. **Almacenamiento (PostgreSQL):** Aplica un upsert transaccional: si es un evento nuevo lo inserta; si es una revisión posterior lo actualiza y audita en el histórico de versiones; si es un mensaje duplicado o retrasado, lo ignora. **Solo tras hacer COMMIT en la base de datos, el consumidor confirma el offset en Kafka.**
> 5. **Consulta y Visualización (FastAPI + ngrok + Angular en Netlify):** FastAPI expone los datos procesados en `/api/eventos` y `/api/estadisticas`. ngrok proporciona el túnel seguro con terminación HTTPS y el panel Angular en Netlify consulta la API cada 60 segundos, mostrando el mapa, gráficos de distribución y comparación histórica frente a la media de 2025.*"

---

## 🧠 SECCIÓN 2: Explicación de una Decisión Técnica Propia Justificada (60 segundos)

Cuando te pregunte por una decisión técnica propia, defiendes esta:

### Decisión Técnica: Semántica At-Least-Once e Idempotencia Transaccional
> *"Una decisión técnica crítica que tomé fue **desactivar el auto-commit automático de Kafka y confirmar los offsets únicamente después de persistir con éxito en PostgreSQL**.  
> En arquitecturas de Big Data, coordinar Kafka y una base de datos relacional sin usar transacciones distribuidas XA de dos fases (que penalizan drásticamente el rendimiento) genera el riesgo de duplicar datos si un contenedor cae o se reinicia.  
> Mi solución implementa **procesamiento al menos una vez con escritura idempotente**:  
> - Si el consumidor cae antes de persistir, la base de datos hace rollback automático y Kafka vuelve a entregar el mensaje al arrancar.  
> - Si cae después de persistir pero antes de confirmar en Kafka, el mensaje se reenvía, pero el consumidor comprueba la versión `updated_ms <= actual`: la transacción detecta que ya fue procesado y no incrementa el recuento ni corrompe el histórico.  
> Además, si por latencia de red una revisión antigua llega después de una nueva, la regla `updated_ms > actual` protege el estado vigente impidiendo regresiones.*"

---

## ⚡ SECCIÓN 3: Demostración de Actividad en Vivo (Si el profesor pide ver actividad)

Si el profesor te dice: *"Quiero ver cómo reacciona el sistema cuando entran eventos nuevos o revisiones"*, ejecutas en tu terminal:

```bash
docker exec actividad_sismica-productor-1 python -m src.productor_replay
```

Le explicas:
> *"He programado un productor de reproducción histórica acelerada que inyecta por Kafka un lote controlado de eventos históricos aislados en el topic `terremotos.replay` bajo la etiqueta `[REPRODUCCIÓN]`.  
> Demuestra en vivo:
> - La inserción de un evento nuevo.
> - La deduplicación automática de un mensaje repetido.
> - La actualización de magnitud de 5.1 a 5.4 tras una revisión.
> - El rechazo de una versión antigua llegada tarde.
> - El aislamiento en cuarentena de un sensor con coordenadas erróneas (latitud 125 > 90)."*
