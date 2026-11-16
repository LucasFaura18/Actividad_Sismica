# Guía de Despliegue y Funcionamiento (Punto 9)

Este documento detalla la arquitectura de despliegue, el túnel HTTPS mediante **ngrok**, la publicación de la SPA en **Netlify** y el protocolo de defensa.

---

## 1. Arquitectura de Red y Puertos

```
[ Visitante en Internet ] 
          │  (HTTPS público con certificado gestionado por Netlify)
          ▼
   [ Netlify SPA ] (Angular 20 + Leaflet)
          │  (HTTPS público con certificado gestionado por ngrok)
          ▼
   [ Túnel ngrok ] (Agente local en host)
          │  (HTTP plano en loopback 127.0.0.1:8000)
          ▼
┌── Docker Compose (Red interna bridge) ───────────────────────────────────┐
│                                                                          │
│  api (FastAPI en 0.0.0.0:8000) ───► postgres (16.10-alpine en 5432)      │
│     ▲                                      ▲                             │
│     │                                      │                             │
│  consumidor (Python) ───────────────┬──────┘                             │
│     ▲                               │                                    │
│     │ (terremotos.live)             │ (guarda estado y raw)              │
│  kafka (KRaft 4.1.0 en 19092)       │                                    │
│     ▲                               │                                    │
│     │                               │                                    │
│  productor (Python) ────────────────┘                                    │
│     │                                                                    │
└─────┼────────────────────────────────────────────────────────────────────┘
      │ (HTTP GET cada 60s)
      ▼
[ USGS API Feed ]
```

### Principios de Seguridad Aplicados
1. **Exposición mínima:** Ni PostgreSQL (`5432`) ni Kafka (`9092`) se publican fuera del equipo. En `compose.yaml` están vinculados estrictamente a `127.0.0.1` solo para inspección y depuración local del alumno.
2. **Terminación TLS en ngrok:** FastAPI atiende HTTP plano en `0.0.0.0:8000` dentro del contenedor. ngrok recibe la conexión segura HTTPS desde el exterior, gestiona el certificado digital TLS válido y entrega tráfico HTTP a la API local. **No se instalan certificados dentro de FastAPI.**
3. **CORS:** El middleware de FastAPI restringe el acceso al origen exacto de la aplicación en Netlify (`https://<tu-sitio>.netlify.app`) y a `http://localhost:4200` durante el desarrollo local, aceptando métodos `GET` y `OPTIONS`, y la cabecera `ngrok-skip-browser-warning`.

---

## 2. Procedimiento de Puesta en Marcha Local

### Paso 1: Configurar variables de entorno
```bash
# Copia la plantilla si no tienes .env creado
copy .env.example .env
```
*(Modifica `POSTGRES_PASSWORD` con una contraseña local segura).*

### Paso 2: Levantar los 5 servicios con Docker Compose
```bash
docker compose up --build -d
```

### Paso 3: Verificar que todos los servicios estén sanos
```bash
docker compose ps
```
Los 5 contenedores (`api`, `consumidor`, `productor`, `postgres`, `kafka`) deben mostrar el estado `Up (healthy)`.

---

## 3. Activación del Túnel HTTPS con ngrok

1. Instala o descarga ngrok en tu equipo (plan gratuito suficiente).
2. Autentica tu agente (solo una vez en tu terminal, nunca en Git):
   ```bash
   ngrok config add-authtoken TU_AUTHTOKEN_SECRETO
   ```
3. Inicia el túnel hacia el puerto de la API:
   ```bash
   ngrok http 8000
   ```
4. ngrok mostrará una dirección pública HTTPS similar a:
   ```
   Forwarding   https://a1b2-c3d4-e5f6.ngrok-free.app -> http://localhost:8000
   ```
   **Copia esa URL HTTPS.**

---

## 4. Despliegue del Frontend en Netlify

### Paso 1: Configurar la URL de la API
Edita el archivo `recursos/observatorio-sismico/frontend/public/config.json`:
```json
{
  "mode": "api",
  "apiUrl": "https://a1b2-c3d4-e5f6.ngrok-free.app",
  "refreshMs": 60000,
  "skipNgrokWarning": true
}
```

### Paso 2: Compilar el proyecto Angular
```bash
cd recursos/observatorio-sismico/frontend
npm run build
```
La compilación generará los archivos estáticos listos para producción en:
`recursos/observatorio-sismico/frontend/dist/observatorio/browser`

### Paso 3: Publicar en Netlify
- Entra en [Netlify Drop](https://app.netlify.com/drop) o vincula tu repositorio.
- Arrastra la carpeta generada: `dist/observatorio/browser`.
- Netlify te asignará una URL pública (ejemplo: `https://mi-observatorio-sismico.netlify.app`).

### Paso 4: Autorizar el dominio en CORS
Añade el nuevo dominio de Netlify a tu `.env`:
```env
CORS_ORIGINS=http://localhost:4200,https://mi-observatorio-sismico.netlify.app
```
Reinicia la API para aplicar los orígenes:
```bash
docker compose up -d api
```

---

## 5. Protocolo de Defensa ante el Profesor

Durante la presentación del proyecto:
1. **Arrancar:** Ejecuta `docker compose up -d` en tu terminal y muestra `docker compose ps` con los 5 servicios en verde.
2. **Túnel:** Abre `ngrok http 8000` y muestra cómo la URL redirige el tráfico HTTPS hacia el contenedor local de FastAPI.
3. **Panel Web:** Abre la URL de Netlify en un navegador (o desde el teléfono móvil del profesor). Muestra:
   - Los terremotos representados en el mapa interactivo con sus fichas emergentes.
   - La tabla accesible y los filtros por magnitud y país.
   - El panel de estadísticas con la comparación con la media histórica de 2025.
4. **Demostración de resiliencia ante cortes:**
   - Detén el túnel de ngrok temporalmente (`Ctrl + C` en ngrok).
   - Observa cómo la aplicación en Netlify detecta el fallo, enciende la alerta de desconexión y **conserva intacta la última lectura en pantalla sin romperse**.
   - Vuelve a iniciar ngrok y observa cómo el panel recupera la sincronización automática en el siguiente ciclo.
