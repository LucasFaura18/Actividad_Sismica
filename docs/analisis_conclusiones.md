# Análisis de Actividad Sísmica y Conclusiones (Punto 8)

> **Observatorio de Actividad Sísmica**  
> **Datos analizados:** Ventana de 24 horas UTC (15–16 de noviembre de 2026) frente a la línea base anual de 2025.  
> **Fuentes:** USGS Earthquake API (feed `all_day.geojson` y catálogo FDSNWS) + Natural Earth (WGS84).

---

## 1. Conclusiones Fundamentadas

### Conclusión 1: Asimetría en la detección y sesgo de cobertura instrumental
* **Datos observados:** De los 199 eventos registrados en la ventana de 24 horas, **168 eventos (84,4 %)** se localizaron en territorio de Estados Unidos (principalmente California, Alaska y Hawái), con una magnitud media de **1,54**. En contraste, regiones sismogénicas globales de alta energía (como el Cinturón de Fuego en Sudamérica o Asia Central) registraron recuentos individuales reducidos (México: 3, Bolivia: 1, Afganistán: 1, Perú: 1) pero con magnitudes medias notablemente superiores (**2,58 a 4,9**).
* **Interpretación:** Este contraste no evidencia una anomalía tectónica en Norteamérica, sino un **sesgo de completitud instrumental**. La densa red de sismógrafos del USGS y de redes regionales (ANSS) permite catalogar microsismos con magnitudes inferiores a 1,5 (e incluso negativas), mientras que en áreas remotas o internacionales la red sismológica global solo garantiza detección completa para magnitudes $M \ge 4,0$ o $M \ge 4,5$.

---

### Conclusión 2: Predominio absoluto de sismicidad superficial y eventos marítimos
* **Datos observados:** La distribución por profundidad revela que **182 de los 199 sismos (91,5 %)** se clasifican como *superficiales* (hipocentro $< 70\text{ km}$), con una profundidad media global de **24,33 km**. Únicamente 15 eventos (7,5 %) fueron de profundidad intermedia ($70\text{–}299\text{ km}$) y solo 2 eventos (1,0 %) fueron profundos ($\ge 300\text{ km}$). Asimismo, **22 eventos (11,1 %)** se localizaron en zonas oceánicas o dorsales marinas sin atribución nacional directa (`country_id = NULL`), presentando una magnitud media de **3,49** (significativamente mayor a la media terrestre).
* **Interpretación:** La concentración de hipocentros superficiales coincide con la fracturación de la corteza superior en fallas transformantes y zonas de intraplaca. Los eventos marítimos reflejan la actividad en dorsales oceánicas y arcos insulares; dado que la distancia a estaciones terrestres es mayor, únicamente los eventos con energía suficiente ($M > 3,0$) son detectados y triangulados con precisión.

---

### Conclusión 3: Variabilidad diaria y retrasos de consolidación frente al histórico 2025
* **Datos observados:** Al filtrar bajo condiciones equivalentes de energía (**Magnitud $M \ge 4,0$**):
  - **Últimas 24 horas registradas:** **14 terremotos** con $M \ge 4,0$.
  - **Línea base histórica (Año 2025):** Media diaria de **38,35 terremotos/día** (basada en ~14.000 eventos anuales distribuidos uniformemente sobre 365 días, incluyendo días con cero eventos).
  - **Variación relativa:** $-63,5\text{ \%}$ respecto a la media histórica anual.
* **Interpretación:** Esta diferencia a la baja en la ventana más reciente responde principalmente a dos factores:
  1. *Variabilidad estocástica a corto plazo:* La actividad sísmica presenta agrupamiento temporal (*clustering*) mediante secuencias de réplicas y periodos de relativa calma.
  2. *Latencia de consolidación USGS:* Las últimas horas de la ventana móvil contienen eventos que aún están en proceso de revisión manual o cálculo de magnitudes definitivas por analistas sismológicos, por lo que el recuento de las últimas 24 horas debe considerarse preliminar.

---

## 2. Advertencia Metodológica y Limitaciones
1. **No predictibilidad:** La sismología actual no permite predecir fecha, hora ni lugar exacto de futuros terremotos. Las métricas aquí expuestas reflejan registros históricos e instrumentales, no probabilidad de recurrencia inminente.
2. **Homogeneidad de magnitudes:** El feed integra diversos tipos de cálculo de magnitud (`ml` local de Richter, `mb` de ondas de cuerpo, `mww` de momento tensor). No representan directamente energía idéntica.
3. **Fronteras cartográficas:** La asignación territorial utiliza la simplificación 1:110m de Natural Earth; en litorales recortados o pequeñas islas, eventos terrestres cercanos a la costa pueden clasificarse como marítimos (`country_id = NULL`).
