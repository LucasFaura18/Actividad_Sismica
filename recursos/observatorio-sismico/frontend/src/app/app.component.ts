import { Component, OnDestroy, signal, computed } from '@angular/core';
import { LeafletDirective } from '@bluehalo/ngx-leaflet';
import { CommonModule } from '@angular/common';
import * as L from 'leaflet';
import type { FeatureCollection, Point } from 'geojson';

type Properties = {
  mag: number | null;
  place: string;
  time: number;
  updated: number;
  country_id?: string | null;
  country_name?: string | null;
};

type EventFeature = {
  type: 'Feature';
  id: string;
  geometry: { type: 'Point'; coordinates: [number, number, number?] };
  properties: Properties;
};

type EventsResponse = FeatureCollection<Point, Properties> & {
  metadata?: {
    generated?: number;
    last_ingestion?: number | null;
    mode?: string;
    count?: number;
    total_count?: number;
    truncated?: boolean;
  };
};

type EstadisticasResponse = {
  periodo: { start_ms: number; end_ms: number; duracion_horas: number };
  resumen: {
    total_eventos: number;
    mag_media: number | null;
    mag_maxima: number | null;
    depth_media_km: number | null;
    sin_pais_total: number;
    mag_nulas: number;
    depth_nulas: number;
  };
  por_pais: Array<{ pais: string; country_id: string; total: number; mag_promedio: number | null }>;
  por_hora_utc: Array<{ hora_utc: string; total: number; mag_promedio: number | null }>;
  distribucion_magnitud: {
    menor_2: number;
    de_2_a_3_9: number;
    de_4_a_4_9: number;
    de_5_a_5_9: number;
    mayor_o_igual_6: number;
    no_disponible: number;
  };
  distribucion_profundidad: {
    superficial_menos_70km: number;
    intermedia_70_300km: number;
    profunda_mas_300km: number;
    no_disponible: number;
  };
  comparacion_historica_m4: {
    ultimas_24h_con_mag_gte_4: number;
    media_diaria_historica_2025: number;
    diferencia_pct: number;
    nota: string;
  };
};

type Config = { mode: 'demo' | 'api'; apiUrl: string; refreshMs: number; skipNgrokWarning: boolean };

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [LeafletDirective, CommonModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnDestroy {
  readonly mode = signal('Cargando configuración');
  readonly status = signal('Preparando el mapa…');
  readonly isConnected = signal(false);
  readonly lastRead = signal('Sin lectura');
  readonly generated = signal('Sin información');
  readonly lastIngestion = signal('Desconocida');
  readonly count = signal(0);
  readonly totalDbCount = signal(0);
  readonly omitted = signal(0);
  readonly isTruncated = signal(false);
  readonly countriesStatus = signal('Cargando catálogo');

  // Filtros
  readonly filtroHoras = signal<number>(24);
  readonly filtroMinMag = signal<number | null>(null);
  readonly filtroPais = signal<string>('');
  readonly busquedaTabla = signal<string>('');
  readonly mostrarTabla = signal<boolean>(false);

  // Lista de eventos y estadísticas
  readonly listaEventos = signal<EventFeature[]>([]);
  readonly stats = signal<EstadisticasResponse | null>(null);
  readonly listaPaisesDisponibles = signal<Array<{ id: string; nombre: string }>>([]);

  // Eventos filtrados para la tabla accesible
  readonly eventosTablaFiltrados = computed(() => {
    const q = this.busquedaTabla().toLowerCase().trim();
    const evs = this.listaEventos();
    if (!q) return evs.slice(0, 100);
    return evs.filter(e => {
      const p = e.properties;
      return (
        (p.place && p.place.toLowerCase().includes(q)) ||
        (p.country_name && p.country_name.toLowerCase().includes(q)) ||
        (e.id && e.id.toLowerCase().includes(q))
      );
    }).slice(0, 100);
  });

  readonly options: L.MapOptions = {
    center: L.latLng(18, 0),
    zoom: 2,
    minZoom: 2,
    maxZoom: 8,
    preferCanvas: true,
    worldCopyJump: false
  };

  private map?: L.Map;
  private eventsLayer = L.layerGroup();
  private timer?: ReturnType<typeof setTimeout>;
  private controller = new AbortController();
  private destroyed = false;
  private currentConfig?: Config;

  async onMapReady(map: L.Map): Promise<void> {
    this.map = map;
    const base = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      noWrap: true,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);

    const borders = L.geoJSON(undefined, {
      style: { color: '#334155', weight: 1, fill: false },
      onEachFeature: (feature, layer) => {
        const name = document.createElement('span');
        name.textContent = feature.properties?.NAME_ES ?? feature.properties?.ADMIN ?? 'Sin nombre';
        layer.bindTooltip(name);
      }
    }).addTo(map);

    this.eventsLayer.addTo(map);
    L.control.layers(
      { 'OpenStreetMap': base },
      { 'Países (Natural Earth)': borders, 'Terremotos': this.eventsLayer }
    ).addTo(map);

    map.attributionControl.addAttribution('Fronteras: Natural Earth · Datos reales: USGS');

    try {
      const cat = await this.readJson<FeatureCollection>('datos/paises.geojson');
      borders.addData(cat);
      this.countriesStatus.set('Catálogo de países cargado (177 entidades)');

      // Extraer lista de países ordenada para el filtro
      const paises = cat.features
        .map(f => ({
          id: (f.properties?.['ADM0_A3'] as string) || '',
          nombre: (f.properties?.['NAME_ES'] as string) || (f.properties?.['ADMIN'] as string) || ''
        }))
        .filter(p => p.id && p.nombre)
        .sort((a, b) => a.nombre.localeCompare(b.nombre));
      this.listaPaisesDisponibles.set(paises);
    } catch {
      this.countriesStatus.set('No se pudo cargar el catálogo de países');
    }

    if (this.destroyed) return;

    try {
      this.currentConfig = await this.readJson<Config>('config.json');
      if (!['demo', 'api'].includes(this.currentConfig.mode)) throw new Error('Modo inválido');
      this.mode.set(this.currentConfig.mode === 'demo' ? 'DEMOSTRACIÓN · datos ficticios' : 'API · backend en vivo');
      await this.refresh();
    } catch {
      this.status.set('Error: revisa public/config.json');
    }
  }

  private async readJson<T>(url: string, headers: Record<string, string> = {}): Promise<T> {
    const response = await fetch(url, {
      headers,
      cache: 'no-store',
      signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(15000)])
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<T>;
  }

  async refresh(): Promise<void> {
    if (!this.currentConfig || this.destroyed) return;
    const cfg = this.currentConfig;

    try {
      const headers: Record<string, string> = {};
      if (cfg.mode === 'api' && cfg.skipNgrokWarning) {
        headers['ngrok-skip-browser-warning'] = '1';
      }

      let urlEventos = 'datos/terremotos-demo.geojson';
      let urlStats = '';

      if (cfg.mode === 'api') {
        const base = cfg.apiUrl.replace(/\/$/, '');
        const queryParams = new URLSearchParams();

        const horas = this.filtroHoras();
        if (horas > 0) {
          const startMs = Date.now() - (horas * 3600 * 1000);
          queryParams.set('start', startMs.toString());
        }

        const minMag = this.filtroMinMag();
        if (minMag !== null && !Number.isNaN(minMag)) {
          queryParams.set('min_mag', minMag.toString());
        }

        const pais = this.filtroPais();
        if (pais) {
          queryParams.set('country_id', pais);
        }

        const qStr = queryParams.toString() ? `?${queryParams.toString()}` : '';
        urlEventos = `${base}/api/eventos${qStr}`;
        urlStats = `${base}/api/estadisticas${qStr}`;
      }

      // 1. Obtener eventos
      const data = await this.readJson<EventsResponse>(urlEventos, headers);
      if (this.destroyed) return;

      if (data.type !== 'FeatureCollection' || !Array.isArray(data.features)) {
        throw new Error('Se esperaba FeatureCollection');
      }

      // 2. Obtener estadísticas si estamos en modo API
      if (urlStats) {
        try {
          const statsData = await this.readJson<EstadisticasResponse>(urlStats, headers);
          this.stats.set(statsData);
        } catch (e) {
          console.warn('No se pudieron obtener estadísticas:', e);
        }
      }

      // 3. Renderizar capa Leaflet de forma atómica
      const nextLayer = L.layerGroup();
      let omitted = 0;
      let count = 0;
      const validFeatures: EventFeature[] = [];

      for (const feature of data.features as EventFeature[]) {
        const p = feature.properties;
        const coords = feature.geometry?.type === 'Point' ? feature.geometry.coordinates : [];
        const [lon, lat, depth] = coords;

        if (!p || !Number.isFinite(lon) || !Number.isFinite(lat) || Math.abs(lon) > 180 || Math.abs(lat) > 90) {
          omitted++;
          continue;
        }

        const magnitude = typeof p.mag === 'number' && Number.isFinite(p.mag) ? p.mag : null;
        const depthOk = typeof depth === 'number' && Number.isFinite(depth);
        const color = !depthOk ? '#64748b' : depth < 70 ? '#e8a317' : depth < 300 ? '#e35b37' : '#7b3fb6';

        const popup = document.createElement('div');
        popup.innerHTML = `
          <strong>${p.place ?? 'Sin descripción'}</strong><br>
          <b>Magnitud:</b> ${magnitude !== null ? magnitude.toFixed(1) : 'ND'}<br>
          <b>Profundidad:</b> ${depthOk ? depth + ' km' : 'ND'}<br>
          <b>Fecha UTC:</b> ${this.date(p.time)}<br>
          <b>País:</b> ${p.country_name ?? 'Sin país asignado (marítimo)'}<br>
          <small>ID: ${feature.id ?? 'ND'}</small>
        `;

        const radius = magnitude === null ? 4 : Math.max(3, Math.min(18, 3 + (magnitude + 1) * 2));
        L.circleMarker([lat, lon], {
          radius,
          color,
          fillColor: color,
          fillOpacity: 0.75,
          weight: 1
        }).bindPopup(popup).addTo(nextLayer);

        count++;
        validFeatures.push(feature);
      }

      this.eventsLayer.clearLayers();
      this.eventsLayer.addLayer(nextLayer);

      this.count.set(count);
      this.totalDbCount.set(data.metadata?.total_count ?? count);
      this.isTruncated.set(data.metadata?.truncated ?? false);
      this.omitted.set(omitted);
      this.listaEventos.set(validFeatures);

      this.lastRead.set(new Date().toISOString());
      this.generated.set(this.date(data.metadata?.generated));
      this.lastIngestion.set(this.date(data.metadata?.last_ingestion));
      this.isConnected.set(true);
      this.status.set(cfg.mode === 'demo' ? 'Ejemplo estático: datos de prueba' : 'API conectada · datos sincronizados');

    } catch (err) {
      if (!this.destroyed) {
        this.isConnected.set(false);
        this.status.set('Aviso: el backend no responde o el túnel está cerrado. Se conserva la última lectura.');
      }
    } finally {
      if (!this.destroyed && cfg.mode === 'api') {
        clearTimeout(this.timer);
        this.timer = setTimeout(() => void this.refresh(), Math.max(15000, Number(cfg.refreshMs) || 60000));
      }
    }
  }

  // Métodos de control de filtros
  onHorasChange(event: Event): void {
    const val = Number((event.target as HTMLSelectElement).value);
    this.filtroHoras.set(val);
    void this.refresh();
  }

  onMinMagChange(event: Event): void {
    const valStr = (event.target as HTMLSelectElement).value;
    const val = valStr === '' ? null : Number(valStr);
    this.filtroMinMag.set(val);
    void this.refresh();
  }

  onPaisChange(event: Event): void {
    const val = (event.target as HTMLSelectElement).value;
    this.filtroPais.set(val);
    void this.refresh();
  }

  onBusquedaTabla(event: Event): void {
    const q = (event.target as HTMLInputElement).value;
    this.busquedaTabla.set(q);
  }

  toggleTabla(): void {
    this.mostrarTabla.update(v => !v);
  }

  date(value: unknown): string {
    if (typeof value !== 'number' || !Number.isFinite(value)) return 'ND';
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? 'ND' : d.toISOString().replace('.000Z', 'Z');
  }

  ngOnDestroy(): void {
    this.destroyed = true;
    clearTimeout(this.timer);
    this.controller.abort();
  }
}
