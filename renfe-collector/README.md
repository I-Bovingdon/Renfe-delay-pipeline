# Colector GTFS-RT — RENFE Cercanías (TFM)

Captura 24/7 los tres feeds en tiempo real de RENFE Cercanías para construir
el dataset histórico del TFM (predicción de retrasos en Cercanías Madrid).

| Feed | URL | Qué aporta |
|---|---|---|
| `trip_updates` | https://gtfsrt.renfe.com/trip_updates.json | **Retraso por tren y parada → variable objetivo** |
| `vehicle_positions` | https://gtfsrt.renfe.com/vehicle_positions.json | Posición GPS, estado, andén → features |
| `alerts` | https://gtfsrt.renfe.com/alerts.json | Incidencias en texto libre → NLP |

Los feeds se actualizan cada ~20-30 s. El colector sondea cada 60 s por
defecto, deduplica por el timestamp de cabecera GTFS-RT y guarda el JSON
crudo comprimido, particionado por feed y día:

```
data/
├── raw/                                       <- archivo histórico inmutable
│   ├── trip_updates/2026-06-13/trip_updates_20260613T070100Z_ft....json.gz
│   ├── vehicle_positions/2026-06-13/...
│   └── alerts/2026-06-13/...
├── processed/                                 <- capa de trabajo (Parquet diario)
│   ├── trip_updates/trip_updates_2026-06-13.parquet
│   ├── vehicle_positions/vehicle_positions_2026-06-13.parquet
│   └── alerts/alerts_2026-06-13.parquet
├── gtfs_static/gtfs_cercanias_20260613.zip   (horarios teóricos, versionados)
└── logs/collector.log
```

Arquitectura por capas: el colector escribe SOLO en `raw/`; cada noche
`compact_day.py` aplana el día anterior a Parquet en `processed/` (una fila
por tren-parada-snapshot en trip_updates). El análisis (notebooks, Colab) se
hace siempre contra `processed/`, nunca contra el crudo.

> Licencia de los datos: CC BY 4.0 según el portal data.renfe.com.
> Citad la fuente en la memoria.

---

## 1. Infraestructura

Desplegado en un VPS Hetzner CX23 (2 vCPU, 4 GB de RAM, 40 GB de disco, Ubuntu 24.04).
Se descartaron Oracle Cloud y Google Cloud porque exigían tarjeta de crédito, y GitHub
Actions con cron porque su planificación no está garantizada. El motivo completo está en
el [README principal](../README.md).

Estos datos no se pueden volver a descargar: la copia diaria a Google Drive es
obligatoria, no opcional.

## 2. Instalación

```bash
sudo apt update && sudo apt install -y python3 python3-pip rsync
pip3 install -r requirements.txt   # requests, pandas, pyarrow

# Editad el USER_AGENT en scripts/collector.py y download_gtfs_static.py
# para poner un email de contacto del grupo (buena práctica al consumir APIs).

# Prueba manual (una sola pasada):
python3 scripts/collector.py --once --data-dir data
python3 scripts/inspect_day.py --data-dir data
```

Si `inspect_day.py` muestra entidades y delays en `trip_updates`, funciona.

## 3. Dejarlo corriendo 24/7 (systemd, recomendado)

```bash
# Ajustad User/rutas en deploy/renfe-collector.service y luego:
sudo cp deploy/renfe-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now renfe-collector

# Ver estado y logs:
systemctl status renfe-collector
tail -f data/logs/collector.log
```

`Restart=always` reinicia el proceso si muere; `enable` lo arranca solo
tras un reinicio de la máquina.

**Alternativa con cron** (menos robusta, pierde resolución):
```cron
* * * * * cd /home/tfm/renfe-collector && /usr/bin/python3 scripts/collector.py --once >> data/logs/cron.log 2>&1
```

## 4. Tareas programadas (cron)

La cadena nocturna real, con sus horas y su orden, está en el
[README principal](../README.md#cadena-nocturna). Las dos tareas de este colector:

```cron
# Usuario tfm. Compacta el día ANTERIOR completo, nunca el día en curso.
30 3 * * * /home/tfm/tfm-cercanias-colectores/renfe-collector/.venv/bin/python /home/tfm/tfm-cercanias-colectores/renfe-collector/scripts/compact_day.py --data-dir /home/tfm/data-renfe >> /home/tfm/logs/compact_renfe.log 2>&1
# GTFS estático del día, antes de que la aplicación regenere su catálogo.
45 3 * * * /usr/bin/python3 /home/tfm/tfm-cercanias-colectores/renfe-collector/scripts/download_gtfs_static.py --data-dir /home/tfm/data-renfe >> /home/tfm/logs/gtfs_download.log 2>&1
```

**Copia a Google Drive con `rclone copy`, nunca con `rclone sync`.** `sync` borra en el
destino lo que no esté en el origen: un fallo o una limpieza en el servidor se
propagaría a la copia de seguridad. `copy` solo añade. El colector nunca escribe
directamente en Drive: escribe en disco local y la copia se hace después.

### Trabajar con los parquets (Colab / local)

```python
import pandas as pd
from pathlib import Path

# Cargar un rango de días de trip_updates
files = sorted(Path("processed/trip_updates").glob("trip_updates_2026-06-*.parquet"))
df = pd.concat(pd.read_parquet(f) for f in files)

# Filtrar Madrid: cruzad route_id/stop_id con el GTFS estático (routes.txt
# y stops.txt indican el núcleo). Recordad: filtrar es procesado, no captura.
```

## 5. Volumetría y elección del intervalo

**¿Por qué 60 s y no 10 min si los trenes pasan cada 10-30 min?** Porque lo
que cambia cada pocos segundos no es "qué trenes hay" sino el ESTADO de
retraso de cientos de trenes con paradas separadas 2-4 minutos. Muestrear
fino permite: (1) capturar la dinámica de propagación del retraso entre
paradas, (2) reconstruir "lo que se sabía en el instante t" para entrenar
sin fugas de información, y (3) hacer downsampling después si sobra detalle
— lo contrario (recuperar lo no capturado) es imposible. La deduplicación
ya evita guardar pasadas sin cambios. Si la volumetría aprieta: subir a
120 s; nunca por encima de 5 min.

Con intervalo de 60 s (~1.440 capturas/día/feed, menos las deduplicadas):

- `trip_updates` es el feed pesado (todos los núcleos de Cercanías de España,
  con todas las paradas futuras por tren). Orden de magnitud esperado:
  **100-500 MB/día comprimido** entre los tres feeds en `raw/`.
- La capa `processed/` (Parquet zstd, columnar) ocupará bastante menos.
- 12 semanas ≈ **10-40 GB** de crudo. De ahí los 40-60 GB recomendados.

Medidlo el primer día con `du -sh data/raw/ data/processed/` y recalculad.

**Capturamos TODA España a propósito**: filtrar Madrid es decisión de
procesado, no de captura. Más núcleos = posibilidad de validar el modelo en
otra ciudad (capítulo extra de la memoria gratis).

## 6. Errores conocidos / FAQ

- **404 en alguna URL de feed**: RENFE puede mover los recursos. Entrad en
  https://data.renfe.com, buscad los datasets "Horarios de viaje",
  "Ubicación de los vehículos" e "Incidencias y avisos", copiad las nuevas
  URLs y actualizad el dict `FEEDS` en `collector.py`.
- **Huecos nocturnos**: Cercanías apenas circula de ~00:30 a ~05:00. Es
  normal que `trip_updates` traiga pocas entidades de madrugada. No es un bug.
- **El feed devuelve el mismo timestamp muchas veces**: el colector lo cuenta
  como "sin cambios" (columna `dup` del resumen horario). Normal de madrugada.
- **Cambio de hora / zonas horarias**: todo se guarda en UTC. Convertid a
  Europe/Madrid SOLO en la fase de análisis.

## 7. Para la memoria del TFM

Este componente ya os da contenido para el capítulo de ingeniería de datos:
arquitectura raw-first, escritura atómica, deduplicación por timestamp,
particionado por fecha, compactación diaria a Parquet (capa processed), sincronización a Drive con rclone, monitorización (logs + chequeo diario), versionado
del GTFS estático y política de backups. Documentadlo con un diagrama.
