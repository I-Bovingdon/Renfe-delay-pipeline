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

## 1. Qué necesitáis (infraestructura)

Una máquina Linux encendida 24/7 con Python 3.10+, ~1 GB de RAM y
**40-60 GB de disco libres** (estimación holgada para ~12 semanas; ver §5).
Opciones, por orden de recomendación:

1. **VPS gratuito**: Oracle Cloud "Always Free" (ARM, 4 GB RAM, 200 GB disco)
   o la e2-micro del free tier de Google Cloud. Ventaja: no depende de la
   luz/wifi de nadie y todo el grupo puede entrar por SSH.
2. **Raspberry Pi / PC viejo en casa de alguien del grupo**: perfecto si
   tenéis uno. Riesgo: cortes de luz/red en vacaciones → mitigad con systemd
   (`Restart=always`) y arranque automático tras corte.
3. **VPS de pago barato** (Hetzner ~4 €/mes): si el grupo prefiere pagar poco
   y dormir tranquilo, es la opción más simple.

**Evitad** GitHub Actions con cron para esto: el scheduling no está
garantizado y perderíais resolución y fiabilidad.

> Consejo: sea cual sea la máquina, configurad un **rsync/backup diario** a
> un Drive o a un segundo sitio. Estos datos no se pueden re-descargar:
> perderlos = perder semanas de TFM.

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

## 4. Tareas programadas adicionales (cron)

```cron
# Compactación diaria raw -> Parquet (compacta AYER), 03:30 UTC
30 3 * * * cd /home/tfm/renfe-collector && /usr/bin/python3 scripts/compact_day.py --data-dir data >> data/logs/compact.log 2>&1

# Sincronización diaria a Google Drive (backup + acceso del grupo/tutores), 04:00
# Requiere: instalar rclone (https://rclone.org/install/) y configurar el
# remote con `rclone config` (tipo "drive", nombre "gdrive").
0 4 * * * rclone sync /home/tfm/renfe-collector/data gdrive:TFM/renfe-data --transfers 4 >> /home/tfm/renfe-collector/data/logs/rclone.log 2>&1

# GTFS estático (horarios teóricos): 1 vez/semana, lunes 05:00
0 5 * * 1 cd /home/tfm/renfe-collector && /usr/bin/python3 scripts/download_gtfs_static.py >> data/logs/gtfs_static.log 2>&1

# Chequeo diario de sanidad: si ayer no hubo capturas, lo veréis en este log
30 7 * * * cd /home/tfm/renfe-collector && /usr/bin/python3 scripts/inspect_day.py --date $(date -u -d yesterday +\%Y-\%m-\%d) >> data/logs/daily_check.log 2>&1
```

**Orden importa**: compactar (03:30) antes de sincronizar (04:00), así el
parquet del día anterior llega a Drive cada mañana.

**Política con Google Drive**: el colector NUNCA escribe directamente en
Drive (montajes inestables, rate limits de la API, riesgo de capturas
perdidas). Escribe en disco local y rclone sincroniza después. Si la cuota
de Drive es limitada (15 GB en cuentas gratuitas), sincronizad solo la capa
de trabajo, que es ligera y es la que usaréis en los notebooks:

```cron
0 4 * * * rclone sync /home/tfm/renfe-collector/data/processed gdrive:TFM/renfe-data/processed >> /home/tfm/renfe-collector/data/logs/rclone.log 2>&1
```

y respaldad `raw/` a otro destino (disco externo, segundo VPS, bucket).

**Rutina humana mínima**: una persona del grupo mira `daily_check.log` y
`compact.log` cada mañana (2 minutos). Rotad por semanas.

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
