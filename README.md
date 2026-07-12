# TFM Cercanías — Pipeline de captura y compactación de datos en tiempo real (RENFE)

Pipeline de ingesta multifuente 24/7 para el TFM **"Predicción de retrasos en Cercanías de Madrid"** (Máster en Data Science, Big Data & Business Analytics — UCM). Construye desde cero el histórico de retrasos necesario para el modelado, capturando los feeds GTFS-Realtime de RENFE y datos meteorológicos de AEMET.

---

## El problema de fondo: no existe histórico público de retrasos

RENFE no archiva ni publica histórico de retrasos de Cercanías. El estado del servicio se expone únicamente mediante tres feeds **GTFS-Realtime efímeros** que se refrescan cada ~20-30 segundos y se sobreescriben sin guardar nada:

| Feed | Contenido | Rol en el proyecto |
|---|---|---|
| `trip_updates` | Retraso por tren y próxima parada | **Variable objetivo** |
| `vehicle_positions` | Posición GPS, estado del tren | Features de estado de red |
| `alerts` | Incidencias en texto libre | Entrada del componente NLP |

**Consecuencia directa:** cada día sin captura activa es información de entrenamiento perdida de forma irrecuperable. No hay forma de recuperar retroactivamente lo que no se capturó. Esto convierte la infraestructura de ingesta — y no el modelado — en la prioridad absoluta de la primera fase del proyecto.

---

## Decisiones de diseño y por qué se tomaron

### 1. VPS 24/7 en lugar de Databricks o servicios cloud gestionados

El colector pasa el 99% del tiempo esperando la respuesta del feed (I/O puro). Databricks y similares cobran por tiempo de cluster activo, lo que haría el coste completamente desproporcionado para una tarea que consume menos de 1 CPU y apenas memoria.

**Decisión:** VPS Hetzner CX23 (2 vCPU / 4GB RAM / 40GB SSD, ~4,83 €/mes), pagado con PayPal. Se evaluaron Google Cloud (e2-micro free tier) y Oracle Cloud (free tier ARM), ambos descartados por requerir tarjeta de crédito y por fricciones en el alta (Oracle bloqueó el registro por antifraude). Hetzner no exige tarjeta y no tiene fricciones.

Databricks sí tendrá sentido en la fase de modelado, donde el cómputo distribuido justifica el coste. No aquí.

### 2. Frecuencia de captura: 60 segundos

El feed GTFS-RT de RENFE **no publica la serie temporal del retraso de un tren**: solo da el estado de la próxima parada en el momento de la consulta. Esto significa que la evolución del retraso minuto a minuto no existe en el feed — hay que reconstruirla captura a captura.

**Decisión:** 60 segundos, validado empíricamente con el notebook de exploración. Con menos frecuencia se pierde resolución en la reconstrucción de la serie. Con más frecuencia el volumen crece sin aportar información nueva (el feed tarda ~20-30s en refrescarse).

### 3. Arquitectura por capas: raw inmutable → processed → backup

Escribir directamente en un formato de análisis (p.ej. Parquet) durante la captura introduce riesgo: un fallo a mitad del proceso puede corromper el archivo del día. Además, los requisitos de consulta para el EDA son distintos de los requisitos de la ingesta en tiempo real.

**Decisión:** separación en dos capas con responsabilidades distintas:
- **Raw:** JSON crudo comprimido, inmutable, deduplicado por timestamp de cabecera GTFS-RT. Nunca se toca tras escribirse.
- **Processed:** Parquet diario generado por un proceso de compactación nocturna independiente. El análisis y el modelado operan siempre sobre esta capa.

Este patrón es el estándar en arquitecturas lakehouse (medallion architecture) y minimiza el riesgo de pérdida de datos ante fallos del proceso de análisis.

### 4. Resiliencia con systemd

Un colector que se cae silenciosamente es equivalente a no tener colector: los datos se pierden sin que nadie lo note.

**Decisión:** servicios systemd con `Restart=always` y `RestartSec=10`. Verificado empíricamente con un reboot de control: ambos colectores volvieron a `active (running)` sin intervención manual. Logs rotativos para diagnóstico. Escritura atómica en la capa raw para evitar archivos parciales.

### 5. Fuente meteorológica: AEMET, descarte de Twitter/X

Se evaluaron varias fuentes exógenas:
- **Twitter/X:** descartado. La API tiene coste (0,005 $/lectura) y el contenido es redundante con el feed oficial de `alerts`, que ya incluye incidencias en texto libre. Se documenta el descarte en la memoria del TFM.
- **AEMET API abierta:** elegida. Gratuita, fiable, con cobertura histórica desde 2020 y resolución horaria. Se seleccionaron 15 estaciones del corredor de Cercanías validadas contra el inventario real.

**Sesgo conocido:** el histórico propio de retrasos cubre desde junio (verano, sin lluvia). El cruce clima-retraso en invierno solo será posible si se consigue histórico de terceros con licencia (p.ej. retrasosrenfe.com — pendiente verificar términos).

### 6. La línea de Cercanías no está en el feed

`route_id` es null en el 100% de los registros de `trip_updates` y `vehicle_positions`. La línea (C-1, C-3, C-4...) hay que obtenerla cruzando `trip_id` con `trips.txt` del GTFS estático, o mediante regex sobre el sufijo del propio `trip_id` (p.ej. `...C3` → línea C-3). El sufijo es rápido pero ambiguo: C-1 existe en Madrid, Valencia, Sevilla y Cádiz — necesita línea + núcleo para ser inequívoco.

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────┐
│  FUENTES                                                         │
│  RENFE GTFS-RT (trip_updates · vehicle_positions · alerts)       │
│  AEMET (observación horaria en vivo · climatología histórica)    │
└───────────────────────────┬──────────────────────────────────────┘
                            │  captura cada 60s (RENFE) / 60min (AEMET)
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  CAPA RAW  (inmutable)                                           │
│  data-renfe/raw/<feed>/<fecha>/*.json.gz                         │
│  data-aemet/raw/<feed>/*.json.gz                                 │
│  JSON crudo comprimido, dedup por timestamp de cabecera GTFS-RT  │
└───────────────────────────┬──────────────────────────────────────┘
                            │  compactación diaria (cron 03:30-03:35)
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  CAPA PROCESSED  (capa de trabajo)                               │
│  data-renfe/processed/<feed>/<feed>_<fecha>.parquet              │
│  data-aemet/processed/observacion_horaria/<fecha>.parquet        │
│  Tabular plano, listo para pandas / EDA / modelado               │
└───────────────────────────┬──────────────────────────────────────┘
                            │  backup diario (cron 04:00, root)
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  BACKUP — Google Drive (rclone), carpeta TFM-backup-datos/       │
└──────────────────────────────────────────────────────────────────┘
```

**Principio rector:** "cada tarea con su herramienta". Captura ligera 24/7 en VPS; cómputo pesado de modelado reservado para Databricks/notebooks — no para la ingesta.

---

## Infraestructura

- **Servidor:** VPS Hetzner CX23 (2 vCPU / 4GB RAM / 40GB SSD, Ubuntu 24.04), ~4,83 €/mes. Elegido por aceptar PayPal sin tarjeta de crédito.
- **Usuarios:** colectores y crons de compactación corren como `tfm`; backup a Drive en el crontab de `root`.
- **Backup:** `rclone` hacia Google Drive (`gdrive:TFM-backup-datos/`), copia diaria de `raw` (y `processed` cuando se incorpore al script).
- **Resiliencia:** servicios systemd con reinicio automático, verificados tras reboot de control.

---

## Estructura del repositorio

```
tfm-cercanias-colectores/
│
├── renfe-collector/
│   ├── scripts/
│   │   ├── collector.py              # Captura 24/7 de los 3 feeds GTFS-RT (systemd)
│   │   ├── compact_day.py            # Compactación diaria raw -> processed (Parquet)
│   │   ├── download_gtfs_static.py   # Descarga GTFS estático (trips.txt, stops.txt)
│   │   └── inspect_day.py            # Inspección/diagnóstico de un día de capturas
│   └── .venv/                        # Entorno Python (pandas, pyarrow)
│
├── aemet-collector/
│   ├── scripts/
│   │   ├── collector_live.py         # Captura horaria en vivo (systemd)
│   │   ├── backfill_historico.py     # Descarga climatología histórica (2020-2025)
│   │   ├── compact_aemet.py          # Compactación raw -> processed (Parquet)
│   │   ├── aemet_common.py           # Utilidades compartidas (API de dos saltos, parsing)
│   │   └── estaciones_madrid.py      # Listado de estaciones del corredor Cercanías
│   └── .env                          # API key AEMET (NO en git, en .gitignore)
│
├── explora.ipynb                     # Notebook de exploración/auditoría inicial
└── .gitignore
```

> En el servidor, los datos viven fuera del repo: `/home/tfm/data-renfe/` y `/home/tfm/data-aemet/` (carpetas `raw/` y `processed/`).

---

## Servicios y crons desplegados

### systemd (captura 24/7)

| Servicio | Comando | Estado |
|---|---|---|
| `renfe-collector.service` | `collector.py --data-dir /home/tfm/data-renfe --interval 60` | `enabled` + `active` |
| `aemet-collector.service` | `collector_live.py --loop --interval 3600 --data-dir /home/tfm/data-aemet` | `enabled` + `active` |

Ambos verificados tras reboot de control. Para actualizar: `git pull` + `systemctl restart <servicio>`.

### Cron de compactación (usuario `tfm`)

```cron
30 3 * * * .../renfe-collector/.venv/bin/python .../renfe-collector/scripts/compact_day.py --data-dir /home/tfm/data-renfe >> /home/tfm/logs/compact_renfe.log 2>&1
35 3 * * * .../renfe-collector/.venv/bin/python .../aemet-collector/scripts/compact_aemet.py --tipo observacion --date $(date -d yesterday +%Y-%m-%d) --data-dir /home/tfm/data-aemet >> /home/tfm/logs/compact_aemet.log 2>&1
```

Compacta siempre el **día anterior completo** (nunca el día en curso, que estaría incompleto). Un Parquet por día y por feed.

### Cron de backup (usuario `root`)

```cron
0 4 * * * /home/tfm/backup-drive.sh
```

Se ejecuta después de la compactación (03:30-03:35), copiando `raw` a Google Drive.

---

## Hallazgos de la auditoría inicial (datos reales, 13-14/06/2026)

Validados con ~469 snapshots RENFE y ~1.100 observaciones AEMET del primer día completo.

### Lo que el feed sí publica

- **Variable objetivo rica:** `arrival_delay_s` — mediana 0s, p90 ~6 min, p99 ~28 min, máximo observado ~54 min. ~33% de observaciones ≥5 min, ~10% ≥15 min.
- **Cobertura nacional:** el feed incluye Madrid, Barcelona, Valencia, Sevilla y otros núcleos (~121 trenes activos en hora punta de viernes). Pendiente confirmar si publica todos los trenes o solo los que presentan desviación — afecta a la definición del target.
- **AEMET sólida:** temperatura y precipitación con 0% de nulos. Viento ~16% nulos. Presión ~48% nulos — candidata a descarte.

### Lo que el feed NO publica (y cómo se compensa)

| Campo ausente | Impacto | Solución adoptada |
|---|---|---|
| `route_id` (100% null) | No se sabe la línea directamente | Cruce `trip_id` ↔ `trips.txt` del GTFS estático; regex sobre sufijo del `trip_id` como Plan B |
| Serie temporal del retraso | El feed da solo la próxima parada, no toda la trayectoria | Se reconstruye capturando cada 60s y cruzando snapshots consecutivos por `trip_id` |
| `bearing` / `speed` (100% null) | No hay velocidad instantánea del tren | No se usarán como features; la posición GPS sí está disponible |
| `cause` / `effect` en alerts (100% null) | El tipo de incidencia no viene estructurado | NLP obligatorio sobre `description_text` para extraer tipo, líneas y estaciones afectadas |
| `departure_delay_s` (~99% null) | Solo disponible el retraso de llegada | El target se define sobre `arrival_delay_s` |

### Anomalías conocidas

- **Glitch del 13/06:** 6,5% de registros con `arrival_delay_s` en torno a ±86.400s (~±24h). No reaparece en el 14/06. Posiblemente relacionado con `trip_schedule_relationship ≠ SCHEDULED`. `compact_day.py` registra aviso en log cuando `|delay| > 2h`. Filtro de consistencia temporal pendiente en la fase de limpieza.
- **Retrasos negativos:** mínimo observado ~-2.580s (~43 min de adelanto). Pueden ser legítimos o artefactos del feed — pendiente de análisis.

---

## Metodología de modelización prevista

- **Variable objetivo** (progresión): (a) clasificación por línea+franja, (b) regresión del retraso medio, (c) predicción de retraso tren+estación a 30-60 min.
- **Métricas**: F1 / PR-AUC (no accuracy, por desbalanceo). Baseline = tasa histórica por línea-franja.
- **Features**: calendario (hora punta, festivos), estado reciente de la red (propagación de retrasos), topológicas (paradas restantes, tramos compartidos), meteo (AEMET), incidencias (NLP sobre `description_text` de alerts).
- **Modelos**: baseline lineal → Random Forest / gradient boosting (XGBoost/LightGBM/CatBoost) → series temporales → deep learning (LSTM/GRU/atención, GNN si el volumen lo justifica).
- **NLP**: extracción de tipo/severidad/líneas afectadas desde `description_text`. Validación temporal sin fugas de datos.
- **Productivización**: servicio línea+franja+condiciones → predicción, con MLflow para ciclo de vida del modelo.

---

## Pendiente (orden de prioridad)

1. ~~Compactación diaria a Parquet (`compact_day.py`, `compact_aemet.py`) + cron~~ ✅ hecho 14/06
2. **Backfill histórico de AEMET** (`backfill_historico.py`) — climatología diaria 2020-2025
3. **Verificar licencia de retrasosrenfe.com** — posible histórico de invierno de terceros
4. **Integrar GTFS estático** — descargar y validar cruce `trip_id` ↔ `trips.txt`
5. Incluir `processed` en el backup a Drive; decidir si migrar a carpeta compartida del grupo


