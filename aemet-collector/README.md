# Colector meteorológico — AEMET OpenData (TFM Cercanías Madrid)

Captura meteorología para usarla como **variable exógena** del modelo de
predicción de retrasos. Dos piezas independientes:

| Pieza | Script | Qué hace | Frecuencia |
|---|---|---|---|
| **En vivo** | `collector_live.py` | Observación horaria de las estaciones de Madrid, para emparejar con los retrasos capturados en tiempo real | 1/hora |
| **Histórico** | `backfill_historico.py` | Climatología diaria de años pasados (contexto de temporales de invierno) | una vez (o por tramos) |

> **Por qué importa**: el histórico propio de retrasos será solo de verano
> (sin apenas lluvia). AEMET es la única fuente con histórico profundo, y la
> meteorología tiene un efecto físico conocido sobre la circulación: los
> temporales degradan el servicio. La feature queda justificada aunque su
> poder predictivo no se demuestre del todo hasta que lleguen las lluvias.

## La API de AEMET en 30 segundos

- Acceso con **API Key** (cabecera `api_key`), gratuita, se pide por web.
- Es **de dos saltos**: la 1ª llamada devuelve una URL; la 2ª descarga el dato.
  El cliente (`aemet_common.py`) lo gestiona solo.
- Los textos vienen en **latin-9** (no UTF-8) y los decimales **con coma**: el
  código ya lo normaliza.
- Códigos: 200 OK · 401/403 clave caducada · 404 sin datos · 429 límite.

## Puesta en marcha

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Clave: copia .env.example a .env y pega tu API Key
cp .env.example .env
# edita .env  ->  AEMET_API_KEY=tu_clave

# 0) (recomendado la primera vez) verifica las estaciones reales:
python3 scripts/collector_live.py --inventario
#   revisa la lista impresa y ajusta scripts/estaciones_madrid.py si hace falta

# 1) prueba EN VIVO (una pasada):
python3 scripts/collector_live.py
python3 scripts/compact_aemet.py --tipo observacion --date $(date -u +%F)

# 2) HISTÓRICO (ejemplo: últimos años):
python3 scripts/backfill_historico.py --desde 2020-01-01 --hasta 2025-12-31
python3 scripts/compact_aemet.py --tipo climatologia
```

## Dejarlo corriendo 24/7 (en vivo)

```bash
# Ajusta User/rutas en deploy/aemet-collector.service
sudo cp deploy/aemet-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aemet-collector
systemctl status aemet-collector
```

El backfill histórico **no** es un servicio: se lanza a mano una vez (o por
tramos de años). Puede tardar, porque pausa 2 s entre peticiones para respetar
los límites de AEMET y trocea el rango en ventanas de ~5 meses.

### Cron

```cron
# Usuario tfm. Compacta el día ANTERIOR, después de la compactación de Renfe.
35 3 * * * /home/tfm/tfm-cercanias-colectores/renfe-collector/.venv/bin/python /home/tfm/tfm-cercanias-colectores/aemet-collector/scripts/compact_aemet.py --tipo observacion --date $(date -u -d yesterday +\%F) --data-dir /home/tfm/data-aemet >> /home/tfm/logs/compact_aemet.log 2>&1
```

Dos detalles que ya costaron un fallo silencioso de 27 días:

- **`yesterday`, no la fecha de hoy.** A las 03:35 el día en curso apenas tiene datos.
- **El `%` va escapado como `\%`.** En cron, un `%` sin escapar corta la línea.

## Caducidad de la API Key

La clave caduca cada cierto tiempo. **No hace falta automatizar la renovación**
(es un formulario de 30 s). El colector está preparado para sobrevivir a la
caducidad: ante un 401/403 **no se cae**, lo registra en el log de forma visible
y reintenta en la siguiente pasada. Cuando eso ocurra:

1. Pide una clave nueva en el portal de AEMET OpenData (mismo correo del grupo).
2. Edita la línea `AEMET_API_KEY=` del `.env`.
3. `sudo systemctl restart aemet-collector`.

Poned un recordatorio en el calendario del grupo unas semanas antes de la
fecha estimada de caducidad. Cero pérdida de datos.

## Estructura de datos

```
data/
├── raw/
│   ├── observacion_horaria/2026-06-13/...     (solo estaciones Madrid)
│   ├── observacion_nacional/2026-06-13/...     (volcado nacional, por si se amplía)
│   └── climatologia_diaria/2024-02/...         (histórico, particionado por mes)
├── processed/
│   ├── observacion_horaria/*.parquet
│   └── climatologia_diaria/*.parquet
├── inventario_estaciones.json
└── logs/
```

## Integración con el modelo

La unión con los retrasos se hace por **tiempo y geografía**:
- Observación horaria ↔ retrasos: por hora y por estación AEMET más cercana
  a la línea/tramo (las coordenadas están en el inventario).
- Climatología diaria: para el análisis de contraste clima-retraso a nivel día.

> Recordatorio honesto para la memoria: el cruce clima↔retraso a nivel de
> invierno solo será posible si se consigue un histórico de retrasos que cubra
> esa época (p.ej. de terceros con licencia). Con el histórico propio, la
> meteorología entra como feature del periodo capturado.
