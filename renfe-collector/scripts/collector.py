#!/usr/bin/env python3
"""
Colector de feeds GTFS-Realtime de RENFE Cercanías.
TFM - Predicción de retrasos en Cercanías Madrid.

Descarga periódicamente los 3 feeds en tiempo real de RENFE:
  - trip_updates.json     -> retrasos por tren y parada (VARIABLE OBJETIVO)
  - vehicle_positions.json-> posición GPS y estado de los trenes
  - alerts.json           -> incidencias y avisos (para NLP)

Los guarda comprimidos (gzip) en una estructura particionada por feed y fecha:
  data/raw/<feed>/<YYYY-MM-DD>/<feed>_<YYYYMMDDTHHMMSSZ>.json.gz

Filosofía: "raw first". Se guarda el JSON crudo tal cual llega.
El procesado/parseo se hace después, en otra fase. Si mañana descubrimos
que necesitamos un campo que hoy ignoraríamos, lo tendremos.

Deduplicación: cada feed GTFS-RT lleva un timestamp de generación en la
cabecera. Si el timestamp no ha cambiado desde la última descarga, no se
guarda (evita duplicados cuando el servidor aún no ha refrescado).

Uso:
  python3 collector.py                 # bucle infinito, intervalo por defecto
  python3 collector.py --interval 30   # sondear cada 30 segundos
  python3 collector.py --once          # una sola pasada (útil con cron)
  python3 collector.py --data-dir /ruta/al/disco/data
"""

import argparse
import gzip
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ----------------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------------

FEEDS = {
    # nombre_feed: URL oficial (portal data.renfe.com -> gtfsrt.renfe.com)
    "trip_updates": "https://gtfsrt.renfe.com/trip_updates.json",
    "vehicle_positions": "https://gtfsrt.renfe.com/vehicle_positions.json",
    "alerts": "https://gtfsrt.renfe.com/alerts.json",
}

DEFAULT_INTERVAL_S = 60          # los feeds se refrescan cada ~20-30 s; 60 s es suficiente y educado
REQUEST_TIMEOUT_S = 20
MAX_RETRIES_PER_POLL = 2         # reintentos dentro de una misma pasada
RETRY_BACKOFF_S = 5
USER_AGENT = "TFM-UCM-CercaniasDelays/1.0 (proyecto academico; contacto: PON_AQUI_TU_EMAIL)"

# --- Watchdog de contenido (detecta apagones silenciosos del emisor, como el 12/07) ---
MADRID_TZ = ZoneInfo("Europe/Madrid")   # hora local; gestiona el cambio de hora solo
SERVICE_START_HOUR = 6                   # horario de servicio Cercanias: 06:00...
SERVICE_END_HOUR = 23                    # ...hasta 23:00 (fuera de esa franja, vacio es esperable)
EMPTY_THRESHOLD = 10                     # nº de ciclos vacios consecutivos antes del 1er WARNING
EMPTY_ALARM_REPEAT = 60                  # recordatorio cada N ciclos (~1h) mientras persista
WATCHDOG_FEEDS = set(FEEDS)              # vigilar los 3 feeds

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("collector")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = RotatingFileHandler(log_dir / "collector.log", maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ----------------------------------------------------------------------------
# Colector
# ----------------------------------------------------------------------------


class Collector:
    def __init__(self, data_dir: Path, logger: logging.Logger):
        self.data_dir = data_dir
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        # último timestamp de cabecera visto por feed (para deduplicar)
        self.last_header_ts: dict[str, str] = {}
        # contadores para el resumen periódico
        self.stats = {name: {"saved": 0, "dup": 0, "errors": 0} for name in FEEDS}
        # Watchdog de contenido: racha de ciclos vacios y flag de alarma por feed
        self.empty_streak: dict[str, int] = {name: 0 for name in FEEDS}
        self.empty_alarm: dict[str, bool] = {name: False for name in FEEDS}
        self._stop = False

    # --- señales (parada limpia con systemd / Ctrl+C) ---
    def install_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    def _handle_stop(self, signum, frame):
        self.logger.info("Señal %s recibida, parando tras la pasada actual...", signum)
        self._stop = True

    # --- descarga de un feed con reintentos ---
    def fetch_feed(self, name: str, url: str) -> dict | None:
        for attempt in range(1, MAX_RETRIES_PER_POLL + 1):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT_S)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, json.JSONDecodeError) as exc:
                self.logger.warning("[%s] intento %d/%d fallido: %s",
                                    name, attempt, MAX_RETRIES_PER_POLL, exc)
                if attempt < MAX_RETRIES_PER_POLL:
                    time.sleep(RETRY_BACKOFF_S)
        self.stats[name]["errors"] += 1
        return None

    # --- extracción del timestamp de cabecera GTFS-RT ---
    @staticmethod
    def header_timestamp(payload: dict) -> str:
        header = payload.get("header", {}) if isinstance(payload, dict) else {}
        return str(header.get("timestamp", ""))

    # --- guardado en disco ---
    def save(self, name: str, payload: dict, fetched_at: datetime) -> Path:
        day = fetched_at.strftime("%Y-%m-%d")
        stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
        out_dir = self.data_dir / "raw" / name / day
        out_dir.mkdir(parents=True, exist_ok=True)
        # Incluimos el timestamp de cabecera del feed en el nombre: garantiza
        # unicidad (la dedup asegura que nunca guardamos el mismo ts dos veces)
        # y permite ordenar por tiempo de generación real del feed.
        feed_ts = self.header_timestamp(payload) or fetched_at.strftime("%f")
        out_path = out_dir / f"{name}_{stamp}_ft{feed_ts}.json.gz"

        # Envolvemos el payload con metadatos de captura: nunca está de más
        record = {
            "fetched_at_utc": fetched_at.isoformat(),
            "feed": name,
            "payload": payload,
        }
        tmp_path = out_path.with_suffix(".tmp")
        with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
            json.dump(record, fh, ensure_ascii=False, separators=(",", ":"))
        tmp_path.rename(out_path)  # escritura atómica: nunca quedan ficheros a medias
        return out_path

    # --- una pasada completa por los 3 feeds ---
    def poll_once(self):
        fetched_at = datetime.now(timezone.utc)
        for name, url in FEEDS.items():
            payload = self.fetch_feed(name, url)
            if payload is None:
                continue

            # Watchdog de contenido: evaluar SIEMPRE, antes de la dedup, para que
            # un apagon con timestamp de cabecera congelado tambien cuente como vacio.
            n_entities = len(payload.get("entity", []) or [])
            self._check_empty_content(name, n_entities, fetched_at)

            ts = self.header_timestamp(payload)
            if ts and ts == self.last_header_ts.get(name):
                # El servidor aún no ha generado un feed nuevo: no duplicamos
                self.stats[name]["dup"] += 1
                continue

            self.last_header_ts[name] = ts
            path = self.save(name, payload, fetched_at)
            self.stats[name]["saved"] += 1
            self.logger.debug("[%s] guardado %s (%d entidades)", name, path.name, n_entities)

    # --- watchdog de contenido: alerta si un feed viene vacio en horario de servicio ---
    def _in_service_window(self, when_utc: datetime) -> bool:
        """True si el instante (en hora local de Madrid) cae en horario de servicio."""
        local = when_utc.astimezone(MADRID_TZ)
        return SERVICE_START_HOUR <= local.hour < SERVICE_END_HOUR

    def _check_empty_content(self, name: str, n_entities: int, when_utc: datetime):
        """Lleva la cuenta de ciclos vacios consecutivos por feed y avisa al log.

        Motivacion: el 12/07 el emisor devolvio HTTP 200 con 'entity' vacio ~29h.
        Como no era un error de proceso, ningun control basado en errores salto.
        Aqui vigilamos el CONTENIDO, no el proceso. Fuera del horario de servicio
        el vacio es esperable (no circulan trenes), asi que no se alarma.
        """
        if name not in WATCHDOG_FEEDS:
            return

        if n_entities > 0:
            # Contenido normal. Si veniamos de alarma, avisamos de la recuperacion.
            if self.empty_alarm[name]:
                self.logger.warning(
                    "[%s] CONTENIDO RECUPERADO tras %d ciclos vacios: el feed "
                    "vuelve a traer entidades.", name, self.empty_streak[name])
                self.empty_alarm[name] = False
            self.empty_streak[name] = 0
            return

        # n_entities == 0: feed estructuralmente valido pero sin contenido
        self.empty_streak[name] += 1
        streak = self.empty_streak[name]

        if not self._in_service_window(when_utc):
            return  # vacio nocturno esperable: no alarmamos

        if streak == EMPTY_THRESHOLD and not self.empty_alarm[name]:
            self.empty_alarm[name] = True
            self.logger.warning(
                "[%s] CONTENIDO VACIO: %d ciclos consecutivos sin entidades en "
                "horario de servicio. Posible caida del emisor (como el 12/07). "
                "Revisar el feed de origen.", name, streak)
        elif self.empty_alarm[name] and streak % EMPTY_ALARM_REPEAT == 0:
            self.logger.warning(
                "[%s] CONTENIDO VACIO PERSISTENTE: %d ciclos consecutivos sin "
                "entidades.", name, streak)

    # --- resumen periódico para el log (sanidad del sistema) ---
    def log_summary(self):
        parts = []
        for name, s in self.stats.items():
            parts.append(f"{name}: {s['saved']} guardados, {s['dup']} sin cambios, {s['errors']} errores")
        self.logger.info("RESUMEN | " + " | ".join(parts))

    # --- bucle principal ---
    def run(self, interval_s: int):
        self.logger.info("Colector iniciado. Intervalo=%ds. Data dir=%s", interval_s, self.data_dir)
        last_summary = time.monotonic()
        while not self._stop:
            start = time.monotonic()
            try:
                self.poll_once()
            except Exception:
                # Nunca dejamos morir el bucle por un error inesperado
                self.logger.exception("Error inesperado en la pasada")

            if time.monotonic() - last_summary > 3600:  # resumen cada hora
                self.log_summary()
                last_summary = time.monotonic()

            elapsed = time.monotonic() - start
            sleep_for = max(1.0, interval_s - elapsed)
            # dormir en trozos pequeños para reaccionar rápido a SIGTERM
            end = time.monotonic() + sleep_for
            while not self._stop and time.monotonic() < end:
                time.sleep(0.5)

        self.log_summary()
        self.logger.info("Colector detenido limpiamente.")


# ----------------------------------------------------------------------------
# Entrada
# ----------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Colector GTFS-RT RENFE Cercanías")
    parser.add_argument("--data-dir", default="data", help="Directorio raíz de datos (por defecto ./data)")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S, help="Segundos entre pasadas")
    parser.add_argument("--once", action="store_true", help="Hacer una sola pasada y salir (modo cron)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    logger = setup_logging(data_dir / "logs")

    collector = Collector(data_dir, logger)
    collector.install_signal_handlers()

    if args.once:
        collector.poll_once()
        collector.log_summary()
    else:
        collector.run(args.interval)


if __name__ == "__main__":
    main()
