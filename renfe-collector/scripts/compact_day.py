#!/usr/bin/env python3
"""
Compactación diaria: convierte las capturas crudas de un día (cientos de
.json.gz) en UN fichero Parquet por feed, con estructura tabular plana
lista para pandas/Colab.

Capas de datos resultantes:
  data/raw/        -> archivo histórico inmutable (lo que bajó el colector)
  data/processed/  -> capa de trabajo (parquets diarios, lo que se analiza
                      y lo que se sincroniza a Google Drive)

Salida:
  data/processed/trip_updates/trip_updates_2026-06-13.parquet
  data/processed/vehicle_positions/vehicle_positions_2026-06-13.parquet
  data/processed/alerts/alerts_2026-06-13.parquet

Se conservan TODOS los snapshots (no se deduplica por trip): la evolución
temporal del delay es información de entrenamiento (estado conocido en t).

Uso:
  python3 compact_day.py                       # compacta AYER (UTC), modo cron
  python3 compact_day.py --date 2026-06-13
  python3 compact_day.py --date 2026-06-13 --data-dir /ruta/data
"""

import argparse
import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

FEEDS = ["trip_updates", "vehicle_positions", "alerts"]


# ----------------------------------------------------------------------------
# Utilidades: los feeds JSON de GTFS-RT pueden venir en camelCase o snake_case
# según la herramienta que los genere. Normalizamos el acceso.
# ----------------------------------------------------------------------------

def g(d: dict, *keys, default=None):
    """Devuelve el primer valor presente probando varias claves."""
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default


def iter_records(day_dir: Path):
    """Itera (metadatos, payload) de cada captura del día, en orden temporal."""
    for path in sorted(day_dir.glob("*.json.gz")):
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                rec = json.load(fh)
            yield rec.get("fetched_at_utc"), rec.get("payload", {}) or {}
        except (OSError, json.JSONDecodeError) as exc:
            print(f"  AVISO: fichero corrupto ignorado: {path.name} ({exc})")


# ----------------------------------------------------------------------------
# Parsers por feed -> lista de filas planas
# ----------------------------------------------------------------------------

def parse_trip_updates(fetched_at: str, payload: dict) -> list[dict]:
    rows = []
    feed_ts = g(payload.get("header", {}) or {}, "timestamp")
    for ent in payload.get("entity", []) or []:
        tu = g(ent, "tripUpdate", "trip_update") or {}
        trip = tu.get("trip", {}) or {}
        base = {
            "fetched_at_utc": fetched_at,
            "feed_timestamp": feed_ts,
            "entity_id": ent.get("id"),
            "trip_id": g(trip, "tripId", "trip_id"),
            "route_id": g(trip, "routeId", "route_id"),
            "start_date": g(trip, "startDate", "start_date"),
            "start_time": g(trip, "startTime", "start_time"),
            "trip_schedule_relationship": g(trip, "scheduleRelationship", "schedule_relationship"),
            "trip_update_timestamp": g(tu, "timestamp"),
        }
        stus = g(tu, "stopTimeUpdate", "stop_time_update") or []
        if not stus:
            # Trip sin detalle de paradas (p.ej. CANCELED): fila única
            rows.append({**base, "stop_id": None, "stop_sequence": None,
                         "arrival_delay_s": None, "arrival_time": None,
                         "departure_delay_s": None, "departure_time": None,
                         "stop_schedule_relationship": None})
            continue
        for stu in stus:
            arr = g(stu, "arrival") or {}
            dep = g(stu, "departure") or {}
            rows.append({
                **base,
                "stop_id": g(stu, "stopId", "stop_id"),
                "stop_sequence": g(stu, "stopSequence", "stop_sequence"),
                "arrival_delay_s": arr.get("delay"),
                "arrival_time": arr.get("time"),
                "departure_delay_s": dep.get("delay"),
                "departure_time": dep.get("time"),
                "stop_schedule_relationship": g(stu, "scheduleRelationship", "schedule_relationship"),
            })
    return rows


def parse_vehicle_positions(fetched_at: str, payload: dict) -> list[dict]:
    rows = []
    feed_ts = g(payload.get("header", {}) or {}, "timestamp")
    for ent in payload.get("entity", []) or []:
        v = ent.get("vehicle", {}) or {}
        trip = v.get("trip", {}) or {}
        pos = v.get("position", {}) or {}
        veh = v.get("vehicle", {}) or {}  # descriptor anidado (id, label)
        rows.append({
            "fetched_at_utc": fetched_at,
            "feed_timestamp": feed_ts,
            "entity_id": ent.get("id"),
            "vehicle_id": g(veh, "id"),
            "vehicle_label": g(veh, "label"),
            "trip_id": g(trip, "tripId", "trip_id"),
            "route_id": g(trip, "routeId", "route_id"),
            "latitude": pos.get("latitude"),
            "longitude": pos.get("longitude"),
            "bearing": pos.get("bearing"),
            "speed": pos.get("speed"),
            "current_status": g(v, "currentStatus", "current_status"),
            "current_stop_sequence": g(v, "currentStopSequence", "current_stop_sequence"),
            "stop_id": g(v, "stopId", "stop_id"),
            "vehicle_timestamp": g(v, "timestamp"),
        })
    return rows


def _first_translation(field: dict | None) -> str | None:
    if not field:
        return None
    trans = field.get("translation") or []
    if not trans:
        return None
    # preferimos español si está etiquetado; si no, la primera
    for t in trans:
        if t.get("language", "").lower().startswith("es"):
            return t.get("text")
    return trans[0].get("text")


def parse_alerts(fetched_at: str, payload: dict) -> list[dict]:
    rows = []
    feed_ts = g(payload.get("header", {}) or {}, "timestamp")
    for ent in payload.get("entity", []) or []:
        a = ent.get("alert", {}) or {}
        periods = g(a, "activePeriod", "active_period") or [{}]
        informed = g(a, "informedEntity", "informed_entity") or [{}]
        header = _first_translation(g(a, "headerText", "header_text"))
        desc = _first_translation(g(a, "descriptionText", "description_text"))
        # explotamos por entidad afectada (ruta/parada) para poder cruzar
        for ie in informed:
            rows.append({
                "fetched_at_utc": fetched_at,
                "feed_timestamp": feed_ts,
                "entity_id": ent.get("id"),
                "cause": a.get("cause"),
                "effect": a.get("effect"),
                "active_start": g(periods[0], "start"),
                "active_end": g(periods[0], "end"),
                "informed_route_id": g(ie, "routeId", "route_id"),
                "informed_stop_id": g(ie, "stopId", "stop_id"),
                "informed_agency_id": g(ie, "agencyId", "agency_id"),
                "header_text": header,
                "description_text": desc,
            })
    return rows


PARSERS = {
    "trip_updates": parse_trip_updates,
    "vehicle_positions": parse_vehicle_positions,
    "alerts": parse_alerts,
}


# ----------------------------------------------------------------------------
# Compactación
# ----------------------------------------------------------------------------

def compact_feed(data_dir: Path, feed: str, date: str) -> Path | None:
    day_dir = data_dir / "raw" / feed / date
    if not day_dir.exists():
        print(f"  {feed}: sin directorio para {date}, se omite.")
        return None

    all_rows: list[dict] = []
    n_files = 0
    for fetched_at, payload in iter_records(day_dir):
        n_files += 1
        all_rows.extend(PARSERS[feed](fetched_at, payload))

    if not all_rows:
        print(f"  {feed}: {n_files} capturas pero 0 filas (¿feeds vacíos de madrugada?).")
        return None

    df = pd.DataFrame(all_rows)
    # tipos compactos donde aplica
    for col in ("arrival_delay_s", "departure_delay_s", "stop_sequence",
                "current_stop_sequence"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    
  # Chequeo de sanidad: detectar delays anómalos (~±24h, glitch conocido del feed)
    if feed == "trip_updates" and "arrival_delay_s" in df.columns:
        n_outliers = (df["arrival_delay_s"].abs() > 7200).sum()
        if n_outliers:
            pct = 100 * n_outliers / len(df)
            print(f"  {feed}: AVISO {n_outliers} filas ({pct:.1f}%) con |delay| > 2h (posible glitch de feed)")
  
    out_dir = data_dir / "processed" / feed
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{feed}_{date}.parquet"
    df.to_parquet(out_path, index=False, compression="zstd")
    size_mb = out_path.stat().st_size / 1e6
    print(f"  {feed}: {n_files} capturas -> {len(df):,} filas -> {out_path.name} ({size_mb:.1f} MB)")
    return out_path


def main():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(description="Compacta raw JSON -> Parquet diario")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--date", default=yesterday, help="Día a compactar (por defecto: ayer UTC)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    print(f"Compactando {args.date} en {data_dir}")
    for feed in FEEDS:
        compact_feed(data_dir, feed, args.date)
    print("Hecho.")


if __name__ == "__main__":
    main()
