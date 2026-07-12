#!/usr/bin/env python3
"""
Inspección rápida de lo capturado en un día. Sirve para dos cosas:
  1. Comprobar a diario que el colector funciona (sanidad).
  2. Empezar a entender los datos (primer mini-EDA).

Uso:
  python3 inspect_day.py                      # hoy (UTC)
  python3 inspect_day.py --date 2026-06-13
  python3 inspect_day.py --date 2026-06-13 --data-dir /ruta/data
"""

import argparse
import gzip
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def load_record(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def human_size(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def summarize_feed(feed_dir: Path, name: str):
    files = sorted(feed_dir.glob("*.json.gz"))
    if not files:
        print(f"  {name}: SIN FICHEROS ⚠️  (¿está corriendo el colector?)")
        return

    total_bytes = sum(f.stat().st_size for f in files)
    first, last = files[0].name, files[-1].name
    print(f"  {name}: {len(files)} capturas, {human_size(total_bytes)} | {first} -> {last}")

    # Análisis de la última captura
    record = load_record(files[-1])
    payload = record.get("payload", {})
    entities = payload.get("entity", []) or []
    print(f"    Última captura: {len(entities)} entidades")

    if name == "trip_updates" and entities:
        delays = []
        routes = Counter()
        for e in entities:
            tu = e.get("tripUpdate") or e.get("trip_update") or {}
            trip = tu.get("trip", {})
            routes[trip.get("routeId") or trip.get("route_id") or "?"] += 1
            for stu in (tu.get("stopTimeUpdate") or tu.get("stop_time_update") or []):
                for key in ("arrival", "departure"):
                    ev = stu.get(key) or {}
                    if "delay" in ev:
                        delays.append(ev["delay"])
        if delays:
            delays.sort()
            n = len(delays)
            p50 = delays[n // 2]
            p90 = delays[int(n * 0.9)]
            mx = delays[-1]
            late = sum(1 for d in delays if d >= 300)
            print(f"    Delays observados: n={n} | mediana={p50}s | p90={p90}s | max={mx}s "
                  f"| >=5min: {late} ({100*late/n:.1f}%)")
        top = ", ".join(f"{r}({c})" for r, c in routes.most_common(8))
        print(f"    Trips por ruta (top): {top}")

    if name == "alerts" and entities:
        sample = entities[0].get("alert", {})
        # texto de cabecera de la primera alerta, si existe
        txts = (sample.get("headerText") or sample.get("header_text") or {}).get("translation", [])
        if txts:
            print(f"    Ejemplo de alerta: {txts[0].get('text', '')[:120]}")

    if name == "vehicle_positions" and entities:
        statuses = Counter()
        for e in entities:
            v = e.get("vehicle", {})
            statuses[v.get("currentStatus") or v.get("current_status") or "?"] += 1
        print(f"    Estados de vehículos: {dict(statuses)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    args = parser.parse_args()

    raw = Path(args.data_dir).resolve() / "raw"
    print(f"Resumen de capturas del {args.date} (UTC) en {raw}\n")
    for name in ["trip_updates", "vehicle_positions", "alerts"]:
        summarize_feed(raw / name / args.date, name)
    print("\nSi 'trip_updates' muestra delays, el proyecto tiene target. ✅")


if __name__ == "__main__":
    main()
