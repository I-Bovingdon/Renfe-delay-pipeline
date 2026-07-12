#!/usr/bin/env python3
"""
Compactación AEMET: convierte las capturas crudas a Parquet tabular.

Maneja los dos tipos de dato:
  - observacion_horaria  -> capa processed/observacion_horaria
  - climatologia_diaria  -> capa processed/climatologia_diaria

Los nombres de campo de AEMET son crípticos; este script los renombra a algo
legible y convierte los decimales (que AEMET da con coma) a float.

Uso:
  python3 compact_aemet.py --tipo observacion --date 2026-06-13
  python3 compact_aemet.py --tipo climatologia        # compacta todo lo descargado
"""

import argparse
import glob
import gzip
import json
from pathlib import Path

import pandas as pd

# Renombrado de campos de OBSERVACIÓN HORARIA (los más útiles)
MAP_OBS = {
    "idema": "estacion_id", "ubi": "estacion_nombre", "fint": "fecha_hora",
    "ta": "temp_aire_c", "tamax": "temp_max_c", "tamin": "temp_min_c",
    "prec": "precip_mm", "vv": "viento_vel_ms", "vmax": "viento_racha_ms",
    "dv": "viento_dir_grados", "hr": "humedad_rel_pct", "pres": "presion_hpa",
    "lat": "latitud", "lon": "longitud", "alt": "altitud_m",
}
# Renombrado de CLIMATOLOGÍA DIARIA
MAP_CLIMA = {
    "indicativo": "estacion_id", "nombre": "estacion_nombre", "fecha": "fecha",
    "provincia": "provincia", "altitud": "altitud_m",
    "tmed": "temp_media_c", "tmax": "temp_max_c", "tmin": "temp_min_c",
    "prec": "precip_mm", "velmedia": "viento_vel_media_ms", "racha": "viento_racha_ms",
    "dir": "viento_dir", "sol": "horas_sol", "presMax": "presion_max_hpa",
    "presMin": "presion_min_hpa",
}


def to_float(v):
    """AEMET usa coma decimal y marca trazas de lluvia como 'Ip'."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", ".")
    if s in ("", "Ip", "Acum"):  # 'Ip' = precipitación inapreciable
        return 0.0 if s == "Ip" else None
    try:
        return float(s)
    except ValueError:
        return None


def iter_payloads(data_dir: Path, feed: str, date_filter: str | None):
    base = data_dir / "raw" / feed
    if not base.exists():
        return
    pattern = f"**/{feed}_*.json.gz"
    for path in sorted(base.glob(pattern)):
        if date_filter and date_filter not in path.parent.name and date_filter not in path.name:
            continue
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                rec = json.load(fh)
            yield rec.get("fetched_at_stamp"), rec.get("payload", [])
        except (OSError, json.JSONDecodeError):
            print(f"  AVISO: fichero ilegible {path.name}")


def compact_observacion(data_dir: Path, date_filter: str | None):
    rows = []
    for stamp, payload in iter_payloads(data_dir, "observacion_horaria", date_filter):
        if not isinstance(payload, list):
            continue
        for obs in payload:
            row = {"captura_stamp": stamp}
            for k, v in MAP_OBS.items():
                if k in obs:
                    row[v] = obs[k]
            for c in ("temp_aire_c", "temp_max_c", "temp_min_c", "precip_mm",
                      "viento_vel_ms", "viento_racha_ms", "humedad_rel_pct", "presion_hpa"):
                if c in row:
                    row[c] = to_float(row[c])
            rows.append(row)
    return pd.DataFrame(rows)


def compact_climatologia(data_dir: Path, date_filter: str | None):
    rows = []
    for stamp, payload in iter_payloads(data_dir, "climatologia_diaria", date_filter):
        if not isinstance(payload, list):
            continue
        for dia in payload:
            row = {}
            for k, v in MAP_CLIMA.items():
                if k in dia:
                    row[v] = dia[k]
            for c in ("temp_media_c", "temp_max_c", "temp_min_c", "precip_mm",
                      "viento_vel_media_ms", "viento_racha_ms", "horas_sol",
                      "presion_max_hpa", "presion_min_hpa"):
                if c in row:
                    row[c] = to_float(row[c])
            rows.append(row)
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--tipo", required=True, choices=["observacion", "climatologia"])
    parser.add_argument("--date", default=None, help="Filtro AAAA-MM-DD (observación) o AAAA-MM (clima)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if args.tipo == "observacion":
        df = compact_observacion(data_dir, args.date)
        feed = "observacion_horaria"
    else:
        df = compact_climatologia(data_dir, args.date)
        feed = "climatologia_diaria"

    if df.empty:
        print(f"{feed}: no hay datos que compactar.")
        return

    out_dir = data_dir / "processed" / feed
    out_dir.mkdir(parents=True, exist_ok=True)
    sufijo = args.date or "all"
    out_path = out_dir / f"{feed}_{sufijo}.parquet"
    df.to_parquet(out_path, index=False, compression="zstd")
    print(f"{feed}: {len(df):,} filas -> {out_path.name} ({out_path.stat().st_size/1e6:.2f} MB)")


if __name__ == "__main__":
    main()
