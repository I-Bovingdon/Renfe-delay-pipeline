#!/usr/bin/env python3
"""
Construye las TABLAS DE REFERENCIA de Madrid a partir del GTFS estático versionado.

Reutiliza el filtro Madrid ESTRUCTURAL validado en el gate (03/08/2026):
  Madrid = route_id cuyo núcleo es 10 (dígitos antes de la 'T') -> sus trips
  -> sus stops (vía stop_times). Nada de bounding box geográfico.
El puente RT <-> GTFS es el 'core' del trip_id: quitar el prefijo de publicación
que RENFE incrusta distinto en RT y en estático (`^\d+[A-Za-z]`).

Genera dos CSV para el equipo (a subir al Drive compartido):
  - linea_por_core.csv    UNA fila por core -> línea (SOLO Madrid).
      Resuelve la línea de cada trip_id del RT, que trae route_id null. RENFE define
      varios route_id para la misma línea (variantes de terminal), así que un core
      puede tocar >1 route_id pero SIEMPRE de la misma línea; se colapsa a una fila
      por core para que el join del equipo no infle filas (fan-out).
  - estaciones_madrid.csv stop_id, nombre y lat/lon de las paradas del núcleo 10.
      Para filtrar Madrid por stop_id en trip_updates y en alerts, y para geo.

Este script NO calcula retrasos: el valor del retraso lo da el feed. Aquí solo se
deriva referencia (línea, filtro Madrid, geo) del estático. La versión-semana
correcta del GTFS es responsabilidad de quien llama: para datos de junio/julio hará
falta el GTFS histórico de esa semana (ver P4), no el vigente.

Uso (como tfm en el VPS):
  python3 build_gtfs_reference.py \
    --gtfs-zip /home/tfm/data-renfe/gtfs_static/gtfs_cercanias_20260805.zip
Si se omite --gtfs-zip, coge el gtfs_cercanias_*.zip más reciente de
  <data-dir>/gtfs_static/. Salida por defecto en <data-dir>/gtfs_static/reference/.
"""
import argparse
import glob
import re
import sys
import zipfile
from pathlib import Path

import pandas as pd

# --- Puentes de identificadores (idénticos al gate validado) ---
_PREF = re.compile(r"^\d+[A-Za-z]")
_NUC = re.compile(r"^(\d+)T")
_LINE = re.compile(r"(C\d+[a-z]?)$")


def core(trip_id: str) -> str:
    """core = trip_id sin el prefijo de publicación. Puente RT <-> GTFS."""
    return _PREF.sub("", trip_id)


def nucleo(route_id: str) -> str:
    """Núcleo = dígitos antes de la 'T' del route_id (10=Madrid, 51=Andalucía...)."""
    m = _NUC.match(route_id)
    return m.group(1) if m else "?"


def line_from_core(c: str) -> str:
    """Línea (C1..C10) codificada al final del core. Cross-check con routes.txt."""
    m = _LINE.search(c)
    return m.group(1) if m else "??"


def _read(z, name, usecols=None):
    """Lee un .txt del GTFS como str, limpiando cabeceras con relleno."""
    df = pd.read_csv(z.open(name), dtype=str, keep_default_na=False, usecols=usecols)
    df.columns = df.columns.str.strip()
    return df


def madrid_subset(gtfs_zip: str) -> dict:
    """Subconjunto Madrid del GTFS: routes, stops, route_ids/trips/stops de Madrid.

    Filtro estructural por núcleo 10 del route_id. `stop_times.txt` (~275 MB) se
    lee por streaming para no cargarlo entero en memoria.
    """
    with zipfile.ZipFile(gtfs_zip) as z:
        routes = _read(z, "routes.txt")
        trips = _read(z, "trips.txt")
        stops = _read(z, "stops.txt")
        for c in ("route_id", "trip_id", "stop_id"):
            for df in (routes, trips, stops):
                if c in df.columns:
                    df[c] = df[c].str.strip()

        mad_route_ids = set(routes.loc[routes["route_id"].map(nucleo) == "10", "route_id"])
        mad_trips = trips[trips["route_id"].isin(mad_route_ids)].copy()
        mad_trip_ids = set(mad_trips["trip_id"])

        # stop_times es enorme: recorrer por trozos y quedarnos solo con las
        # paradas de los trips de Madrid. Memoria plana, sin cargar el fichero.
        mad_stop_ids: set[str] = set()
        reader = pd.read_csv(
            z.open("stop_times.txt"),
            dtype=str,
            keep_default_na=False,
            usecols=lambda c: c.strip() in ("trip_id", "stop_id"),
            chunksize=500_000,
        )
        for chunk in reader:
            chunk.columns = chunk.columns.str.strip()
            tid = chunk["trip_id"].str.strip()
            sid = chunk["stop_id"].str.strip()
            mad_stop_ids.update(sid[tid.isin(mad_trip_ids)].tolist())

    return {
        "routes": routes,
        "stops": stops,
        "mad_route_ids": mad_route_ids,
        "mad_trips": mad_trips,
        "mad_stop_ids": mad_stop_ids,
    }


def build_linea_por_core(sub: dict) -> pd.DataFrame:
    """Una fila por core -> línea (solo Madrid).

    RENFE define varios route_id para la misma línea (variantes de terminal/itinerario:
    p.ej. C10 Villalba-Aeropuerto vs C10 El Escorial-Príncipe Pío). Por eso un core puede
    tocar >1 route_id, pero SIEMPRE de la misma línea. Se colapsa a una fila por core para
    que el join del equipo no infle filas (fan-out). El modelo necesita la LÍNEA; los
    route_id que colisionan se listan en `route_ids` para trazabilidad.
    """
    trips = sub["mad_trips"].copy()
    trips["core"] = trips["trip_id"].map(core)

    routes = sub["routes"].copy()
    for col in ("route_short_name", "route_long_name"):
        if col in routes.columns:
            routes[col] = routes[col].str.strip()  # RENFE rellena los nombres con espacios
    name_cols = [c for c in ("route_short_name", "route_long_name") if c in routes.columns]

    pairs = (
        trips[["core", "route_id"]]
        .merge(routes[["route_id"] + name_cols], on="route_id", how="left")
        .drop_duplicates()
    )
    has_short = "route_short_name" in pairs.columns

    # Chequeo que de verdad importa: ¿cada core -> una sola LÍNEA?
    if has_short:
        lines_per_core = pairs.groupby("core")["route_short_name"].nunique()
        collision = lines_per_core[lines_per_core > 1]
        if len(collision):
            print(
                f"AVISO GRAVE: {len(collision)} cores con >1 LÍNEA en Madrid "
                f"(colisión real del puente RT<->GTFS): {list(collision.index[:5])}",
                file=sys.stderr,
            )
        else:
            print("OK: cada core -> una única línea dentro de Madrid.")

    # Colapsar a una fila por core.
    grp = pairs.groupby("core")
    data = {
        "n_route_ids": grp["route_id"].nunique(),
        "route_ids": grp["route_id"].agg(lambda s: "|".join(sorted(set(s)))),
    }
    if has_short:
        data["linea"] = grp["route_short_name"].agg(lambda s: sorted(set(s))[0])
    ref = pd.DataFrame(data).reset_index()

    ref["line_from_core"] = ref["core"].map(line_from_core)
    if has_short:
        mismatch = (ref["linea"].str.upper() != ref["line_from_core"].str.upper()).sum()
        if mismatch:
            print(f"Nota: {mismatch} cores donde routes.txt y el sufijo del core discrepan.")
        ref = ref[["core", "linea", "line_from_core", "n_route_ids", "route_ids"]]
        ref = ref.sort_values(["linea", "core"])
    else:
        ref = ref.sort_values("core")
    return ref.reset_index(drop=True)


def build_estaciones_madrid(sub: dict) -> pd.DataFrame:
    """stop_id, nombre y lat/lon de las paradas del núcleo 10."""
    stops = sub["stops"]
    keep = [c for c in ("stop_id", "stop_name", "stop_lat", "stop_lon") if c in stops.columns]
    est = stops.loc[stops["stop_id"].isin(sub["mad_stop_ids"]), keep].copy()
    if "stop_name" in est.columns:
        est["stop_name"] = est["stop_name"].str.strip()
    return est.sort_values("stop_id").reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description="Tablas de referencia Madrid desde GTFS estático")
    ap.add_argument("--gtfs-zip", default=None, help="Ruta al zip GTFS. Si se omite, el más reciente.")
    ap.add_argument("--data-dir", default="/home/tfm/data-renfe")
    ap.add_argument("--out-dir", default=None, help="Salida. Por defecto <data-dir>/gtfs_static/reference")
    args = ap.parse_args()

    if args.gtfs_zip:
        gtfs_zip = args.gtfs_zip
    else:
        pattern = str(Path(args.data_dir) / "gtfs_static" / "gtfs_cercanias_*.zip")
        cands = sorted(glob.glob(pattern))
        if not cands:
            sys.exit(f"No hay GTFS en {pattern}. Ejecuta download_gtfs_static.py primero.")
        gtfs_zip = cands[-1]

    out_dir = Path(args.out_dir) if args.out_dir else Path(args.data_dir) / "gtfs_static" / "reference"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"GTFS: {gtfs_zip}")
    sub = madrid_subset(gtfs_zip)
    print(
        f"Madrid: {len(sub['mad_route_ids'])} rutas | "
        f"{len(sub['mad_trips'])} trips | {len(sub['mad_stop_ids'])} stops"
    )

    linea = build_linea_por_core(sub)
    est = build_estaciones_madrid(sub)

    p1 = out_dir / "linea_por_core.csv"
    p2 = out_dir / "estaciones_madrid.csv"
    linea.to_csv(p1, index=False)
    est.to_csv(p2, index=False)
    print(f"Escrito: {p1}  ({len(linea)} filas, 1 por core)")
    print(f"Escrito: {p2}  ({len(est)} filas)")


if __name__ == "__main__":
    main()
