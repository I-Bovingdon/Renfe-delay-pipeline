#!/usr/bin/env python3
"""
Descarga del GTFS ESTÁTICO de RENFE Cercanías (horarios teóricos).

¿Por qué es imprescindible?
  retraso = hora real (trip_updates) - hora programada (GTFS estático)
  Además, el estático contiene el catálogo de trips, rutas/líneas, paradas
  y calendarios que permiten interpretar los trip_id del feed en tiempo real.

Los horarios cambian (festivos, obras, nuevos servicios), así que hay que
descargarlo periódicamente (1 vez/semana es razonable) y VERSIONARLO:
guardamos cada descarga con su fecha y solo si el contenido ha cambiado.

Uso:
  python3 download_gtfs_static.py
  python3 download_gtfs_static.py --data-dir /ruta/data

Nota sobre la URL: el zip oficial está enlazado en el portal
https://data.renfe.com (dataset de horarios GTFS de Cercanías).
Si la URL de abajo dejara de funcionar, entrad al portal, localizad el
dataset GTFS de Cercanías y actualizad GTFS_STATIC_URL.
"""

import argparse
import hashlib
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

# URL histórica del GTFS de Cercanías (Fomento/MITMA). Verificad en
# data.renfe.com que sigue siendo la vigente la primera vez que lo ejecutéis.
GTFS_STATIC_URL = "https://data.renfe.com/dataset/77525d66-f095-46d0-80c0-37f735d4342f/resource/6f1523c6-a9e3-48e3-9ace-bb107a762be6/download/fomento_transit.zip"

USER_AGENT = "TFM-UCM-CercaniasDelays/1.0 (proyecto academico; contacto: PON_AQUI_TU_EMAIL)"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Descarga GTFS estático RENFE Cercanías")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--url", default=GTFS_STATIC_URL)
    args = parser.parse_args()

    out_dir = Path(args.data_dir).resolve() / "gtfs_static"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Descargando {args.url} ...")
    resp = requests.get(args.url, timeout=120, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    content = resp.content

    # Validar que es un zip GTFS de verdad antes de guardarlo
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    tmp = out_dir / f"_tmp_{stamp}.zip"
    tmp.write_bytes(content)
    try:
        with zipfile.ZipFile(tmp) as zf:
            names = zf.namelist()
            required = {"stops.txt", "trips.txt", "stop_times.txt", "routes.txt"}
            missing = required - {Path(n).name for n in names}
            if missing:
                print(f"AVISO: el zip no contiene {missing}. ¿Es la URL correcta?")
    except zipfile.BadZipFile:
        tmp.unlink()
        sys.exit("ERROR: lo descargado no es un zip válido. Revisa la URL en data.renfe.com")

    digest = sha256(content)

    # ¿Es idéntico al último guardado? Entonces no versionamos otra copia.
    hash_file = out_dir / "last_hash.txt"
    if hash_file.exists() and hash_file.read_text().strip() == digest:
        tmp.unlink()
        print("Sin cambios respecto a la última versión. No se guarda copia nueva.")
        return

    final = out_dir / f"gtfs_cercanias_{stamp}.zip"
    tmp.rename(final)
    hash_file.write_text(digest)
    print(f"Guardado: {final}  (sha256={digest[:12]}...)")


if __name__ == "__main__":
    main()
