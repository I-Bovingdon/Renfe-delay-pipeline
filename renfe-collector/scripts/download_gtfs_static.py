#!/usr/bin/env python3
"""
Descarga del GTFS ESTÁTICO de RENFE Cercanías (horario teórico nacional).

¿Para qué lo necesitamos?
  El feed en tiempo real (trip_updates) ya trae el retraso calculado por RENFE
  en `arrival_delay_s`: el VALOR del retraso NO se calcula aquí. Lo que el GTFS
  estático aporta es el contexto que el feed no da:
    - Resolver la línea / `route_id` (viene 100% nula en el feed) cruzando por
      núcleo de `trip_id` contra trips.txt.
    - Filtrar el núcleo de Madrid de forma estructural (route_id que empieza por
      "10T"), en vez de un bounding box geográfico aproximado.
    - Horario teórico y topología de la red (stop_times, paradas restantes,
      tramos compartidos) como features del modelo.
    - Definir la población de trenes (programados vs observados) para acotar el
      target.

  En una frase: el valor de retraso lo da el feed; su delimitación al núcleo
  Madrid y la definición de la población de trenes dependen del GTFS.

El GTFS estático es EFÍMERO: RENFE publica solo la versión vigente (ventanas de
~1 mes), así que hay que descargarlo periódicamente (1 vez/semana) y VERSIONARLO
por contenido: guardamos cada descarga solo si su sha256 cambia respecto a la
anterior. Para casar datos de RT antiguos hace falta la versión que regía ESA
semana (histórico vía Transitland / Mobility Database), no la vigente hoy.

Uso:
  python3 download_gtfs_static.py
  python3 download_gtfs_static.py --data-dir /home/tfm/data-renfe
"""
import argparse
import hashlib
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

# URL vigente del GTFS de Cercanías (descarga directa desde RENFE, CC BY 4.0).
# Corregida 08/2026: la antigua de data.renfe.com quedó obsoleta (cambio de
# producer URL avisado por Mobility Database). Si dejara de funcionar, consultar
# el feed `f-cercanias~renfe` en Transitland / Mobility Database.
GTFS_STATIC_URL = "https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip"

# Contacto público en el User-Agent (no exponer datos personales: usamos el repo).
USER_AGENT = (
    "TFM-UCM-CercaniasDelays/1.0 "
    "(proyecto academico; github.com/I-Bovingdon/Renfe-delay-prediction-pipeline)"
)


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

    # Validar que es un zip GTFS de verdad antes de guardarlo.
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
        sys.exit("ERROR: lo descargado no es un zip válido. Revisa GTFS_STATIC_URL.")

    digest = sha256(content)

    # Dedup por contenido: si es idéntico al último guardado, no versionamos otra copia.
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
