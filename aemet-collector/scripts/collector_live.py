#!/usr/bin/env python3
"""
Colector AEMET EN VIVO — observación horaria.

Captura los datos de observación convencional (cada ~hora) de las estaciones
del área de Cercanías Madrid, para emparejarlos con los retrasos que se
capturan en tiempo real. Estas observaciones traen temperatura, precipitación,
viento, humedad, presión, etc. del momento.

Frecuencia recomendada: 1 vez/hora (la observación se actualiza ~horariamente;
sondear más a menudo solo gasta cuota sin aportar dato nuevo).

Endpoint usado (todas las estaciones, se filtran las de Madrid en local):
  /api/observacion/convencional/todas

Uso:
  python3 collector_live.py                 # una pasada (modo cron horario)
  python3 collector_live.py --loop           # bucle, sondea cada hora
  python3 collector_live.py --interval 3600
  python3 collector_live.py --inventario      # descarga y muestra el inventario de estaciones
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aemet_common as ac
from estaciones_madrid import ESTACIONES_MADRID


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("aemet_live")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = RotatingFileHandler(log_dir / "aemet_live.log", maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt); logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)
    return logger


def poll_once(client: ac.AemetClient, data_dir: Path, logger: logging.Logger):
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    ids_madrid = set(ESTACIONES_MADRID.keys())
    try:
        data = client.fetch("/api/observacion/convencional/todas")
    except ac.AemetAuthError:
        logger.error("CLAVE AEMET CADUCADA O INVÁLIDA. Renueva el .env. "
                     "El colector seguirá vivo y reintentará en la próxima pasada.")
        return
    except ac.AemetNoData:
        logger.warning("La observación no devolvió datos en esta pasada.")
        return

    if not isinstance(data, list):
        logger.warning("Formato inesperado de observación (se esperaba lista).")
        return

    # Filtramos a las estaciones de Madrid (cada registro trae 'idema')
    madrid = [obs for obs in data if obs.get("idema") in ids_madrid]
    ac.save_raw(madrid, data_dir, "observacion_horaria", day, stamp)
    # también guardamos el volcado nacional comprimido por si en el futuro
    # se amplía el área de estudio (decisión de procesado, no de captura)
    ac.save_raw(data, data_dir, "observacion_nacional", day, stamp)

    estaciones_con_dato = {o.get("idema") for o in madrid}
    logger.info("Observación guardada: %d registros Madrid (%d estaciones) de %d nacionales",
                len(madrid), len(estaciones_con_dato), len(data))


def descargar_inventario(client: ac.AemetClient, data_dir: Path, logger: logging.Logger):
    """Descarga el inventario de estaciones y muestra las de Madrid/Guadalajara."""
    data = client.fetch("/api/valores/climatologicos/inventarioestaciones/todasestaciones")
    out = data_dir / "inventario_estaciones.json"
    import json
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    logger.info("Inventario guardado en %s (%d estaciones)", out, len(data))
    print("\nEstaciones en provincias de Madrid (M) y Guadalajara (GU):")
    for est in data:
        if est.get("provincia", "").upper() in ("MADRID", "GUADALAJARA"):
            print(f"  {est.get('indicativo'):7} {est.get('nombre'):35} "
                  f"{est.get('provincia'):12} (lat {est.get('latitud')}, lon {est.get('longitud')})")


def main():
    parser = argparse.ArgumentParser(description="Colector AEMET en vivo (observación horaria)")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--loop", action="store_true", help="Bucle continuo")
    parser.add_argument("--interval", type=int, default=3600, help="Segundos entre pasadas (loop)")
    parser.add_argument("--inventario", action="store_true", help="Descargar inventario de estaciones y salir")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    logger = setup_logging(data_dir / "logs")
    client = ac.AemetClient(ac.load_api_key(), logger)

    if args.inventario:
        descargar_inventario(client, data_dir, logger)
        return

    if args.loop:
        logger.info("Colector AEMET en vivo iniciado. Intervalo=%ds", args.interval)
        while True:
            try:
                poll_once(client, data_dir, logger)
            except Exception:
                logger.exception("Error inesperado en la pasada (se continúa)")
            time.sleep(args.interval)
    else:
        poll_once(client, data_dir, logger)


if __name__ == "__main__":
    main()
