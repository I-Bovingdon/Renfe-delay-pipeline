#!/usr/bin/env python3
"""
Descarga HISTÓRICA — climatología diaria de AEMET (backfill).

Descarga los valores climatológicos diarios (precipitación, temperaturas
máx/mín/media, racha de viento, etc.) de las estaciones del corredor de
Cercanías Madrid para un rango de fechas amplio. Sirve para:
  - dar contexto climático de inviernos/temporales pasados,
  - y, si se consigue un histórico de retrasos que cubra el invierno,
    cruzar clima adverso con retrasos.

Restricción de la API: el endpoint por rango de fechas admite como máximo
unos 6 meses por petición. El script trocea automáticamente el rango en
ventanas de 5 meses para no chocar con ese límite.

Endpoint (por estación):
  /api/valores/climatologicos/diarios/datos/fechaini/{ini}/fechafin/{fin}/estacion/{idema}

Uso:
  python3 backfill_historico.py --desde 2020-01-01 --hasta 2025-12-31
  python3 backfill_historico.py --desde 2024-01-01 --hasta 2024-12-31 --data-dir data
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aemet_common as ac
from estaciones_madrid import ESTACIONES_MADRID

# Pausa cortés entre peticiones (AEMET limita peticiones/minuto)
PAUSA_ENTRE_PETICIONES_S = 2
VENTANA_DIAS = 150  # ~5 meses por petición (límite ~6 meses)


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("aemet_backfill")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = RotatingFileHandler(log_dir / "aemet_backfill.log", maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt); logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)
    return logger


def ventanas(desde: datetime, hasta: datetime):
    """Trocea [desde, hasta] en ventanas de VENTANA_DIAS días."""
    actual = desde
    while actual <= hasta:
        fin = min(actual + timedelta(days=VENTANA_DIAS - 1), hasta)
        yield actual, fin
        actual = fin + timedelta(days=1)


def fmt_fecha(d: datetime) -> str:
    # Formato exigido por AEMET: AAAA-MM-DDTHH:MM:SSUTC
    return d.strftime("%Y-%m-%dT%H:%M:%SUTC")


def main():
    parser = argparse.ArgumentParser(description="Backfill histórico climatología diaria AEMET")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--desde", required=True, help="Fecha inicial AAAA-MM-DD")
    parser.add_argument("--hasta", required=True, help="Fecha final AAAA-MM-DD")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    logger = setup_logging(data_dir / "logs")
    client = ac.AemetClient(ac.load_api_key(), logger)

    desde = datetime.strptime(args.desde, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    hasta = datetime.strptime(args.hasta, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    total_ok, total_vacio, total_error = 0, 0, 0
    for idema, (nombre, _) in ESTACIONES_MADRID.items():
        logger.info("=== Estación %s (%s) ===", idema, nombre)
        for ini, fin in ventanas(desde, hasta):
            endpoint = (f"/api/valores/climatologicos/diarios/datos"
                        f"/fechaini/{fmt_fecha(ini)}/fechafin/{fmt_fecha(fin)}/estacion/{idema}")
            etiqueta = f"{idema}_{ini.strftime('%Y%m%d')}_{fin.strftime('%Y%m%d')}"
            try:
                data = client.fetch(endpoint)
                ac.save_raw(data, data_dir, "climatologia_diaria",
                            ini.strftime("%Y-%m"), etiqueta)
                n = len(data) if isinstance(data, list) else 0
                logger.info("  %s -> %d días", etiqueta, n)
                total_ok += 1
            except ac.AemetNoData:
                logger.info("  %s -> sin datos (estación sin registro en ese rango)", etiqueta)
                total_vacio += 1
            except ac.AemetAuthError:
                logger.error("CLAVE CADUCADA durante el backfill. Renueva el .env y relanza "
                             "(las ventanas ya guardadas no se repiten si usas --reanudar manualmente).")
                return
            except Exception:
                logger.exception("  %s -> ERROR (se continúa con la siguiente ventana)", etiqueta)
                total_error += 1
            time.sleep(PAUSA_ENTRE_PETICIONES_S)

    logger.info("Backfill terminado. Ventanas OK=%d, vacías=%d, con error=%d",
                total_ok, total_vacio, total_error)


if __name__ == "__main__":
    main()
