#!/usr/bin/env python3
"""
Cliente común para AEMET OpenData.

La API de AEMET es "de dos saltos":
  1ª llamada al endpoint  -> devuelve un JSON con un campo 'datos' (una URL)
  2ª llamada a esa URL     -> devuelve los datos reales

Además:
  - La API Key se pasa como cabecera 'api_key' (NUNCA incrustada en el código).
  - Códigos relevantes: 200 OK | 401/403 clave inválida o caducada |
    404 sin datos para la petición | 429 límite de peticiones superado.
  - Los textos vienen en ISO-8859-15 (latin-9), no en UTF-8: hay que decodificar
    explícitamente o aparecerán caracteres corruptos en las tildes.

Este módulo no se ejecuta solo; lo importan collector_live.py y backfill_historico.py.
"""

import gzip
import json
import logging
import os
import time
from pathlib import Path

import requests

BASE_URL = "https://opendata.aemet.es/opendata"

# Errores que conviene distinguir para reaccionar distinto
class AemetAuthError(Exception):
    """401/403: API Key caducada o inválida. El operador debe renovarla."""


class AemetRateLimit(Exception):
    """429: límite de peticiones superado. Hay que esperar y reintentar."""


class AemetNoData(Exception):
    """404: la petición es válida pero no hay datos (p.ej. fecha sin registro)."""


def load_api_key() -> str:
    """Lee la API Key de la variable de entorno AEMET_API_KEY o de un fichero .env."""
    key = os.environ.get("AEMET_API_KEY")
    if key:
        return key.strip()
    # fallback: fichero .env junto a la raíz del proyecto (NO versionado en Git)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("AEMET_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError(
        "No se encontró la API Key. Define la variable de entorno AEMET_API_KEY "
        "o crea un fichero .env con la línea: AEMET_API_KEY=tu_clave"
    )


class AemetClient:
    def __init__(self, api_key: str, logger: logging.Logger,
                 timeout: int = 30, max_retries: int = 3, backoff: int = 10):
        self.api_key = api_key
        self.logger = logger
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = requests.Session()
        self.session.headers.update({
            "api_key": api_key,
            "Accept": "application/json",
            "User-Agent": "TFM-UCM-CercaniasDelays/1.0 (proyecto academico)",
        })

    def _decode(self, resp: requests.Response) -> str:
        """AEMET sirve los datos en latin-9; forzamos la decodificación correcta."""
        resp.encoding = "ISO-8859-15"
        return resp.text

    def fetch(self, endpoint: str) -> list | dict:
        """
        Ejecuta el patrón de dos saltos para un endpoint relativo
        (p.ej. '/api/valores/climatologicos/diarios/datos/...').
        Devuelve los datos reales ya parseados.
        """
        # --- 1er salto: pedir la URL de los datos ---
        meta = self._request_json(BASE_URL + endpoint, paso="1 (metadatos)")
        if not isinstance(meta, dict) or "datos" not in meta:
            estado = meta.get("estado") if isinstance(meta, dict) else "?"
            desc = meta.get("descripcion") if isinstance(meta, dict) else meta
            if estado == 404:
                raise AemetNoData(f"Sin datos: {desc}")
            raise RuntimeError(f"Respuesta inesperada en el 1er salto: estado={estado} desc={desc}")

        datos_url = meta["datos"]
        # --- 2º salto: descargar los datos reales ---
        # Esta URL ya no necesita api_key, pero la sesión la lleva igualmente sin daño
        data = self._request_json(datos_url, paso="2 (datos)")
        return data

    def _request_json(self, url: str, paso: str):
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:
                self.logger.warning("Salto %s, intento %d/%d: error de red %s",
                                    paso, attempt, self.max_retries, exc)
                if attempt < self.max_retries:
                    time.sleep(self.backoff)
                    continue
                raise

            if resp.status_code in (401, 403):
                raise AemetAuthError(
                    f"AEMET devolvió {resp.status_code}: API Key caducada o inválida. "
                    "Renueva la clave en el portal y actualiza el fichero .env."
                )
            if resp.status_code == 429:
                wait = self.backoff * attempt * 2
                self.logger.warning("429 (límite). Esperando %ds antes de reintentar...", wait)
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                # devolvemos el cuerpo para que fetch() lo interprete como NoData
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    return {"estado": 404, "descripcion": "sin datos"}

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                self.logger.warning("Salto %s, intento %d/%d: HTTP %s",
                                    paso, attempt, self.max_retries, exc)
                if attempt < self.max_retries:
                    time.sleep(self.backoff)
                    continue
                raise

            text = self._decode(resp)
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                self.logger.warning("Salto %s: JSON inválido (%s). Reintentando...", paso, exc)
                if attempt < self.max_retries:
                    time.sleep(self.backoff)
                    continue
                raise
        raise RuntimeError(f"Agotados los reintentos en el salto {paso} para {url}")


def save_raw(data, data_dir: Path, feed: str, day: str, stamp: str) -> Path:
    """Guarda la respuesta cruda comprimida, particionada por feed y día."""
    out_dir = data_dir / "raw" / feed / day
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{feed}_{stamp}.json.gz"
    record = {"fetched_at_stamp": stamp, "feed": feed, "payload": data}
    tmp = out_path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False, separators=(",", ":"))
    tmp.rename(out_path)
    return out_path
