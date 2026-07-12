#!/usr/bin/env python3
"""
Estaciones AEMET del area de influencia de Cercanias Madrid.

VERSION CORREGIDA contra el inventario real de AEMET (verificado con
collector_live.py --inventario). Cada indicativo de esta lista se ha
confirmado que existe y devuelve datos.

Cada estacion: idema -> (nombre legible, motivo de inclusion / corredor).
Si alguna dejara de devolver datos, se elimina sin afectar al resto:
el colector trata cada estacion de forma independiente.
"""

ESTACIONES_MADRID = {
    # --- Nucleo urbano de Madrid ---
    "3195":  ("Madrid - Retiro", "Estacion historica de referencia (centro)"),
    "3129":  ("Madrid - Aeropuerto (Barajas)", "NE / corredor C-1"),
    "3196":  ("Madrid - Cuatro Vientos", "SO del area metropolitana"),
    "3194U": ("Madrid - Ciudad Universitaria", "Centro-NO"),
    "3126Y": ("Madrid - El Goloso", "N de Madrid (corredor C-4)"),

    # --- Corredor norte (C-3 / C-4 / C-8) ---
    "3125Y": ("San Sebastian de los Reyes", "Corredor C-4 norte"),
    "3191E": ("Colmenar Viejo", "Corredor C-4 norte (sierra)"),
    "3338":  ("Robledo de Chavela", "NO / area sierra (C-8/C-10)"),
    "3330Y": ("Las Rozas de Puerto Real", "NO corredor (C-8/C-10)"),

    # --- Corredor este / Henares (C-2 / C-7) ---
    "3170Y": ("Alcala de Henares", "Corredor C-2 / C-7 (Henares)"),
    "3175":  ("Torrejon de Ardoz", "Corredor C-2 / C-7 (Henares)"),
    "3168D": ("Guadalajara", "Corredor C-2 / C-7 este (extremo Henares)"),

    # --- Corredor sur (C-3 / C-4 / C-5) ---
    "3110C": ("Getafe", "Corredor C-3 / C-4 sur"),
    "3200":  ("Aranjuez", "Corredor C-3 sur (extremo)"),

    # --- Corredor SE (C-2 / C-7 sur) ---
    "3182Y": ("Arganda del Rey", "SE del area metropolitana"),
}

# Provincias para la climatologia diaria historica (estaciones del corredor
# repartidas entre Madrid (28) y Guadalajara (19)).
PROVINCIAS_HISTORICO = ["28", "19"]

# Codigo de area para los avisos meteorologicos adversos (CAP)
AREA_AVISOS_MADRID = "72"
