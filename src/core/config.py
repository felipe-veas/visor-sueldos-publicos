import os

DATA_DIR = "data"
METADATA_FILE = os.path.join(DATA_DIR, "metadata_cache.json")

# Configuration for the datasets
# Keys are displayed in the UI dropdown
DATASETS_CONFIG = {
    "Personal de Planta": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalPlanta.csv",
        "filename": "TA_PersonalPlanta.parquet",
    },
    "Personal a Contrata": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContrata.csv",
        "filename": "TA_PersonalContrata.parquet",
    },
    "Personal a Honorarios": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContratohonorarios.csv",
        "filename": "TA_PersonalContratohonorarios.parquet",
    },
}

MONTHS_MAP = {
    "Enero": 1,
    "Febrero": 2,
    "Marzo": 3,
    "Abril": 4,
    "Mayo": 5,
    "Junio": 6,
    "Julio": 7,
    "Agosto": 8,
    "Septiembre": 9,
    "Octubre": 10,
    "Noviembre": 11,
    "Diciembre": 12,
}
