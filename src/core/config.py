import os

DATA_DIR = "data"

# Instead of local files, the app now points directly to the static GitHub Release URLs.
# This makes the web application 100% stateless (requires zero disk space).
# GitHub Releases allow up to 2GB per file and infinite bandwidth.
GITHUB_RELEASE_BASE_URL = (
    "https://github.com/fveas/visor-sueldos-publicos/releases/download/latest-data"
)

METADATA_FILE = os.path.join(DATA_DIR, "metadata_cache.json")


# Used only for local overrides if the file exists locally (for development/testing)
def resolve_data_path(filename: str) -> str:
    local_path = os.path.join(DATA_DIR, filename)
    if os.path.exists(local_path):
        return local_path
    return f"{GITHUB_RELEASE_BASE_URL}/{filename}"


# Configuration for the datasets
# Keys are displayed in the UI dropdown
DATASETS_CONFIG = {
    "Personal de Planta": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalPlanta.csv",
        "filename": "TA_PersonalPlanta.parquet",
        "path": resolve_data_path("TA_PersonalPlanta.parquet"),
    },
    "Personal a Contrata": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContrata.csv",
        "filename": "TA_PersonalContrata.parquet",
        "path": resolve_data_path("TA_PersonalContrata.parquet"),
    },
    "Personal a Honorarios": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContratohonorarios.csv",
        "filename": "TA_PersonalContratohonorarios.parquet",
        "path": resolve_data_path("TA_PersonalContratohonorarios.parquet"),
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
