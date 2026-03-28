import streamlit as st
import duckdb
import pandas as pd
import os
import time
import json
import requests
import unicodedata
from src.core.config import METADATA_FILE
from src.core.logger import get_logger

logger = get_logger()


def unaccent_lower_python(text: str) -> str:
    """Normalizes string to match search vector."""
    if not text:
        return ""
    text = text.lower()
    # Handle decomposed characters (NFD) like Mac's "ñ" (n + \u0303)
    # and strip all accents safely
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")


def load_cache() -> dict:
    """Loads metadata from JSON if it exists locally, otherwise fetches from GitHub."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)

    # Try fetching from remote release URL
    from src.core.config import GITHUB_RELEASE_BASE_URL

    remote_url = f"{GITHUB_RELEASE_BASE_URL}/metadata_cache.json"
    try:
        response = requests.get(remote_url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass

    return {}


@st.cache_data(show_spinner=False, persist=True)
def get_available_years(file_path) -> list:
    """Gets available years strictly from cache."""
    base_name = (
        os.path.basename(file_path)
        if isinstance(file_path, str)
        else "Todas (Búsqueda Global)"
    )
    lookup_name = (
        base_name.replace(".parquet", ".csv")
        if base_name != "Todas (Búsqueda Global)"
        else base_name
    )

    global_cache = load_cache()
    if lookup_name in global_cache and "anios" in global_cache[lookup_name]:
        return global_cache[lookup_name]["anios"]

    return [2026, 2025, 2024, 2023, 2022, 2021, 2020]


@st.cache_data(show_spinner=False, persist=True)
def get_organizations(file_path, _version=3) -> list:
    """Gets organizations strictly from cache."""
    base_name = (
        os.path.basename(file_path)
        if isinstance(file_path, str)
        else "Todas (Búsqueda Global)"
    )
    lookup_name = (
        base_name.replace(".parquet", ".csv")
        if base_name != "Todas (Búsqueda Global)"
        else base_name
    )

    global_cache = load_cache()
    if lookup_name in global_cache and "organismos" in global_cache[lookup_name]:
        return global_cache[lookup_name]["organismos"]

    return []


def quick_query(
    paths_to_query,
    organization,
    start_year,
    end_year,
    month=None,
    person_name=None,
    limit=500,
):
    """Queries the filtered data using Parquet with UNION ALL and Pagination."""
    start_time = time.time()
    conditions = []
    query_params = []

    if organization:
        conditions.append("organismo_nombre = ?")
        query_params.append(organization)

    if person_name:
        name_clean = unaccent_lower_python(person_name.strip())
        words = name_clean.split()
        for word in words:
            conditions.append("search_vector LIKE ?")
            query_params.append(f"%{word}%")

    if start_year and end_year:
        try:
            start = int(start_year)
            end = int(end_year)
            conditions.append("anyo BETWEEN ? AND ?")
            query_params.extend([start, end])
        except Exception:
            pass

    if month and month != "Todos":
        conditions.append("Mes = ?")
        query_params.append(month)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    selects = []
    for source_name, source_path in paths_to_query:
        columns = [
            "organismo_nombre",
            "anyo",
            "Mes",
            "estamento",
            "Nombres",
            "Paterno",
            "Materno",
            "cargo",
            "remuliquida_mensual",
            "remuneracionbruta_mensual",
            "origen",
        ]
        cols_str = ", ".join(columns)

        selects.append(f"""
            SELECT {cols_str}
            FROM read_parquet('{source_path}')
            WHERE {where_clause}
        """)

    final_query = " UNION ALL ".join(selects)
    final_query += f" LIMIT {limit}"

    full_params = query_params * len(paths_to_query)

    logger.info(
        "fetching parquet chunks via duckdb httpfs",
        extra={
            "sources_count": len(paths_to_query),
            "urls": [source_path for _, source_path in paths_to_query],
        },
    )

    try:
        df = duckdb.query(final_query, params=full_params).to_df()
        duration = time.time() - start_time
        logger.info(
            "search query completed",
            extra={
                "duration": round(duration, 5),
                "org": organization if organization else "all",
                "person": person_name if person_name else "none",
                "years": f"{start_year}-{end_year}",
                "month": month if month else "all",
                "rows": len(df),
                "status": "success",
            },
        )
        return df
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            "search query failed",
            extra={
                "duration": round(duration, 5),
                "org": organization if organization else "all",
                "person": person_name if person_name else "none",
                "error": str(e).replace("\n", " "),
                "status": "error",
            },
        )
        st.error(f"Error en la consulta: {e}")
        return pd.DataFrame()


def get_last_record(paths_to_query, person_name):
    """Searches for the last available record of a person ignoring date filters."""
    start_time = time.time()
    try:
        name_clean = unaccent_lower_python(person_name.strip())
        words = name_clean.split()

        conditions = []
        query_params = []
        for word in words:
            conditions.append("search_vector LIKE ?")
            query_params.append(f"%{word}%")

        where_name = " AND ".join(conditions)

        selects = []
        for source_name, path in paths_to_query:
            selects.append(f"""
            SELECT anyo, Mes, organismo_nombre, origen
            FROM read_parquet('{path}')
            WHERE {where_name}
            """)

        final_query = " UNION ALL ".join(selects) + " ORDER BY anyo DESC LIMIT 1"
        full_params = query_params * len(paths_to_query)

        logger.info(
            "fetching parquet chunks via duckdb httpfs (last record)",
            extra={
                "sources_count": len(paths_to_query),
                "urls": [source_path for _, source_path in paths_to_query],
            },
        )

        df = duckdb.query(final_query, params=full_params).to_df()
        duration = time.time() - start_time

        if not df.empty:
            reg = df.iloc[0]
            logger.info(
                "last record query completed",
                extra={
                    "duration": round(duration, 5),
                    "person": person_name,
                    "found": True,
                    "status": "success",
                },
            )
            return {
                "origen": reg["origen"],
                "organismo": reg["organismo_nombre"],
                "anyo": reg["anyo"],
                "mes": reg["Mes"],
            }

        logger.info(
            "last record query completed",
            extra={
                "duration": round(duration, 5),
                "person": person_name,
                "found": False,
                "status": "success",
            },
        )
        return None

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            "last record query failed",
            extra={
                "duration": round(duration, 5),
                "person": person_name,
                "error": str(e).replace("\n", " "),
                "status": "error",
            },
        )
        return None
