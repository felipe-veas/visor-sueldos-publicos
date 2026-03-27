import streamlit as st
import duckdb
import pandas as pd
import os
import json
from src.core.config import METADATA_FILE


def unaccent_lower_python(text: str) -> str:
    """Normalizes string to match search vector."""
    text = text.lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "à": "a",
        "è": "e",
        "ì": "i",
        "ò": "o",
        "ù": "u",
        "ä": "a",
        "ë": "e",
        "ï": "i",
        "ö": "o",
        "ü": "u",
        "ñ": "n",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def load_cache() -> dict:
    """Loads metadata from JSON if it exists."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
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

    try:
        return duckdb.query(final_query, params=full_params).to_df()
    except Exception as e:
        st.error(f"Error en la consulta: {e}")
        return pd.DataFrame()


def get_last_record(paths_to_query, person_name):
    """Searches for the last available record of a person ignoring date filters."""
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

        df = duckdb.query(final_query, params=full_params).to_df()

        if not df.empty:
            reg = df.iloc[0]
            return {
                "origen": reg["origen"],
                "organismo": reg["organismo_nombre"],
                "anyo": reg["anyo"],
                "mes": reg["Mes"],
            }
        return None

    except Exception:
        return None
