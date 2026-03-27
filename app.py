import streamlit as st
import os
import sys

# Add src to path to import local modules cleanly
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from public_salary_monitor import audit_utils
from core.config import DATA_DIR, DATASETS_CONFIG
from core.queries import (
    get_available_years,
    get_organizations,
    quick_query,
    get_last_record,
)
from ui.views import process_and_display_results

# --- CONFIGURATION ---
st.set_page_config(page_title="Visor de Transparencia", layout="wide")

# --- INTERFACE ---
st.sidebar.title("Navegación")
app_mode = st.sidebar.radio(
    "Seleccionar Modo", ["📊 Explorador de Sueldos", "🕵️ Auditoría / Anomalías"]
)

if app_mode.startswith("🕵️"):
    audit_utils.render_audit_ui(DATA_DIR, DATASETS_CONFIG)
    st.stop()  # Stop execution here

st.title("🇨🇱 Explorador de Sueldos Públicos")
st.markdown(
    "Herramienta de análisis sobre Datos Abiertos del Consejo para la Transparencia."
)

# Sidebar
st.sidebar.header("1. Configuración")
db_options = list(DATASETS_CONFIG.keys()) + ["Todas (Búsqueda Global)"]
dataset_name = st.sidebar.selectbox("Seleccionar Base de Datos", db_options)

# Logic for "Todas" vs Individual Base
if dataset_name == "Todas (Búsqueda Global)":
    st.sidebar.info(
        "Modo Global: Se buscará en Planta, Contrata y Honorarios simultáneamente."
    )
    paths_to_query = []

    # Check which files exist
    for name, info in DATASETS_CONFIG.items():
        path = os.path.join(DATA_DIR, info["filename"])
        if os.path.exists(path):
            paths_to_query.append((name, path))
        else:
            st.sidebar.warning(f"⚠️ Falta descargar/procesar: {name}")

    if not paths_to_query:
        st.error(
            "No hay bases de datos procesadas disponibles. Por favor, corre el script de sincronización."
        )
else:
    # Individual Mode
    dataset_info = DATASETS_CONFIG[dataset_name]
    file_path = os.path.join(DATA_DIR, dataset_info["filename"])
    paths_to_query = [(dataset_name, file_path)]

    if not os.path.exists(file_path):
        st.warning(
            f"El archivo '{dataset_info['filename']}' no se encuentra en el disco. Contacte al administrador para actualizar la base de datos."
        )
    else:
        st.sidebar.success("Archivo cargado y listo.")

# Dynamic Filters
st.sidebar.header("2. Filtros")
person_name_input = st.sidebar.text_input(
    "Buscar Persona", placeholder="Nombre o Apellido"
)

# Load organizations
org_list = []
if dataset_name != "Todas (Búsqueda Global)" and paths_to_query:
    with st.spinner("Indexando organismos..."):
        org_list = get_organizations(paths_to_query[0][1])
elif dataset_name == "Todas (Búsqueda Global)":
    st.sidebar.caption(
        "En búsqueda global, la lista de organismos se deshabilita por rendimiento. Escribe el nombre exacto si lo conoces."
    )

# Load available years
year_list = []
if paths_to_query:
    with st.spinner("Cargando años..."):
        year_list = get_available_years(paths_to_query[0][1])

if not year_list:
    year_list = [2026, 2025, 2024, 2023, 2022, 2021, 2020]

org_select = st.sidebar.selectbox(
    "Organismo",
    org_list,
    index=None,
    placeholder="Escribe para buscar..."
    if org_list
    else "Modo Global: Filtro opcional",
)

col1, col2 = st.sidebar.columns(2)
with col1:
    start_year_input = st.selectbox(
        "Año Inicio", year_list, index=len(year_list) - 1 if len(year_list) > 0 else 0
    )
with col2:
    end_year_input = st.selectbox("Año Fin", year_list, index=0)

month_input = st.sidebar.selectbox(
    "Mes",
    [
        "Todos",
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ],
)

# Search Button
if st.sidebar.button("Buscar", type="primary"):
    if not org_select and not person_name_input:
        st.warning(
            "⚠️ Por seguridad y rendimiento, debes ingresar al menos un Organismo O un Nombre de persona."
        )
    else:
        progress_bar = st.progress(0, text="Iniciando búsqueda...")
        result_df = quick_query(
            paths_to_query,
            org_select,
            start_year_input,
            end_year_input,
            month_input,
            person_name_input,
            limit=500,
        )
        progress_bar.empty()

        if not result_df.empty:
            if len(result_df) == 500:
                st.warning(
                    "⚠️ Se muestran los primeros 500 resultados. Por favor, refine su búsqueda."
                )

            process_and_display_results(result_df)
        else:
            st.info("No se encontraron resultados con esos filtros.")

            if person_name_input:
                with st.spinner("Buscando registros históricos..."):
                    last_record = get_last_record(paths_to_query, person_name_input)
                    if last_record:
                        st.warning(
                            f"🔍 Dato: Aunque no hay registros en la fecha seleccionada, "
                            f"el último rastro de **{person_name_input}** fue en **{last_record['mes']} {last_record['anyo']}** "
                            f"en **{last_record['organismo']}** ({last_record['origen']})."
                        )

st.sidebar.markdown("---")
st.sidebar.info(
    "Tip: Implementado con DuckDB Parquet Storage para consultas en milisegundos sobre 28GB+ de datos."
)
