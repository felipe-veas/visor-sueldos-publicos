import streamlit as st
import os
import sys

# Add src to path to import local modules cleanly
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from audits import audit_utils
from core.config import DATA_DIR, DATASETS_CONFIG
from core.queries import (
    get_available_years,
    quick_query,
    get_last_record,
)
from ui.views import process_and_display_results
from src.core.logger import get_logger

logger = get_logger()

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Visor de Transparencia", page_icon=":material/search:", layout="wide"
)

# CSS Injection for UI Polish
st.markdown(
    """
    <style>
    /* Hide the 'open' text artifact from Streamlit selectboxes */
    div[data-baseweb="select"] span[style*="position: absolute"] {
        display: none !important;
    }
    /* Increase padding in KPI metric cards */
    div[data-testid="stMetric"] {
        padding: 1rem;
        background-color: #1e293b; /* Tailwind slate-800 */
        border-radius: 0.5rem;
        border: 1px solid #334155; /* Tailwind slate-700 */
    }
    /* Ensure WCAG contrast for gray text */
    p, span, label, div[data-testid="stMarkdownContainer"] {
        color: #e2e8f0; /* Tailwind slate-200 */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Log connection only once per session
if "session_id" not in st.session_state:
    import uuid

    st.session_state.session_id = str(uuid.uuid4())
    logger.info(
        "new session started", extra={"session_id": st.session_state.session_id}
    )

# --- INTERFACE ---
with st.sidebar:
    st.markdown(
        """
        <div style="text-align: center; padding-bottom: 1rem;">
            <h1 style="margin-bottom: 0;">Navegación</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )

    app_mode = st.radio(
        "Seleccionar Modo",
        [
            ":material/bar_chart: Explorador de Sueldos",
            ":material/policy: Auditoría / Anomalías",
        ],
        label_visibility="collapsed",
    )

    st.markdown("---")

if app_mode.startswith(":material/policy:"):
    audit_utils.render_audit_ui(DATA_DIR, DATASETS_CONFIG)
    st.stop()  # Stop execution here

st.title(":material/flag: Explorador de Sueldos Públicos")
st.markdown(
    "Herramienta de análisis sobre Datos Abiertos del Consejo para la Transparencia."
)

# Sidebar
with st.sidebar:
    st.subheader("Filtros")

    # Always use Global Mode
    dataset_name = "Todas (Búsqueda Global)"
    paths_to_query = []

    # Check which files exist
    for name, info in DATASETS_CONFIG.items():
        try:
            from pathlib import Path

            file_path_str = info.get("path", "")

            if str(file_path_str).startswith("http"):
                paths_to_query.append((name, file_path_str))
            else:
                path = Path(file_path_str).resolve()
                if not str(path).startswith(str(Path(DATA_DIR).resolve())):
                    raise ValueError(
                        f"Invalid path traversal attempted: {info['filename']}"
                    )

                if path.exists():
                    paths_to_query.append((name, str(path)))
                else:
                    st.warning(
                        f":material/warning: Falta descargar/procesar localmente: {name}"
                    )
        except Exception as e:
            st.error(f"Error cargando base de datos {name}: {e}")

    if not paths_to_query:
        st.error(
            "No hay bases de datos procesadas disponibles. Por favor, corre el script de sincronización."
        )

    person_name_input = st.text_input("Buscar Persona", placeholder="Ej: Juan Pérez")

# Load available years
year_list = []
if paths_to_query:
    with st.spinner("Cargando años..."):
        year_list = get_available_years(paths_to_query[0][1])

if not year_list:
    year_list = [2026, 2025, 2024, 2023, 2022, 2021, 2020]

with st.sidebar:
    # Ensure year_list is sorted
    sorted_years = sorted(year_list)
    min_year = min(sorted_years)
    max_year = max(sorted_years)

    # Toggle for single year vs range
    use_year_range = st.toggle("Buscar en un rango de años", value=False)

    if use_year_range:
        # Ensure the default value is an actual range, not a single year
        default_range = (
            (min_year, max_year) if min_year != max_year else (min_year, min_year)
        )

        year_range = st.slider(
            "Rango de Años",
            min_value=min_year,
            max_value=max_year,
            value=default_range,
            step=1,
        )
        start_year_input, end_year_input = year_range

        # Validate minimum range of 1 year
        is_invalid_range = start_year_input == end_year_input
        if is_invalid_range:
            st.error(
                ":material/error: El rango mínimo debe ser de al menos 1 año. Para buscar un solo año, desactiva el interruptor superior."
            )

        # If querying a range of years, force 'Todos' to avoid confusing results
        month_input = "Todos"
        st.caption("Filtro de mes deshabilitado al buscar en múltiples años.")
    else:
        # Default: Single year selection
        selected_year = st.selectbox(
            "Año",
            options=sorted_years,
            index=len(sorted_years) - 1 if len(sorted_years) > 0 else 0,
        )
        start_year_input = selected_year
        end_year_input = selected_year
        is_invalid_range = False

        month_input = st.selectbox(
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

    st.markdown("<br>", unsafe_allow_html=True)

    # Disable search button if no name is provided OR if the year range is invalid
    is_search_disabled = is_invalid_range or not person_name_input.strip()

    search_clicked = st.button(
        "Buscar", type="primary", use_container_width=True, disabled=is_search_disabled
    )

# Search Button
if search_clicked:
    logger.info(
        "search requested",
        extra={
            "org": "all",
            "person": person_name_input,
            "dataset": "Global (Filtered)",
        },
    )
    progress_bar = st.progress(
        0, text="Descargando y consultando datos remotos vía DuckDB..."
    )
    result_df = quick_query(
        paths_to_query,
        None,  # org_select is now always None
        start_year_input,
        end_year_input,
        month_input,
        person_name_input,
        limit=500,
    )
    progress_bar.empty()
    st.session_state["search_results"] = result_df
    st.session_state["last_person"] = person_name_input

# Always display results if they exist in session state
if "search_results" in st.session_state:
    result_df = st.session_state["search_results"]
    last_person_input = st.session_state.get("last_person", "")

    if not result_df.empty:
        if len(result_df) == 500:
            st.warning(
                ":material/warning: Se muestran los primeros 500 resultados. Por favor, refine su búsqueda."
            )

        process_and_display_results(result_df)
    else:
        st.info("No se encontraron resultados con esos filtros.")

        if last_person_input:
            with st.spinner(
                "Buscando registros históricos (descargando bloques remotos)..."
            ):
                last_record = get_last_record(paths_to_query, last_person_input)
                if last_record:
                    st.warning(
                        f":material/search: Dato: Aunque no hay registros en la fecha seleccionada, "
                        f"el último rastro de **{last_person_input}** fue en **{last_record['mes']} {last_record['anyo']}** "
                        f"en **{last_record['organismo']}** ({last_record['origen']})."
                    )

with st.sidebar:
    st.markdown("---")
    st.info(
        "Tip: Implementado con DuckDB Parquet Storage para consultas en milisegundos sobre 28GB+ de datos."
    )
