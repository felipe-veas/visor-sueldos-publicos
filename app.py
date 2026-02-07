import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os
import sys
import json
import requests
import datetime
import unicodedata

# Add src to path to import local modules
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from public_salary_monitor import audit_utils  # Audit module

# --- CONFIGURATION ---
st.set_page_config(page_title="Visor de Transparencia", layout="wide")

DATA_DIR = "data"
# Configuration for the datasets
# Keys are displayed in the UI dropdown
DATASETS_CONFIG = {
    "Personal de Planta": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalPlanta.csv",
        "filename": "TA_PersonalPlanta.csv",
    },
    "Personal a Contrata": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContrata.csv",
        "filename": "TA_PersonalContrata.csv",
    },
    "Personal a Honorarios": {
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContratohonorarios.csv",
        "filename": "TA_PersonalContratohonorarios.csv",
    },
}

METADATA_FILE = os.path.join(DATA_DIR, "metadata_cache.json")


# --- FUNCTIONS ---


def download_file(dataset_info):
    """Downloads the file if it doesn't exist."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    file_path = os.path.join(DATA_DIR, dataset_info["filename"])

    if os.path.exists(file_path):
        return True, "Archivo ya existe."

    try:
        # Headers to simulate a real browser and avoid 403 blocks
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }

        # Progress bar
        progress_text = "Iniciando descarga..."
        progress_bar = st.progress(0, text=progress_text)

        response = requests.get(dataset_info["url"], headers=headers, stream=True)
        response.raise_for_status()

        # Check if response is HTML
        content_type = response.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            return (
                False,
                "Bloqueo detectado: El servidor devolvió una web en lugar del CSV.",
            )

        total_size = int(response.headers.get("content-length", 0))
        downloaded_size = 0

        with open(file_path, "wb") as file:
            for data in response.iter_content(1024 * 1024):  # 1MB chunks
                file.write(data)
                downloaded_size += len(data)

                # Update bar
                if total_size > 0:
                    percent = downloaded_size / total_size
                    mb_downloaded = downloaded_size / (1024 * 1024)
                    mb_total = total_size / (1024 * 1024)

                    # Avoid progress > 1.0 error
                    percent = min(percent, 1.0)

                    progress_bar.progress(
                        percent,
                        text=f"Descargando: {mb_downloaded:.1f} MB / {mb_total:.1f} MB ({percent:.1%})",
                    )

        progress_bar.empty()  # Clear bar when done
        return True, "Descarga completada exitosamente."
    except Exception as e:
        return False, str(e)


def save_cache(cache_data):
    """Saves metadata to a JSON file for instant loading."""
    with open(METADATA_FILE, "w") as f:
        json.dump(cache_data, f)


def load_cache():
    """Loads metadata from JSON if it exists."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}


@st.cache_data(show_spinner=False, persist=True)
def get_available_years(file_path):
    """Gets available years, using disk cache if possible."""

    # 1. Try loading from our custom JSON cache
    base_name = os.path.basename(file_path)
    global_cache = load_cache()

    if base_name in global_cache and "anios" in global_cache[base_name]:
        return global_cache[base_name]["anios"]

    # 2. If not in cache, calculate with DuckDB (Slow the first time)
    try:
        current_year = datetime.date.today().year

        query = f"""
        SELECT DISTINCT anyo
        FROM read_csv('{file_path}', delim=';', encoding='latin-1', ignore_errors=true, null_padding=true)
        WHERE TRY_CAST(anyo AS INTEGER) BETWEEN 2000 AND {current_year + 1}
        ORDER BY anyo DESC
        """
        df = duckdb.query(query).to_df()
        year_list = sorted(df["anyo"].astype(int).tolist(), reverse=True)

        # 3. Save to cache for next time
        global_cache[base_name] = global_cache.get(base_name, {})
        global_cache[base_name]["anios"] = year_list
        save_cache(global_cache)

        return year_list
    except Exception:
        return [2026, 2025, 2024, 2023, 2022, 2021, 2020]


@st.cache_data(show_spinner=False, persist=True)
def get_organizations(file_path, _version=3):
    """Gets organizations with persistent cache."""

    base_name = os.path.basename(file_path)
    global_cache = load_cache()

    if base_name in global_cache and "organismos" in global_cache[base_name]:
        return global_cache[base_name]["organismos"]

    try:
        query = f"""
        SELECT DISTINCT organismo_nombre
        FROM read_csv('{file_path}', delim=';', encoding='latin-1', ignore_errors=true, null_padding=true)
        ORDER BY organismo_nombre
        """
        df = duckdb.query(query).to_df()
        org_list = df["organismo_nombre"].tolist()

        # Save to cache
        global_cache[base_name] = global_cache.get(base_name, {})
        global_cache[base_name]["organismos"] = org_list
        save_cache(global_cache)

        return org_list
    except Exception as e:
        st.error(f"Error leyendo organismos: {e}")
        return []


def quick_query(
    file_path, organization, start_year, end_year, month=None, person_name=None
):
    """Queries the filtered data."""

    # Build WHERE clause
    conditions = []

    # 1. Organization Filter
    if organization:
        org_sql = organization.replace("'", "''")
        conditions.append(f"organismo_nombre = '{org_sql}'")

    # 2. Person Name Filter (Smart Concatenated Search)
    if person_name:
        # Clean input
        name_clean = person_name.strip().replace("'", "''")

        # Generate variants to handle inconsistencies (Ñ/N and Accents)
        def normalize(text):
            return "".join(
                c
                for c in unicodedata.normalize("NFD", text)
                if unicodedata.category(c) != "Mn"
            )

        variants = set()
        variants.add(name_clean)
        variants.add(normalize(name_clean))
        if "n" in name_clean.lower():
            variants.add(name_clean.replace("n", "ñ").replace("N", "Ñ"))

        # Build conditions: Search in the CONCATENATION of fields
        or_conditions = []
        for variant in variants:
            filter_concat = f"""
                (COALESCE(Nombres, '') || ' ' || COALESCE(Paterno, '') || ' ' || COALESCE(Materno, '')) ILIKE '%{variant}%'
            """
            or_conditions.append(filter_concat)

        conditions.append(f"({' OR '.join(or_conditions)})")

    # 3. Year Range Filter
    if start_year and end_year:
        # Try converting to int to use BETWEEN safely
        try:
            start = int(start_year)
            end = int(end_year)
            # Safe CAST to integer
            conditions.append(
                f"CAST(TRY_CAST(anyo AS INTEGER) AS INTEGER) BETWEEN {start} AND {end}"
            )
        except Exception:
            pass  # If non-numeric text entered, ignore filter

    if month and month != "Todos":
        conditions.append(f"Mes = '{month}'")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # --- DYNAMIC COLUMN LOGIC ---
    # 1. Get real columns in THIS specific file
    try:
        schema_query = f"SELECT * FROM read_csv('{file_path}', delim=';', encoding='latin-1', ignore_errors=true) LIMIT 0"
        schema_df = duckdb.query(schema_query).to_df()
        real_columns = set(schema_df.columns)
    except Exception as e:
        st.error(f"No se pudo leer la estructura del archivo: {e}")
        return pd.DataFrame()

    # 2. Define search priorities for each concept
    # The system will use the first column found from each list
    # Keys are the alias used in the SQL query (result columns)
    concept_mapping = {
        "organismo_nombre": ["organismo_nombre", "Organismo", "Institucion"],
        "anyo": ["anyo", "Año", "Year"],
        "Mes": ["Mes", "mes", "Month"],
        "estamento": [
            "Tipo Estamento",
            "tipo_calificacionp",
            "estamento",
            "Calificacion Profesional",
            "Tipo Calificacion Profesional",
        ],
        "Nombres": ["Nombres", "nombres", "Nombre"],
        "Paterno": ["Paterno", "paterno", "Apellido Paterno"],
        "Materno": ["Materno", "materno", "Apellido Materno"],
        "cargo": ["Tipo cargo", "descripcion_funcion", "Cargo", "Funcion", "Grado EUS"],
        # For net salary, try liquid first, if not (honorarios), use gross
        "remuliquida_mensual": [
            "remuliquida_mensual",
            "remuneracionbruta",
            "Sueldo Liquido",
            "Honorario Bruto",
        ],
        "remuneracionbruta_mensual": [
            "remuneracionbruta_mensual",
            "remuneracionbruta",
            "Sueldo Bruto",
        ],
    }

    selected_columns = []

    for concept, candidates in concept_mapping.items():
        found_column = "NULL"  # Default if nothing found
        for candidate in candidates:
            if candidate in real_columns:
                found_column = f'"{candidate}"'  # Quotes to handle spaces
                break

        # Select as concept alias
        selected_columns.append(f"{found_column} AS {concept}")

    select_clause = ", ".join(selected_columns)

    # Main query with dynamic columns
    query = f"""
    SELECT {select_clause}
    FROM read_csv('{file_path}', delim=';', encoding='latin-1', ignore_errors=true, null_padding=true)
    WHERE {where_clause}
    """

    # Execute query
    try:
        # DuckDB is very efficient. We can bring ~50k rows to Pandas without issue.
        df = duckdb.query(query).to_df()
        return df
    except Exception as e:
        st.error(f"Error en la consulta: {e}")
        return pd.DataFrame()


def get_last_record(paths_to_query, person_name):
    """Searches for the last available record of a person ignoring date filters."""
    try:
        # Reuse normalization logic
        def normalize(text):
            return "".join(
                c
                for c in unicodedata.normalize("NFD", text)
                if unicodedata.category(c) != "Mn"
            )

        name_clean = person_name.strip().replace("'", "''")
        variants = set([name_clean, normalize(name_clean)])
        if "n" in name_clean.lower():
            variants.add(name_clean.replace("n", "ñ").replace("N", "Ñ"))

        or_conditions = []
        for variant in variants:
            filter_concat = f"(COALESCE(Nombres, '') || ' ' || COALESCE(Paterno, '') || ' ' || COALESCE(Materno, '')) ILIKE '%{variant}%'"
            or_conditions.append(filter_concat)

        where_name = f"({' OR '.join(or_conditions)})"

        # Iterate over available databases to find MAX(date)
        last_found = None

        for source_name, path in paths_to_query:
            # Lightweight query just to get MAX year/month
            query = f"""
            SELECT anyo, Mes, organismo_nombre
            FROM read_csv('{path}', delim=';', encoding='latin-1', ignore_errors=true, null_padding=true)
            WHERE {where_name}
            ORDER BY anyo DESC
            LIMIT 1
            """
            df = duckdb.query(query).to_df()

            if not df.empty:
                reg = df.iloc[0]
                # If we find a more recent record, save it
                if last_found is None or reg["anyo"] >= last_found["anyo"]:
                    last_found = {
                        "origen": source_name,
                        "organismo": reg["organismo_nombre"],
                        "anyo": reg["anyo"],
                        "mes": reg["Mes"],
                    }

        return last_found

    except Exception:
        return None


# --- INTERFACE ---

st.sidebar.title("Navegación")
app_mode = st.sidebar.radio(
    "Seleccionar Modo", ["📊 Explorador de Sueldos", "🕵️ Auditoría / Anomalías"]
)

if app_mode.startswith("🕵️"):
    # Pass config using 'filename' key as expected by updated code
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
            st.sidebar.warning(f"⚠️ Falta descargar: {name}")

    if not paths_to_query:
        st.error(
            "No hay ninguna base de datos descargada. Por favor selecciona una individualmente para descargarla primero."
        )
else:
    # Individual Mode
    dataset_info = DATASETS_CONFIG[dataset_name]
    file_path = os.path.join(DATA_DIR, dataset_info["filename"])
    paths_to_query = [(dataset_name, file_path)]

    # File status
    file_exists = os.path.exists(file_path)

    if not file_exists:
        st.warning(
            f"El archivo '{dataset_info['filename']}' no se encuentra en el disco."
        )
        if st.sidebar.button("Descargar Archivo Ahora"):
            ok, msg = download_file(dataset_info)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
    else:
        st.sidebar.success("Archivo cargado y listo.")

# Dynamic Filters
st.sidebar.header("2. Filtros")

# Search by Name
person_name_input = st.sidebar.text_input(
    "Buscar Persona", placeholder="Nombre o Apellido (El RUT no es público)"
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
        "Año Inicio",
        year_list,
        index=len(year_list) - 1 if len(year_list) > 0 else 0,
    )
with col2:
    end_year_input = st.selectbox("Año Fin", year_list, index=0)  # Most recent

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
    # Validation
    if not org_select and not person_name_input:
        st.warning(
            "⚠️ Por seguridad, debes ingresar al menos un Organismo O un Nombre de persona."
        )
    else:
        accumulated_dfs = []
        progress_bar = st.progress(0, text="Iniciando búsqueda...")

        for i, (source_name, source_path) in enumerate(paths_to_query):
            progress_bar.progress(
                (i / len(paths_to_query)), text=f"Consultando {source_name}..."
            )

            partial_df = quick_query(
                source_path,
                org_select,
                start_year_input,
                end_year_input,
                month_input,
                person_name_input,
            )

            if not partial_df.empty:
                partial_df["Origen"] = source_name  # Mark source
                accumulated_dfs.append(partial_df)

        progress_bar.empty()

        if accumulated_dfs:
            result_df = pd.concat(accumulated_dfs, ignore_index=True)

            if not result_df.empty:
                # --- DATA CLEANING AND FORMATTING ---

                # 1. Create date column for sorting
                months_map = {
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

                # Normalize month
                result_df["mes_num"] = (
                    result_df["Mes"]
                    .astype(str)
                    .str.capitalize()
                    .map(months_map)
                    .fillna(0)
                    .astype(int)
                )

                # Sort chronologically
                result_df = result_df.sort_values(
                    by=["anyo", "mes_num"], ascending=[False, False]
                )

                money_cols = ["remuliquida_mensual", "remuneracionbruta_mensual"]

                for col in money_cols:
                    if col in result_df.columns:
                        # Convert to int
                        result_df[col] = (
                            pd.to_numeric(
                                result_df[col]
                                .astype(str)
                                .str.replace(
                                    r"[^\d,]", "", regex=True
                                )  # Keep digits and commas
                                .str.replace(",", "."),  # Change comma to dot
                                errors="coerce",
                            )
                            .fillna(0)
                            .astype(int)
                        )

                # KPIs
                calc_col = (
                    "remuliquida_mensual"
                    if "remuliquida_mensual" in result_df.columns
                    else "remuneracionbruta_mensual"
                )

                total_expense = result_df[calc_col].sum()
                average = result_df[calc_col].mean()
                maximum = result_df[calc_col].max()
                count = len(result_df)

                col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

                def fmt_clp(value):
                    return (
                        f"$ {value:,.0f}".replace(",", "X")
                        .replace(".", ",")
                        .replace("X", ".")
                    )

                col_kpi1.metric("Registros", f"{count:,}".replace(",", "."))
                col_kpi2.metric("Gasto Total (Mes)", fmt_clp(total_expense))
                col_kpi3.metric("Promedio", fmt_clp(average))
                col_kpi4.metric("Sueldo Máximo", fmt_clp(maximum))

                # Tabs
                tab1, tab2, tab3 = st.tabs(
                    ["📋 Datos", "📊 Distribución", "🏆 Top Sueldos"]
                )

                with tab1:
                    # Display Copy
                    # Explicit column order
                    column_order = [
                        "anyo",
                        "Mes",
                        "Nombres",
                        "Paterno",
                        "Materno",
                        "remuneracionbruta_mensual",
                        "remuliquida_mensual",
                        "organismo_nombre",
                        "cargo",
                        "estamento",
                        "Origen",
                    ]

                    final_cols = [c for c in column_order if c in result_df.columns]
                    other_cols = [
                        c
                        for c in result_df.columns
                        if c not in final_cols and c != "mes_num"
                    ]

                    display_df = result_df[final_cols + other_cols].copy()

                    for col in money_cols:
                        if col in display_df.columns:
                            display_df[col] = display_df[col].apply(
                                lambda x: fmt_clp(x)
                            )

                    st.dataframe(display_df, use_container_width=True, hide_index=True)

                    # Download button
                    csv_data = result_df.to_csv(
                        index=False, sep=";", encoding="latin-1"
                    )
                    st.download_button(
                        "Descargar CSV", csv_data, "reporte.csv", "text/csv"
                    )

                with tab2:
                    fig = px.histogram(
                        result_df,
                        x=calc_col,
                        nbins=20,
                        title="Distribución de Sueldos",
                        labels={calc_col: "Monto ($)"},
                        template="plotly_dark",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with tab3:
                    top_10 = result_df.nlargest(10, calc_col).copy()

                    # Truncate long text
                    if "cargo" in top_10.columns:
                        top_10["cargo"] = (
                            top_10["cargo"]
                            .astype(str)
                            .apply(lambda x: x[:100] + "..." if len(x) > 100 else x)
                        )

                    # Rename for view
                    cols_to_show = {
                        "Nombres": "Nombre",
                        "Paterno": "Apellido",
                        "cargo": "Cargo / Función",
                        calc_col: "Sueldo",
                    }

                    valid_cols = {
                        k: v for k, v in cols_to_show.items() if k in top_10.columns
                    }
                    top_10_view = top_10[list(valid_cols.keys())].rename(
                        columns=valid_cols
                    )

                    st.dataframe(
                        top_10_view,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Sueldo": st.column_config.NumberColumn(
                                "Sueldo Líquido",
                                help="Monto mensual en pesos chilenos",
                                format="$ %d",
                            )
                        },
                    )

            else:
                st.info("No se encontraron resultados con esos filtros.")

                # Last record search
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
        "Tip: DuckDB permite leer estos archivos de 7GB+ en segundos sin ocupar toda tu RAM."
    )
