import streamlit as st
import pandas as pd
import plotly.express as px
from src.core.config import MONTHS_MAP
from src.core.logger import get_logger

logger = get_logger()


def render_kpis(result_df, calc_col):
    """Calculates and renders high-level metric KPIs."""
    total_expense = result_df[calc_col].sum()
    average = result_df[calc_col].mean()
    maximum = result_df[calc_col].max()
    count = len(result_df)

    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

    col_kpi1.metric("Registros", f"{count:,}".replace(",", "."))

    # Check if we're showing a single month or multiple records
    # to display an accurate label for the total sum
    if count == 1:
        total_label = "Gasto Total (Mes)"
    else:
        total_label = "Gasto Total (Acumulado)"

    col_kpi2.metric(total_label, format_clp(total_expense))
    col_kpi3.metric("Promedio", format_clp(average))
    col_kpi4.metric("Sueldo Máximo", format_clp(maximum))


def format_clp(value):
    """Formats a number as Chilean Pesos."""
    try:
        return (
            f"$ {int(value):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )
    except (ValueError, TypeError):
        return ""


def process_and_display_results(result_df):
    """Main rendering function for the search results."""
    # 1. Clean and normalize data
    result_df["mes_num"] = (
        result_df["Mes"]
        .astype(str)
        .str.capitalize()
        .map(MONTHS_MAP)
        .fillna(0)
        .astype(int)
    )

    result_df = result_df.sort_values(by=["anyo", "mes_num"], ascending=[False, False])

    calc_col = (
        "remuliquida_mensual"
        if "remuliquida_mensual" in result_df.columns
        else "remuneracionbruta_mensual"
    )

    # 2. Render KPIs
    render_kpis(result_df, calc_col)

    # 3. Render Tabs
    tab1, tab2, tab3 = st.tabs(
        [
            ":material/table: Datos",
            ":material/pie_chart: Distribución",
            ":material/emoji_events: Top Sueldos",
        ]
    )

    with tab1:
        render_data_table(result_df)

    with tab2:
        render_distribution_chart(result_df, calc_col)

    with tab3:
        render_top_salaries(result_df, calc_col)


def render_data_table(result_df):
    """Renders the raw data table and download button."""
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
        "origen",
    ]

    final_cols = [c for c in column_order if c in result_df.columns]
    other_cols = [
        c
        for c in result_df.columns
        if c not in final_cols and c not in ["mes_num", "search_vector"]
    ]

    display_df = result_df[final_cols + other_cols].copy()

    # Restore format_clp for money columns to keep Chilean thousands separators
    money_cols = ["remuliquida_mensual", "remuneracionbruta_mensual"]
    for col in money_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: format_clp(x) if pd.notnull(x) else ""
            )

    # Define human-readable column configurations
    col_config = {
        "anyo": st.column_config.NumberColumn("Año", format="%d"),
        "Mes": "Mes",
        "Nombres": "Nombres",
        "Paterno": "Apellido Paterno",
        "Materno": "Apellido Materno",
        "remuneracionbruta_mensual": "Remuneración Bruta",
        "remuliquida_mensual": "Remuneración Líquida",
        "organismo_nombre": "Organismo",
        "cargo": "Cargo",
        "estamento": "Estamento",
        "origen": "Origen",
    }

    selection = st.dataframe(
        display_df,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config=col_config,
    )

    # Show details if a Senate row was selected
    sel_dict = selection.get("selection", {}) if selection else {}
    rows_list = sel_dict.get("rows", []) if sel_dict else []
    if rows_list:
        row_idx = rows_list[0]
        selected_row = display_df.iloc[row_idx]
        # Allow checking if the row comes from the Senate or Camara
        origen = str(selected_row.get("origen", "")).lower()
        organismo = str(selected_row.get("organismo_nombre", "")).lower()
        if (
            "senado" in origen
            or "senado" in organismo
            or "cámara" in origen
            or "camara" in origen
            or "cámara" in organismo
            or "camara" in organismo
        ):
            render_gastos_detalle(selected_row)

    csv_data = result_df.to_csv(index=False, sep=";", encoding="latin-1")
    st.download_button("Descargar CSV", csv_data, "reporte.csv", "text/csv")


def render_gastos_detalle(selected_row):
    """Shows a sub-window or table with the detailed expenses for the selected row."""
    import duckdb
    import os

    # Extract key variables from the row
    nombres = selected_row["Nombres"]
    paterno = selected_row["Paterno"]
    materno = selected_row.get("Materno", "")

    # The key in the parquet file is nombre+paterno+materno
    parts = []
    if pd.notna(nombres) and str(nombres).strip():
        parts.append(str(nombres).strip())
    if pd.notna(paterno) and str(paterno).strip():
        parts.append(str(paterno).strip())
    if pd.notna(materno) and str(materno).strip():
        parts.append(str(materno).strip())
    llave = " ".join(parts).replace("  ", " ")

    # Handle the month as integer because parquet uses numbers
    from src.core.config import MONTHS_MAP

    mes_str = selected_row["Mes"]
    mes_num = MONTHS_MAP.get(mes_str, 1)

    anyo = int(selected_row["anyo"])

    logger.info(
        "expenses requested", extra={"person": llave, "year": anyo, "month": mes_str}
    )

    import glob

    gastos_files = glob.glob(
        os.path.join("data", "parquet", "*_gastos_detalle.parquet")
    )

    if not gastos_files:
        return

    selects = []
    for f in gastos_files:
        selects.append(f"""
            SELECT gastos_operacionales AS Concepto, sum(monto) as Monto
            FROM read_parquet('{f}')
            WHERE anyo = ? AND Mes = ? AND llave_senador = ?
            GROUP BY gastos_operacionales
        """)

    query = " UNION ALL ".join(selects)
    query = f"SELECT Concepto, sum(Monto) as Monto FROM ({query}) GROUP BY Concepto ORDER BY Monto DESC"

    try:
        full_params = [anyo, mes_num, llave] * len(gastos_files)
        df_detalle = duckdb.query(query, params=full_params).to_df()
    except Exception as e:
        logger.error(
            "expenses query failed",
            extra={"person": llave, "error": str(e).replace("\n", " ")},
        )
        st.error(f"No se pudo cargar el detalle: {e}")
        return

    st.markdown("---")
    st.subheader(
        f":material/search: Desglose de Gastos Operacionales ({mes_str} {anyo})"
    )

    # Adapt title depending on where it came from
    origen = str(selected_row.get("origen", "")).lower()
    organismo = str(selected_row.get("organismo_nombre", "")).lower()
    if (
        "cámara" in origen
        or "camara" in origen
        or "cámara" in organismo
        or "camara" in organismo
    ):
        st.markdown(f"**Diputado(a):** {llave}")
    else:
        st.markdown(f"**Senador(a):** {llave}")

    if df_detalle.empty:
        logger.info("expenses query completed", extra={"person": llave, "found": False})
        st.info("No se registraron gastos operacionales extraordinarios este mes.")
    else:
        logger.info(
            "expenses query completed",
            extra={"person": llave, "found": True, "rows": len(df_detalle)},
        )
        col1, col2 = st.columns([1, 1])
        with col1:
            df_display = df_detalle.copy()
            df_display["Monto"] = df_display["Monto"].apply(
                lambda x: format_clp(x) if pd.notnull(x) else ""
            )

            st.dataframe(
                df_display,
                hide_index=True,
                column_config={"Monto": "Monto Gastado"},
            )
        with col2:
            import plotly.express as px

            # Filter out 0 values to prevent chart clutter
            df_chart = df_detalle[df_detalle["Monto"] > 0]

            fig = px.pie(
                df_chart,
                values="Monto",
                names="Concepto",
                hole=0.4,
                title="Distribución de Gastos",
            )
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)


def render_distribution_chart(result_df, calc_col):
    """Renders the Plotly histogram of salary distributions."""
    fig = px.histogram(
        result_df,
        x=calc_col,
        nbins=20,
        title="Distribución de Sueldos",
        labels={calc_col: "Monto ($)"},
        template="plotly_dark",
    )
    st.plotly_chart(fig)


def render_top_salaries(result_df, calc_col):
    """Renders the top 10 salaries table."""
    top_10 = result_df.nlargest(10, calc_col).copy()

    if "cargo" in top_10.columns:
        top_10["cargo"] = (
            top_10["cargo"]
            .astype(str)
            .apply(lambda x: x[:100] + "..." if len(x) > 100 else x)
        )

    cols_to_show = {
        "Nombres": "Nombre",
        "Paterno": "Apellido",
        "cargo": "Cargo / Función",
        calc_col: "Sueldo",
    }

    valid_cols = {k: v for k, v in cols_to_show.items() if k in top_10.columns}
    top_10_view = top_10[list(valid_cols.keys())].rename(columns=valid_cols)

    if "Sueldo" in top_10_view.columns:
        top_10_view["Sueldo"] = top_10_view["Sueldo"].apply(
            lambda x: format_clp(x) if pd.notnull(x) else ""
        )

    st.dataframe(
        top_10_view,
        hide_index=True,
        column_config={
            "Sueldo": st.column_config.TextColumn(
                "Sueldo Líquido", help="Monto mensual"
            )
        },
    )
