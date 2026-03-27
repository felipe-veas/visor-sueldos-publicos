import streamlit as st
import pandas as pd
import plotly.express as px
from src.core.config import MONTHS_MAP


def render_kpis(result_df, calc_col):
    """Calculates and renders high-level metric KPIs."""
    total_expense = result_df[calc_col].sum()
    average = result_df[calc_col].mean()
    maximum = result_df[calc_col].max()
    count = len(result_df)

    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

    col_kpi1.metric("Registros", f"{count:,}".replace(",", "."))
    col_kpi2.metric("Gasto Total (Mes)", format_clp(total_expense))
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
    tab1, tab2, tab3 = st.tabs(["📋 Datos", "📊 Distribución", "🏆 Top Sueldos"])

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

    money_cols = ["remuliquida_mensual", "remuneracionbruta_mensual"]
    for col in money_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: format_clp(x) if pd.notnull(x) else ""
            )

    st.dataframe(display_df, hide_index=True)

    csv_data = result_df.to_csv(index=False, sep=";", encoding="latin-1")
    st.download_button("Descargar CSV", csv_data, "reporte.csv", "text/csv")


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

    st.dataframe(
        top_10_view,
        hide_index=True,
        column_config={
            "Sueldo": st.column_config.NumberColumn(
                "Sueldo Líquido",
                help="Monto mensual en pesos chilenos",
                format="$ %d",
            )
        },
    )
