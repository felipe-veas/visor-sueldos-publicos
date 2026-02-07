import streamlit as st
import duckdb
import os


def generate_unified_sql(valid_paths):
    """Generates a UNION ALL query for all available files."""
    subqueries = []

    # Mapping of key concepts to normalize column names across files
    # Keys are the target column aliases (kept in Spanish for Frontend/SQL consistency)
    # Values are the possible source column names in the CSVs
    column_mapping = {
        "Nombres": ["Nombres", "nombres", "Nombre"],
        "Paterno": ["Paterno", "paterno", "Apellido Paterno"],
        "Materno": ["Materno", "materno", "Apellido Materno"],
        "anyo": ["anyo", "Año", "Year"],
        "Mes": ["Mes", "mes", "Month"],
        "organismo": ["organismo_nombre", "Organismo", "Institucion"],
        "sueldo": [
            "remuliquida_mensual",
            "remuneracionbruta",
            "Sueldo Liquido",
            "Honorario Bruto",
        ],
        "estamento": [
            "Tipo Estamento",
            "tipo_calificacionp",
            "estamento",
            "Calificacion Profesional",
        ],
        "cargo": ["Tipo cargo", "descripcion_funcion", "Cargo", "Funcion"],
    }

    for source_name, path in valid_paths:
        # Detect real columns in the file
        try:
            # Read only the header (limit 0) to check columns
            schema_query = f"SELECT * FROM read_csv('{path}', delim=';', encoding='latin-1', ignore_errors=true) LIMIT 0"
            df_schema = duckdb.query(schema_query).to_df()
            real_cols = set(df_schema.columns)
        except Exception:
            continue

        selects = [f"'{source_name}' AS Origen"]

        for alias, options in column_mapping.items():
            found_col = "NULL"
            for op in options:
                if op in real_cols:
                    found_col = f'"{op}"'
                    break
            selects.append(f"{found_col} AS {alias}")

        # Build the subquery for this file
        subqueries.append(
            f"SELECT {', '.join(selects)} FROM read_csv('{path}', delim=';', encoding='latin-1', ignore_errors=true, null_padding=true)"
        )

    if not subqueries:
        return ""

    return " UNION ALL ".join(subqueries)


def render_audit_ui(data_dir, urls_config):
    st.header("🕵️ Auditoría Civil de Anomalías")
    st.markdown(
        "Herramientas avanzadas para detectar patrones sospechosos en el gasto público cruzando todas las bases de datos."
    )

    # Detect available files on disk
    paths = []
    for name, info in urls_config.items():
        # Note: 'info' comes from the URLS dict in analisis_sueldos.py.
        # I previously changed 'archivo' to 'filename' in analisis_sueldos.py.
        # I must ensure I use 'filename' here.
        # But wait, app.py imports URLS and passes it here?
        # If I changed URLS in analisis_sueldos.py, I must rely on that change.
        # Yes, I changed it to 'filename'.
        file_path = os.path.join(data_dir, info["filename"])
        if os.path.exists(file_path):
            paths.append((name, file_path))

    if len(paths) < 2:
        st.warning(
            "⚠️ Se recomienda descargar todas las bases de datos (Planta, Contrata, Honorarios) en el modo 'Explorador' para una auditoría completa."
        )

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "🔄 Multiempleo",
            "💰 Ranking Nacional",
            "👨‍👩‍👧‍👦 Apellidos (Nepotismo)",
            "📈 Sueldos Atípicos",
        ]
    )

    # Common date configuration
    with st.sidebar:
        st.markdown("### Filtros Auditoría")
        audit_year = st.selectbox("Año", [2026, 2025, 2024, 2023, 2022], index=1)
        audit_month = st.selectbox(
            "Mes",
            [
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
            index=0,
        )

    where_audit_month = f"AND Mes = '{audit_month}'"

    with tab1:
        st.subheader("Detección de Multiempleo Simultáneo")
        st.write(
            "Busca personas que aparecen recibiendo sueldo en **más de un organismo** en el mismo mes."
        )

        if st.button("🔍 Escanear Multiempleo"):
            with st.spinner("Cruzando bases de datos..."):
                base_sql = generate_unified_sql(paths)
                if not base_sql:
                    st.error("No hay datos.")
                else:
                    query = f"""
                    WITH unificados AS ({base_sql}),
                    limpios AS (
                        SELECT
                            upper(trim(COALESCE(Nombres,'')) || ' ' || trim(COALESCE(Paterno,'')) || ' ' || trim(COALESCE(Materno,''))) as nombre_completo,
                            organismo,
                            TRY_CAST(anyo AS INTEGER) as anyo_int,
                            Mes,
                            TRY_CAST(regexp_replace(replace(CAST(sueldo AS VARCHAR), '.', ''), '[^0-9]', '', 'g') AS BIGINT) as sueldo_num,
                            Origen
                        FROM unificados
                    ),
                    conteo AS (
                        SELECT
                            nombre_completo,
                            anyo_int as anyo,
                            Mes,
                            COUNT(DISTINCT organismo) as num_empleos,
                            SUM(sueldo_num) as sueldo_total,
                            LIST(DISTINCT organismo) as lista_organismos,
                            LIST(DISTINCT Origen) as tipos_contrato
                        FROM limpios
                        WHERE anyo_int = {audit_year} {where_audit_month}
                        GROUP BY 1, 2, 3
                        HAVING num_empleos > 1
                    )
                    SELECT * FROM conteo ORDER BY sueldo_total DESC LIMIT 100
                    """
                    try:
                        df = duckdb.query(query).to_df()
                        if not df.empty:
                            st.error(f"🚨 {len(df)} casos detectados.")
                            df["sueldo_total"] = df["sueldo_total"].apply(
                                lambda x: f"$ {x:,.0f}".replace(",", "X")
                                .replace(".", ",")
                                .replace("X", ".")
                            )
                            st.dataframe(
                                df,
                                use_container_width=True,
                                column_config={
                                    "lista_organismos": st.column_config.ListColumn(
                                        "Organismos"
                                    )
                                },
                            )
                        else:
                            st.success("✅ Sin hallazgos.")
                    except Exception as e:
                        st.error(f"Error: {e}")

    with tab2:
        st.subheader("Ranking Nacional de Sueldos")
        if st.button("🏆 Generar Ranking"):
            with st.spinner("Analizando..."):
                base_sql = generate_unified_sql(paths)
                if base_sql:
                    query = f"""
                    SELECT
                        organismo,
                        upper(trim(COALESCE(Nombres,'')) || ' ' || trim(COALESCE(Paterno,'')) || ' ' || trim(COALESCE(Materno,''))) as nombre_completo,
                        TRY_CAST(regexp_replace(replace(CAST(sueldo AS VARCHAR), '.', ''), '[^0-9]', '', 'g') AS BIGINT) as sueldo_num,
                        Origen,
                        cargo
                    FROM ({base_sql})
                    WHERE sueldo_num IS NOT NULL AND TRY_CAST(anyo AS INTEGER) = {audit_year} {where_audit_month}
                    ORDER BY sueldo_num DESC
                    LIMIT 100
                    """
                    df = duckdb.query(query).to_df()
                    df["sueldo_num"] = df["sueldo_num"].apply(
                        lambda x: f"$ {x:,.0f}".replace(",", "X")
                        .replace(".", ",")
                        .replace("X", ".")
                    )
                    st.dataframe(df, use_container_width=True)

    with tab3:
        st.subheader("Concentración de Apellidos (Posible Nepotismo)")
        st.markdown(
            "Lista organismos donde se repite inusualmente un mismo apellido paterno."
        )

        min_repeats = st.slider("Mínimo de personas con mismo apellido", 3, 50, 5)

        if st.button("🔍 Buscar Clanes"):
            with st.spinner("Agrupando apellidos..."):
                base_sql = generate_unified_sql(paths)
                if base_sql:
                    # Exclude common surnames in Chile to reduce noise
                    common_surnames = "'GONZALEZ', 'MUÑOZ', 'ROJAS', 'DIAZ', 'PEREZ', 'SOTO', 'CONTRERAS', 'SILVA', 'MARTINEZ', 'SEPULVEDA'"

                    query = f"""
                    SELECT
                        organismo,
                        upper(Paterno) as apellido,
                        COUNT(*) as cantidad_personas,
                        SUM(TRY_CAST(regexp_replace(replace(CAST(sueldo AS VARCHAR), '.', ''), '[^0-9]', '', 'g') AS BIGINT)) as costo_mensual_total
                    FROM ({base_sql})
                    WHERE
                        TRY_CAST(anyo AS INTEGER) = {audit_year}
                        {where_audit_month}
                        AND length(Paterno) > 2
                        AND upper(Paterno) NOT IN ({common_surnames})
                    GROUP BY 1, 2
                    HAVING cantidad_personas >= {min_repeats}
                    ORDER BY cantidad_personas DESC
                    LIMIT 100
                    """
                    df = duckdb.query(query).to_df()
                    if not df.empty:
                        df["costo_mensual_total"] = df["costo_mensual_total"].apply(
                            lambda x: f"$ {x:,.0f}".replace(",", "X")
                            .replace(".", ",")
                            .replace("X", ".")
                        )
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info(
                            "No se encontraron concentraciones altas de apellidos (excluyendo los comunes)."
                        )

    with tab4:
        st.subheader("Sueldos Atípicos (Outliers)")
        st.markdown(
            "Detecta sueldos que se desvían más de **3 veces** del promedio de su estamento/función."
        )

        if st.button("📈 Detectar Atípicos"):
            with st.spinner("Calculando estadísticas por estamento..."):
                base_sql = generate_unified_sql(paths)
                if base_sql:
                    query = f"""
                    WITH base AS (
                        SELECT
                            organismo,
                            estamento,
                            upper(trim(COALESCE(Nombres,'')) || ' ' || trim(COALESCE(Paterno,'')) || ' ' || trim(COALESCE(Materno,''))) as nombre,
                            TRY_CAST(regexp_replace(replace(CAST(sueldo AS VARCHAR), '.', ''), '[^0-9]', '', 'g') AS BIGINT) as sueldo_num
                        FROM ({base_sql})
                        WHERE TRY_CAST(anyo AS INTEGER) = {audit_year} {where_audit_month}
                    ),
                    stats AS (
                        SELECT
                            estamento,
                            AVG(sueldo_num) as promedio,
                            STDDEV(sueldo_num) as desv
                        FROM base
                        WHERE sueldo_num > 400000 -- Ignore low/symbolic salaries for average
                        GROUP BY 1
                        HAVING COUNT(*) > 10 -- Only roles with enough people
                    )
                    SELECT
                        b.organismo,
                        b.nombre,
                        b.estamento,
                        b.sueldo_num as sueldo,
                        CAST(s.promedio AS BIGINT) as promedio_estamento,
                        CAST((b.sueldo_num / NULLIF(s.promedio, 0)) AS DECIMAL(10,1)) as veces_promedio
                    FROM base b
                    JOIN stats s ON b.estamento = s.estamento
                    WHERE
                        b.sueldo_num > (s.promedio + 3 * s.desv) -- 3 Sigma rule
                        AND b.sueldo_num > 2000000 -- Only check relevant salaries
                    ORDER BY veces_promedio DESC
                    LIMIT 100
                    """
                    df = duckdb.query(query).to_df()

                    for col in ["sueldo", "promedio_estamento"]:
                        df[col] = df[col].apply(
                            lambda x: f"$ {x:,.0f}".replace(",", "X")
                            .replace(".", ",")
                            .replace("X", ".")
                        )

                    st.dataframe(df, use_container_width=True)
