import os
import glob
import logging
import pandas as pd
import unicodedata

logger = logging.getLogger("DiputadosProcessor")


class DiputadosProcessor:
    def __init__(
        self,
        raw_dir="data/raw/diputados",
        output_dir="data/parquet",
        processed_dir="data/processed/diputados",
    ):
        self.raw_dir = raw_dir
        self.output_dir = output_dir
        self.processed_dir = processed_dir
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

        self.meses_map = {
            1: "Enero",
            2: "Febrero",
            3: "Marzo",
            4: "Abril",
            5: "Mayo",
            6: "Junio",
            7: "Julio",
            8: "Agosto",
            9: "Septiembre",
            10: "Octubre",
            11: "Noviembre",
            12: "Diciembre",
        }

    def _unaccent_lower(self, text):
        if pd.isna(text) or not str(text).strip():
            return ""
        text = str(text).lower()
        return (
            unicodedata.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode("utf-8")
            .strip()
        )

    def _clean_money(self, series):
        """Converts strings like '1.234.567' to integers."""
        if series is None:
            return 0
        s = series.astype(str).str.replace(r"[^\d]", "", regex=True)
        return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

    def process_gastos_operacionales(self):
        """Process Gastos Operacionales into a parquet file."""
        logger.info("Processing Gastos Operacionales...")
        files = glob.glob(os.path.join(self.raw_dir, "gastos_operacionales", "*.csv"))

        # We need mapping from ID_Diputado to Name.
        dieta_files = glob.glob(os.path.join(self.raw_dir, "diputados_dieta", "*.csv"))
        id_to_name = {}
        for f in dieta_files:
            try:
                df_d = pd.read_csv(f)
                if not df_d.empty:
                    for _, r in df_d.iterrows():
                        id_to_name[r["ID_Diputado"]] = r["Nombre"].upper()
            except Exception as e:
                logger.error(f"Failed to read dietas file {f}: {e}")

        all_gastos = []
        for f in files:
            try:
                parts = os.path.basename(f).replace(".csv", "").split("_")
                if len(parts) == 3:
                    pid, year, month = parts
                    df = pd.read_csv(f)
                    if (
                        not df.empty
                        and "Concepto" in df.columns
                        and "Monto" in df.columns
                    ):
                        df["anyo"] = int(year)
                        df["Mes"] = int(month)
                        df["ID_Diputado"] = int(pid)
                        all_gastos.append(df)
            except Exception as e:
                logger.error(f"Error processing gasto file {f}: {e}")

        if not all_gastos:
            logger.warning("No Gastos Operacionales found to process.")
            return

        df_gastos = pd.concat(all_gastos, ignore_index=True)
        df_gastos["llave_senador"] = df_gastos["ID_Diputado"].map(id_to_name)
        df_gastos["gastos_operacionales"] = df_gastos["Concepto"]
        df_gastos["monto"] = self._clean_money(df_gastos["Monto"])
        df_gastos["organismo_nombre"] = "Cámara de Diputadas y Diputados"

        # Keep only required cols
        df_gastos_pq = df_gastos[
            [
                "anyo",
                "Mes",
                "llave_senador",
                "gastos_operacionales",
                "monto",
                "organismo_nombre",
            ]
        ].copy()

        gastos_path = os.path.join(self.output_dir, "diputados_gastos_detalle.parquet")
        df_gastos_pq.to_parquet(gastos_path, engine="pyarrow", compression="zstd")
        logger.info(f"Parquet file generated for Gastos: {gastos_path}")

    def process_all(self):
        logger.info("== Starting Data Processing for Camara de Diputados ==")

        all_dfs = []

        # 1. Process Personal de Apoyo
        apoyo_files = glob.glob(os.path.join(self.raw_dir, "personal_apoyo", "*.csv"))
        for f in apoyo_files:
            try:
                year, month = os.path.basename(f).replace(".csv", "").split("_")
                df = pd.read_csv(f)
                if not df.empty and "Nombre" in df.columns:
                    df["anyo"] = int(year)
                    df["Mes_id"] = int(month)
                    df["estamento"] = "Personal de Apoyo Parlamentario"

                    # Try to map columns. Apoyo has "Sueldo", "Cargo", "Nombre", "Diputado"
                    df["Nombres"] = df["Nombre"].str.upper()
                    df["cargo"] = (
                        df["Cargo"].fillna("Asesor")
                        + " de "
                        + df.filter(regex=r"(?i)diputad").iloc[:, 0].fillna("Diputado")
                    )

                    # Both are the same for Apoyo as we don't have gross vs liquid distinction easily
                    df["remuliquida_mensual"] = self._clean_money(df.get("Sueldo"))
                    df["remuneracionbruta_mensual"] = df["remuliquida_mensual"]
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {f}: {e}")

        # 2. Process Personal de Planta / Contrata / Honorarios
        staff_types = {
            "personal_planta": "Personal de Planta",
            "personal_contrata": "Personal a Contrata",
            "personal_honorarios": "Personal a Honorarios",
        }
        for subdir, estamento_name in staff_types.items():
            files = glob.glob(os.path.join(self.raw_dir, subdir, "*.csv"))
            for f in files:
                try:
                    year, month = os.path.basename(f).replace(".csv", "").split("_")
                    df = pd.read_csv(f)
                    if not df.empty:
                        df["anyo"] = int(year)
                        df["Mes_id"] = int(month)
                        df["estamento"] = estamento_name

                        # Find name column
                        name_cols = [
                            c for c in df.columns if "nombre" in str(c).lower()
                        ]
                        if name_cols:
                            df["Nombres"] = df[name_cols[0]].str.upper()

                        # Find cargo
                        cargo_cols = [
                            c
                            for c in df.columns
                            if "cargo" in str(c).lower() or "función" in str(c).lower()
                        ]
                        if cargo_cols:
                            df["cargo"] = df[cargo_cols[0]]

                        # Find money
                        money_cols = [
                            c
                            for c in df.columns
                            if "remun" in str(c).lower()
                            or "sueldo" in str(c).lower()
                            or "honorario" in str(c).lower()
                        ]
                        if money_cols:
                            df["remuliquida_mensual"] = self._clean_money(
                                df[money_cols[0]]
                            )
                            df["remuneracionbruta_mensual"] = df["remuliquida_mensual"]

                        all_dfs.append(df)
                except Exception as e:
                    logger.error(f"Error processing {f}: {e}")

        # 3. Process Diputados (Base Salary)
        dieta_files = glob.glob(os.path.join(self.raw_dir, "diputados_dieta", "*.csv"))
        for f in dieta_files:
            try:
                year, month = os.path.basename(f).replace(".csv", "").split("_")
                df = pd.read_csv(f)
                if not df.empty:
                    df["anyo"] = int(year)
                    df["Mes_id"] = int(month)
                    df["estamento"] = "Diputado(a)"
                    df["Nombres"] = df["Nombre"].str.upper()
                    df["cargo"] = df["Cargo"]
                    df["remuliquida_mensual"] = df["Sueldo Liquido"]
                    df["remuneracionbruta_mensual"] = df["Sueldo Bruto"]
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {f}: {e}")

        if not all_dfs:
            logger.warning("No Camara data found to process.")
            return

        # Combine all
        df_final = pd.concat(all_dfs, ignore_index=True)

        # Standardize missing columns
        for col in [
            "Nombres",
            "cargo",
            "remuliquida_mensual",
            "remuneracionbruta_mensual",
        ]:
            if col not in df_final.columns:
                df_final[col] = "" if "nombre" in col or "cargo" in col else 0

        df_final["Nombres"] = df_final["Nombres"].fillna("")
        df_final["Paterno"] = ""
        df_final["Materno"] = ""
        df_final["cargo"] = df_final["cargo"].fillna("Funcionario/a")
        df_final["remuliquida_mensual"] = (
            df_final["remuliquida_mensual"].fillna(0).astype(int)
        )
        df_final["remuneracionbruta_mensual"] = (
            df_final["remuneracionbruta_mensual"].fillna(0).astype(int)
        )

        # Mapping for the final output
        df_app = pd.DataFrame()
        df_app["organismo_nombre"] = "Cámara de Diputadas y Diputados"
        df_app["anyo"] = df_final["anyo"]
        df_app["Mes"] = df_final["Mes_id"].map(self.meses_map)
        df_app["estamento"] = df_final["estamento"]
        df_app["Nombres"] = df_final["Nombres"].astype(str)
        df_app["Paterno"] = df_final["Paterno"]
        df_app["Materno"] = df_final["Materno"]
        df_app["cargo"] = df_final["cargo"].astype(str)
        df_app["remuliquida_mensual"] = df_final["remuliquida_mensual"]
        df_app["remuneracionbruta_mensual"] = df_final["remuneracionbruta_mensual"]
        df_app["origen"] = "Cámara de Diputados"

        df_app["search_vector"] = df_app.apply(
            lambda x: self._unaccent_lower(
                f"{x['Nombres']} {x['Paterno']} {x['Materno']}"
            ),
            axis=1,
        )

        # Drop completely empty rows where there is no name
        df_app = df_app[df_app["Nombres"].str.strip() != ""]

        # Export to CSV for Analysts
        csv_path = os.path.join(self.processed_dir, "diputados_consolidado.csv")
        df_app.to_csv(csv_path, index=False)

        # Export to Parquet for Web App
        parquet_path = os.path.join(self.output_dir, "diputados_consolidado.parquet")
        df_app.to_parquet(parquet_path, engine="pyarrow", compression="zstd")

        self.process_gastos_operacionales()

        logger.info(f"🎉 Parquet file generated for Web App: {parquet_path}")
