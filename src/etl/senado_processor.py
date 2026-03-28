import os
import json
import logging
from glob import glob
import pandas as pd

logger = logging.getLogger("DataProcessor")


class DataProcessor:
    def __init__(
        self,
        cache_dir="data/raw/senado",
        output_dir="data/parquet",
        processed_dir="data/processed/senado",
    ):
        self.cache_dir = cache_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        # Auxiliary directory for raw/analyst outputs
        self.processed_dir = processed_dir
        os.makedirs(self.processed_dir, exist_ok=True)

        # Month mapping
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

    def _load_json_files(self, endpoint_name):
        all_data = []
        path_pattern = os.path.join(self.cache_dir, endpoint_name, "*", "*.json")
        files = glob(path_pattern)

        for file_path in files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = json.load(f)
                    items = []
                    if (
                        "data" in content
                        and isinstance(content["data"], dict)
                        and "data" in content["data"]
                    ):
                        items = content["data"]["data"]
                    elif "data" in content and isinstance(content["data"], list):
                        items = content["data"]

                    for item in items:
                        if isinstance(item, dict) and "attributes" in item:
                            all_data.append(item["attributes"])
                        else:
                            all_data.append(item)
            except Exception as e:
                logger.error(f"Error parsing {file_path}: {e}")

        return all_data

    def _normalize_name(self, nombre, appaterno, apmaterno=None):
        parts = []
        if pd.notna(nombre) and str(nombre).strip():
            parts.append(str(nombre).strip().upper())
        if pd.notna(appaterno) and str(appaterno).strip():
            parts.append(str(appaterno).strip().upper())
        if pd.notna(apmaterno) and str(apmaterno).strip():
            parts.append(str(apmaterno).strip().upper())

        return " ".join(parts).replace("  ", " ").strip()

    def _unaccent_lower(self, text):
        import unicodedata

        if pd.isna(text) or not str(text).strip():
            return ""
        text = str(text).lower()
        return (
            unicodedata.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode("utf-8")
            .strip()
        )

    def process_all(self):
        logger.info("== Starting data processing (Generating Parquet and CSVs) ==")

        dietas_raw = self._load_json_files("dietas")
        df_dietas = pd.DataFrame(dietas_raw)

        if df_dietas.empty:
            logger.warning("No diet data in cache.")
            return

        df_dietas["llave_senador"] = df_dietas.apply(
            lambda x: self._normalize_name(
                x.get("nombre"), x.get("appaterno"), x.get("apmaterno")
            ),
            axis=1,
        )

        gastos_raw = self._load_json_files("gastos_operacionales")
        df_gastos = pd.DataFrame(gastos_raw)

        df_final = df_dietas.copy()

        if not df_gastos.empty:
            df_gastos["llave_senador"] = df_gastos.apply(
                lambda x: self._normalize_name(
                    x.get("nombre"), x.get("appaterno"), x.get("apmaterno")
                ),
                axis=1,
            )

            gastos_agrupados = (
                df_gastos.groupby(["ano", "mes", "llave_senador"])["monto"]
                .sum()
                .reset_index()
            )
            gastos_agrupados.rename(
                columns={"monto": "total_gastos_operacionales"}, inplace=True
            )

            df_final = pd.merge(
                df_final,
                gastos_agrupados,
                on=["ano", "mes", "llave_senador"],
                how="left",
            )
            df_final["total_gastos_operacionales"] = df_final[
                "total_gastos_operacionales"
            ].fillna(0)
        else:
            df_final["total_gastos_operacionales"] = 0

        df_final["costo_total_mensual"] = (
            df_final["dieta"] + df_final["total_gastos_operacionales"]
        )

        # Generate CSV/Excel for the Analyst
        cols = [
            "ano",
            "mes",
            "rut",
            "llave_senador",
            "dieta",
            "deducciones",
            "saldo",
            "total_gastos_operacionales",
            "costo_total_mensual",
        ]
        cols_presentes = [c for c in cols if c in df_final.columns]
        df_export = df_final[cols_presentes].sort_values(
            by=["ano", "mes", "llave_senador"]
        )

        csv_path = os.path.join(self.processed_dir, "senadores_consolidado.csv")
        df_export.to_csv(csv_path, index=False)

        logger.info(f"💾 Saved raw consolidated files in: {csv_path}")

        # ==== Transformation to Main App Schema (DuckDB Parquet) ====
        df_app = pd.DataFrame()
        df_app["organismo_nombre"] = pd.Series(
            ["Senado de la República"] * len(df_final)
        )
        df_app["anyo"] = df_final["ano"].astype(int)
        df_app["Mes"] = df_final["mes"].apply(
            lambda m: self.meses_map.get(int(m), "Enero")
        )
        df_app["estamento"] = pd.Series(["Senador(a)"] * len(df_final))
        df_app["Nombres"] = df_final["nombre"].str.upper().fillna("")
        df_app["Paterno"] = df_final["appaterno"].str.upper().fillna("")
        df_app["Materno"] = df_final["apmaterno"].str.upper().fillna("")
        df_app["cargo"] = pd.Series(["Senador(a) de la República"] * len(df_final))
        df_app["remuliquida_mensual"] = df_final["saldo"].fillna(0).astype(int)
        df_app["remuneracionbruta_mensual"] = (
            df_final["costo_total_mensual"].fillna(0).astype(int)
        )
        df_app["origen"] = pd.Series(["Senado"] * len(df_final))

        df_app["search_vector"] = df_app.apply(
            lambda x: self._unaccent_lower(
                f"{x['Nombres']} {x['Paterno']} {x['Materno']}"
            ),
            axis=1,
        )

        parquet_path = os.path.join(self.output_dir, "senado_consolidado.parquet")

        # Export to Parquet
        df_app.to_parquet(parquet_path, engine="pyarrow", compression="zstd")
        logger.info(f"🎉 Parquet file generated for Web App: {parquet_path}")

        if not df_gastos.empty:
            gastos_path = os.path.join(self.output_dir, "senado_gastos_detalle.parquet")
            if "gastos_operacionales" in df_gastos.columns:
                # Rename cols to match standard (for metadata cache script to not break)
                df_gastos_pq = df_gastos[
                    ["ano", "mes", "llave_senador", "gastos_operacionales", "monto"]
                ].copy()
                df_gastos_pq.rename(columns={"ano": "anyo", "mes": "Mes"}, inplace=True)
                df_gastos_pq["organismo_nombre"] = "Senado de la República"

                df_gastos_pq.to_parquet(
                    gastos_path, engine="pyarrow", compression="zstd"
                )
