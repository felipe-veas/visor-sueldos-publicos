import duckdb
import os
import glob
import logging

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_DIR = "data"

# Unified standard schema for the Parquet files
# Target column: list of possible source columns
CONCEPT_MAPPING = {
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


def clean_money_sql(col_name):
    """Generates DuckDB SQL to clean a money column from '$ 1.000,50' to an integer."""
    if col_name == "NULL":
        return "NULL"
    # Remove non-digits and comma (since comma was used as decimal in app.py we keep digits and commas, then replace comma with dot)
    # DuckDB string functions: REGEXP_REPLACE, REPLACE, TRY_CAST
    return f"TRY_CAST(REPLACE(REGEXP_REPLACE({col_name}::VARCHAR, '[^\\d,]', '', 'g'), ',', '.') AS INTEGER)"


def unaccent_lower_sql(col_expr):
    """SQL to convert string to lowercase and remove accents for the search vector."""
    expr = f"LOWER({col_expr})"
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
        expr = f"REPLACE({expr}, '{old}', '{new}')"
    return expr


def process_csv_to_parquet(csv_path: str):
    """Converts a raw CSV to a standardized Parquet file."""
    base_name = os.path.basename(csv_path)
    parquet_dir = os.path.join(DATA_DIR, "parquet")
    os.makedirs(parquet_dir, exist_ok=True)
    parquet_path = os.path.join(parquet_dir, base_name.replace(".csv", ".parquet"))

    if os.path.exists(parquet_path):
        logging.info(f"Parquet file {parquet_path} already exists. Skipping.")
        return

    logging.info(f"Processing {csv_path}...")
    conn = duckdb.connect()

    try:
        # Read a small sample to determine actual columns
        schema_query = "SELECT * FROM read_csv(?, delim=';', encoding='latin-1', ignore_errors=true) LIMIT 0"
        df_schema = conn.execute(schema_query, [csv_path]).df()
        real_columns = set(df_schema.columns)

        select_clauses = []
        found_names = "NULL"
        found_paterno = "NULL"
        found_materno = "NULL"
        found_anyo = "NULL"

        for target_col, candidates in CONCEPT_MAPPING.items():
            found_col = "NULL"
            for candidate in candidates:
                if candidate in real_columns:
                    found_col = f'"{candidate}"'
                    break

            # Record found columns for the search vector
            if target_col == "Nombres":
                found_names = found_col
            elif target_col == "Paterno":
                found_paterno = found_col
            elif target_col == "Materno":
                found_materno = found_col
            elif target_col == "anyo":
                found_anyo = found_col

            # Handle special cleaning for money columns
            if (
                target_col in ["remuliquida_mensual", "remuneracionbruta_mensual"]
                and found_col != "NULL"
            ):
                expr = clean_money_sql(found_col)
                select_clauses.append(f"{expr} AS {target_col}")
            # Handle year column casting
            elif target_col == "anyo" and found_col != "NULL":
                select_clauses.append(
                    f"TRY_CAST({found_col} AS INTEGER) AS {target_col}"
                )
            else:
                select_clauses.append(f"{found_col} AS {target_col}")

        # Build the search vector expression
        name_concat = f"COALESCE({found_names}::VARCHAR, '') || ' ' || COALESCE({found_paterno}::VARCHAR, '') || ' ' || COALESCE({found_materno}::VARCHAR, '')"
        search_vector_expr = unaccent_lower_sql(name_concat)
        select_clauses.append(f"{search_vector_expr} AS search_vector")

        # Also append 'origen' source name based on the file type
        if "Contratohonorarios" in base_name:
            origen = "'Honorarios'"
        elif "Contrata" in base_name:
            origen = "'Contrata'"
        elif "Planta" in base_name:
            origen = "'Planta'"
        else:
            origen = "'Desconocido'"
        select_clauses.append(f"{origen} AS origen")

        select_sql = ",\n            ".join(select_clauses)

        # Build the final COPY query
        copy_query = f"""
        COPY (
            SELECT
                {select_sql}
            FROM read_csv('{csv_path}', delim=';', encoding='latin-1', ignore_errors=true, null_padding=true)
            WHERE TRY_CAST({found_anyo} AS INTEGER) BETWEEN 2000 AND 2050
        ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        logging.info(f"Executing conversion for {base_name}...")
        conn.execute(copy_query)
        logging.info(f"Successfully created {parquet_path}")

    except Exception as e:
        logging.error(f"Failed to process {csv_path}: {e}")
    finally:
        conn.close()


def main():
    if not os.path.exists(DATA_DIR):
        logging.warning(f"Data directory '{DATA_DIR}' not found.")
        return

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not csv_files:
        logging.info("No CSV files found to process.")
        return

    for csv_file in csv_files:
        process_csv_to_parquet(csv_file)

    # Pre-compute metadata
    generate_metadata_cache()


def generate_metadata_cache():
    import json

    parquet_dir = os.path.join(DATA_DIR, "parquet")
    metadata_file = os.path.join(DATA_DIR, "metadata_cache.json")
    logging.info("Generating global metadata cache...")

    conn = duckdb.connect()
    parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))

    if not parquet_files:
        logging.warning("No parquet files to cache metadata from.")
        return

    metadata = {}

    for pq_path in parquet_files:
        base_name = os.path.basename(pq_path)
        # Keep original csv name mapping for frontend compatibility
        orig_name = base_name.replace(".parquet", ".csv")

        logging.info(f"Caching metadata for {base_name}...")
        try:
            # Get years
            years_df = conn.execute(
                f"SELECT DISTINCT anyo FROM read_parquet('{pq_path}') WHERE anyo IS NOT NULL ORDER BY anyo DESC"
            ).df()
            anios = years_df["anyo"].tolist()

            # Get organizations
            orgs_df = conn.execute(
                f"SELECT DISTINCT organismo_nombre FROM read_parquet('{pq_path}') WHERE organismo_nombre IS NOT NULL ORDER BY organismo_nombre"
            ).df()
            organismos = orgs_df["organismo_nombre"].tolist()

            metadata[orig_name] = {
                "anios": [int(y) for y in anios],
                "organismos": organismos,
            }
        except Exception as e:
            logging.error(f"Error caching {pq_path}: {e}")

    # Also create a 'Todas (Búsqueda Global)' global entry
    global_years = set()
    global_orgs = set()
    for ds in metadata.values():
        global_years.update(ds.get("anios", []))
        global_orgs.update(ds.get("organismos", []))

    metadata["Todas (Búsqueda Global)"] = {
        "anios": sorted(list(global_years), reverse=True),
        "organismos": sorted(list(global_orgs)),
    }

    with open(metadata_file, "w") as f:
        json.dump(metadata, f)

    logging.info(f"Metadata saved to {metadata_file}")


if __name__ == "__main__":
    main()
