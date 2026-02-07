import pandas as pd
import requests
from tqdm import tqdm
import os

# Official Open Data URLs (Council for Transparency)
# Keys are kept for menu selection logic
URLS = {
    "1": {
        "name": "Personal de Planta",  # Display name in Spanish
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalPlanta.csv",
        "filename": "TA_PersonalPlanta.csv",
    },
    "2": {
        "name": "Personal a Contrata",  # Display name in Spanish
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContrata.csv",
        "filename": "TA_PersonalContrata.csv",
    },
    "3": {
        "name": "Personal a Honorarios",  # Display name in Spanish
        "url": "https://www.consejotransparencia.cl/transparencia_activa/datoabierto/archivos/TA_PersonalContratohonorarios.csv",
        "filename": "TA_PersonalContratohonorarios.csv",
    },
}

DATA_DIR = "data"


def ensure_directory():
    """Ensures the data directory exists."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)


def download_file(url, destination_path):
    """Downloads a large file with a progress bar."""
    print(f"Iniciando descarga desde: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        block_size = 1024  # 1 Kibibyte

        progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True)

        with open(destination_path, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()

        if total_size != 0 and progress_bar.n != total_size:
            print("ERROR: La descarga no coincide con el tamaño esperado.")
            return False

        print(f"\nDescarga completada: {destination_path}")
        return True

    except Exception as e:
        print(f"\nError durante la descarga: {e}")
        return False


def process_file(file_path, org_filter=None, year_filter=None):
    """Reads the CSV in chunks and filters results."""
    print(f"\nAnalizando {file_path}...")
    print("Este proceso puede tardar unos minutos dependiendo del tamaño del archivo.")
    print("Leyendo en bloques para optimizar memoria...")

    results = []
    total_rows_processed = 0
    matches = 0

    # Reading configuration
    CHUNK_SIZE = 50000
    ENCODING = "latin-1"  # Standard in Chilean government files
    SEP = ";"

    try:
        # Iterate over the file in chunks
        with tqdm(desc="Procesando filas", unit="filas") as pbar:
            for chunk in pd.read_csv(
                file_path,
                sep=SEP,
                encoding=ENCODING,
                chunksize=CHUNK_SIZE,
                on_bad_lines="skip",
                low_memory=False,
            ):
                # Normalize column names (remove extra spaces)
                chunk.columns = chunk.columns.str.strip()

                # Apply filters
                current_filter = chunk

                if org_filter:
                    # Case-insensitive search
                    # Note: 'organismo_nombre' is the column name in the CSV, keeping it as is
                    mask_org = (
                        current_filter["organismo_nombre"]
                        .astype(str)
                        .str.contains(org_filter, case=False, na=False)
                    )
                    current_filter = current_filter[mask_org]

                if year_filter:
                    # Convert to string for safe comparison
                    # 'anyo' is the column name in the CSV
                    mask_year = current_filter["anyo"].astype(str) == str(year_filter)
                    current_filter = current_filter[mask_year]

                if not current_filter.empty:
                    results.append(current_filter)
                    matches += len(current_filter)

                total_rows_processed += len(chunk)
                pbar.update(len(chunk))

        if results:
            print(f"\n¡Análisis completado! Se encontraron {matches} registros.")
            final_df = pd.concat(results, ignore_index=True)

            # Generate report name
            org_safe = org_filter.replace(" ", "_") if org_filter else "Todo"
            year_safe = year_filter if year_filter else "Historico"
            report_name = f"reporte_{org_safe}_{year_safe}.xlsx"

            print(f"Guardando resultados en {report_name}...")
            final_df.to_excel(report_name, index=False)
            print("Archivo guardado exitosamente.")
        else:
            print("\nNo se encontraron coincidencias con los filtros proporcionados.")

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {file_path}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


def main():
    ensure_directory()

    print("=== SCRAPER DE SUELDOS PÚBLICOS (CHILE) ===")
    print("Fuente: Portal de Transparencia - Datos Abiertos")

    print("\nSeleccione el dataset a analizar:")
    for key, val in URLS.items():
        print(f"{key}. {val['name']}")

    option = input("\nIngrese opción (1-3): ")

    if option not in URLS:
        print("Opción inválida.")
        return

    dataset = URLS[option]
    local_path = os.path.join(DATA_DIR, dataset["filename"])

    # Check if it already exists
    if os.path.exists(local_path):
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"\nEl archivo ya existe ({size_mb:.2f} MB).")
        use_existing = input("¿Desea usar el archivo existente? (s/n): ").lower()
        if use_existing != "s":
            download_file(dataset["url"], local_path)
    else:
        print("\nEl archivo no existe localmente.")
        confirm = input(
            "El archivo puede pesar varios GB. ¿Desea descargarlo ahora? (s/n): "
        ).lower()
        if confirm == "s":
            success = download_file(dataset["url"], local_path)
            if not success:
                return
        else:
            print("Operación cancelada.")
            return

    # Filters
    print("\n--- Configuración de Filtros ---")
    org_input = input(
        "Ingrese nombre del organismo (ej: 'Presidencia', 'Salud', 'Carabineros') o Enter para todos: "
    ).strip()
    year_input = input("Ingrese año (ej: 2024) o Enter para todos: ").strip()

    if not org_input and not year_input:
        print(
            "ADVERTENCIA: No ha seleccionado filtros. Procesar todo el archivo y exportarlo a Excel puede fallar por tamaño."
        )
        confirm = input("¿Está seguro? (s/n): ")
        if confirm.lower() != "s":
            return

    process_file(
        local_path,
        org_filter=org_input if org_input else None,
        year_filter=year_input if year_input else None,
    )


if __name__ == "__main__":
    main()
