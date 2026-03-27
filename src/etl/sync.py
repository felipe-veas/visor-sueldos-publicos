import os
import requests
import logging
from email.utils import parsedate_to_datetime
import datetime
import subprocess

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_DIR = "data"

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


def get_remote_metadata(url):
    """Fetches the Last-Modified and Content-Length using a lightweight HEAD request."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
        }
        response = requests.head(url, headers=headers, timeout=10)
        response.raise_for_status()

        content_length = int(response.headers.get("Content-Length", 0))
        last_modified_str = response.headers.get("Last-Modified")

        last_modified = None
        if last_modified_str:
            last_modified = parsedate_to_datetime(last_modified_str)
            # Make timezone naive UTC for simple comparison
            if last_modified.tzinfo:
                last_modified = last_modified.replace(tzinfo=None)

        return content_length, last_modified
    except Exception as e:
        logging.error(f"Failed to fetch metadata for {url}: {e}")
        return None, None


def download_file(url, file_path):
    """Downloads the file with a basic progress indication."""
    logging.info(f"Downloading new data from {url} to {file_path}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    # We download to a .part file first to avoid corruption if interrupted
    part_path = file_path + ".part"
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(part_path, "wb") as f:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024 * 5
            ):  # 5MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        logging.info(
                            f"Downloaded: {downloaded / (1024 * 1024):.1f} MB / {total_size / (1024 * 1024):.1f} MB"
                        )

        # Replace the old file
        os.replace(part_path, file_path)
        logging.info(f"Download complete for {file_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to download {url}: {e}")
        if os.path.exists(part_path):
            os.remove(part_path)
        return False


def check_and_sync():
    """Checks all datasets and downloads them if they are outdated."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    updates_made = False

    for name, config in DATASETS_CONFIG.items():
        url = config["url"]
        file_path = os.path.join(DATA_DIR, config["filename"])

        logging.info(f"Checking status for {name}...")

        remote_size, remote_modified = get_remote_metadata(url)

        if remote_size == 0 or remote_size is None:
            logging.warning(f"Could not verify remote metadata for {name}. Skipping.")
            continue

        needs_download = False

        if not os.path.exists(file_path):
            logging.info(f"File {file_path} does not exist locally. Must download.")
            needs_download = True
        else:
            local_size = os.path.getsize(file_path)
            local_mtime = datetime.datetime.utcfromtimestamp(
                os.path.getmtime(file_path)
            )

            # If the sizes differ by more than a few bytes, it's a new file (Council files usually grow)
            if local_size != remote_size:
                logging.info(
                    f"Size mismatch for {name}: Local ({local_size} bytes) vs Remote ({remote_size} bytes). Must download."
                )
                needs_download = True
            elif remote_modified and local_mtime < remote_modified:
                logging.info(
                    f"Date mismatch for {name}: Remote is newer. Must download."
                )
                needs_download = True
            else:
                logging.info(
                    f"✅ {name} is fully up-to-date (Size: {local_size / (1024 * 1024 * 1024):.2f} GB)."
                )

        if needs_download:
            # First, clean up the old Parquet so the ingest script knows it must be regenerated
            parquet_path = file_path.replace(".csv", ".parquet")
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
                logging.info(f"Removed outdated parquet: {parquet_path}")

            success = download_file(url, file_path)
            if success:
                # Set the local file timestamp to match the remote server's for future checks
                if remote_modified:
                    timestamp = remote_modified.replace(
                        tzinfo=datetime.timezone.utc
                    ).timestamp()
                    os.utime(file_path, (timestamp, timestamp))
                updates_made = True

    if updates_made:
        logging.info(
            "Updates were downloaded. Triggering Parquet ingestion pipeline..."
        )
        subprocess.run(["uv", "run", "src/etl/ingest.py"], check=True)
    else:
        logging.info("All files are up-to-date. No ingestion needed.")


if __name__ == "__main__":
    check_and_sync()
