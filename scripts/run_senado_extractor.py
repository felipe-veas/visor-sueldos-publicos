import logging
import os
import sys

# Add src to PATH
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from etl.senado_scraper import SenadoScraper
from etl.senado_processor import DataProcessor
from etl.ingest import generate_metadata_cache

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():
    print("🚀 Starting Extractor - Visor de Sueldos Públicos - Senado de Chile")

    # 1. Scraping (Download data to Cache)
    scraper = SenadoScraper(start_year=2022, end_year=2026, force_refresh=False)
    scraper.run_all()

    # 2. Processing (Cross data in Pandas -> Generate CSV and Parquet)
    processor = DataProcessor(cache_dir="data/raw", output_dir="data")
    processor.process_all()

    # 3. Regenerate Global Metadata Cache
    logging.info("Regenerating local duckdb metadata...")
    generate_metadata_cache()


if __name__ == "__main__":
    main()
