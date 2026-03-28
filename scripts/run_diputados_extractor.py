#!/usr/bin/env python3
import sys
import os

# Ensure the root of the project is in the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from src.etl.diputados_scraper import DiputadosScraper
from src.etl.diputados_processor import DiputadosProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    logger = logging.getLogger("DiputadosPipeline")
    logger.info("Starting Cámara de Diputados Extraction Pipeline")

    scraper = DiputadosScraper(start_year=2022, end_year=2026, force_refresh=False)
    scraper.run_all()

    processor = DiputadosProcessor()
    processor.process_all()

    try:
        from src.etl.ingest import generate_metadata_cache

        generate_metadata_cache()
    except Exception as e:
        logger.warning(f"Failed to generate metadata cache: {e}")

    logger.info("Pipeline completed.")


if __name__ == "__main__":
    main()
