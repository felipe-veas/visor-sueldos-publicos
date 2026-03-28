import os
import json
import logging
from core.api_client import SenadoAPIClient
from tqdm import tqdm

logger = logging.getLogger("SenadoScraper")


class SenadoScraper:
    def __init__(self, start_year=2022, end_year=2024, force_refresh=False):
        self.start_year = start_year
        self.end_year = end_year
        self.force_refresh = force_refresh
        self.api_client = SenadoAPIClient(base_cache_dir="data/raw/senado")
        self.base_url = "https://web-back.senado.cl/api/transparency"

    def _build_url(
        self, endpoint: str, year: int, month: int, page_size: int = 500, page: int = 1
    ) -> str:
        """Builds the URL with year, month, and pagination filters."""
        url = f"{self.base_url}/{endpoint}?"
        url += f"filters[ano][$eq]={year}&filters[mes][$eq]={month}"
        url += f"&pagination[pageSize]={page_size}&pagination[page]={page}"
        return url

    def fetch_category(self, category_name: str, endpoint: str):
        """Downloads and iterates months and years for a specific category resolving API pagination."""
        logger.info(
            f"== Starting extraction for {category_name} ({self.start_year}-{self.end_year}) =="
        )

        total_months = (self.end_year - self.start_year + 1) * 12
        pbar = tqdm(total=total_months, desc=category_name)

        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                pbar.set_postfix({"Year": year, "Month": f"{month:02d}"})

                cache_path = self.api_client._get_cache_path(category_name, year, month)
                if not self.force_refresh and os.path.exists(cache_path):
                    pbar.update(1)
                    continue

                all_pages_data = []
                current_page = 1
                total_pages = 1

                while current_page <= total_pages:
                    url = self._build_url(
                        endpoint, year, month, page_size=500, page=current_page
                    )

                    try:
                        response_json = self.api_client._fetch_with_retry(url)

                        if "data" in response_json and isinstance(
                            response_json["data"], dict
                        ):
                            # Strapi v4 paginated format
                            page_items = response_json["data"].get("data", [])
                            meta = response_json["data"].get("meta", {})

                            all_pages_data.extend(page_items)

                            if "pagination" in meta:
                                total_pages = meta["pagination"].get("pageCount", 1)
                            else:
                                total_pages = 1
                        elif "data" in response_json and isinstance(
                            response_json["data"], list
                        ):
                            # Simple list
                            all_pages_data.extend(response_json["data"])
                            total_pages = 1
                        else:
                            total_pages = 1

                    except Exception as e:
                        logger.error(f"Error extracting {url}: {e}")
                        break

                    current_page += 1

                # Save the consolidated JSON with all pages to disk (only if data was retrieved)
                if all_pages_data:
                    consolidated_json = {
                        "data": {
                            "data": all_pages_data,
                            "meta": {
                                "pagination": {
                                    "page": 1,
                                    "pageSize": len(all_pages_data),
                                    "pageCount": 1,
                                    "total": len(all_pages_data),
                                }
                            },
                        }
                    }
                    with open(cache_path, "w", encoding="utf-8") as f:
                        json.dump(consolidated_json, f, ensure_ascii=False, indent=2)

                pbar.update(1)

        pbar.close()

    def run_all(self):
        """Executes the download of the main data sources for senators."""
        self.fetch_category("dietas", "diet")
        self.fetch_category(
            "gastos_operacionales", "expenses/senator-Operational-expenses"
        )
        self.fetch_category("viajes_nacionales", "domestic-air-tickets")
        self.fetch_category("misiones_extranjero", "foreign-missions")
        # Fee staff are very heavy (sometimes 15,000+ records/month).
        # They are omitted by default if only the Senator is relevant, or they can be uncommented.
        # self.fetch_category("dotacion_contrata", "dotation/staffing")
        # self.fetch_category("dotacion_honorarios", "dotation/fee")

        logger.info("== Extraction successfully completed ==")
