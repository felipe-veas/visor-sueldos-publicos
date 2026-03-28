import os
import json
import time
import random
import logging
import requests
from requests.exceptions import RequestException
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SenadoAPI")


class RateLimitError(Exception):
    """Exception thrown when a 429 Too Many Requests error occurs."""

    pass


class SenadoAPIClient:
    """
    Defensive HTTP client to extract data from the Senate.
    Includes rate limiting (delays), exponential retries, and disk caching.
    """

    def __init__(self, base_cache_dir="data/raw"):
        self.base_cache_dir = base_cache_dir
        self.session = requests.Session()

        # Simple rotation or standard header to prevent basic blocks
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-CL,es;q=0.9,en-US;q=0.8,en;q=0.7",
                "Origin": "https://www.senado.cl",
                "Referer": "https://www.senado.cl/",
            }
        )

        # Create base directory if it doesn't exist
        os.makedirs(self.base_cache_dir, exist_ok=True)

    def _get_cache_path(self, endpoint_name, year, month):
        """Builds and creates (if necessary) the cache file path."""
        dir_path = os.path.join(self.base_cache_dir, endpoint_name, str(year))
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f"{month:02d}.json")

    @retry(
        wait=wait_exponential(
            multiplier=2, min=4, max=60
        ),  # Wait 4, 8, 16, 32, 60... seconds
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((RequestException, RateLimitError)),
    )
    def _fetch_with_retry(self, url):
        """Executes the HTTP request with retries and a polite delay."""
        # Politeness delay: Wait between 1.5 and 3.5 random seconds
        delay = random.uniform(1.5, 3.5)
        logger.debug(f"Waiting {delay:.2f}s before request...")
        time.sleep(delay)

        logger.info(f"Downloading API: {url}")
        response = self.session.get(url, timeout=20)

        if response.status_code == 429:
            logger.warning("Temporary block detected (Status 429). Starting backoff...")
            raise RateLimitError("Rate limit exceeded on Senado.")

        response.raise_for_status()
        return response.json()

    def get_data(self, endpoint_name, url, year, month, force_refresh=False):
        """
        Gets the data from the local cache or, if it doesn't exist, calls the API.

        :param endpoint_name: Logical name for the folder (e.g., 'dietas', 'gastos_op')
        :param url: Full API URL with applied filters
        :param year: Year (e.g., 2024)
        :param month: Month (1 to 12)
        :param force_refresh: If True, ignores cache and downloads again
        :return: Dictionary with the JSON response or None if it fails.
        """
        cache_path = self._get_cache_path(endpoint_name, year, month)

        # 1. Try to load from Cache
        if not force_refresh and os.path.exists(cache_path):
            logger.debug(f"Loaded from cache: {cache_path}")
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(
                    f"Corrupted cache file in {cache_path}. Will download again."
                )

        # 2. Download if there is no cache or if it is corrupted/forced
        try:
            data = self._fetch_with_retry(url)

            # 3. Save to Cache
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            return data

        except Exception as e:
            logger.error(
                f"Definitive error getting data for {endpoint_name} ({year}-{month}): {str(e)}"
            )
            return None
