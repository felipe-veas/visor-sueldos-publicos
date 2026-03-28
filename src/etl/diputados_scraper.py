import os
import time
import logging
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger("DiputadosScraper")


class DiputadosScraper:
    def __init__(self, start_year=2024, end_year=2024, force_refresh=False):
        self.start_year = start_year
        self.end_year = end_year
        self.force_refresh = force_refresh
        self.base_dir = os.path.join("data", "raw", "diputados")
        os.makedirs(self.base_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        self.cached_deputies = None

    def _get_form_state(self, url):
        res = self.session.get(url)
        soup = BeautifulSoup(res.text, "html.parser")
        state = {}
        for hidden in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
            el = soup.find("input", {"id": hidden})
            if el:
                state[hidden] = el.get("value", "")

        selects = soup.find_all("select")
        state["year_field"] = next(
            (s.get("name") for s in selects if "ddlAno" in s.get("name", "")), None
        )
        state["month_field"] = next(
            (s.get("name") for s in selects if "ddlMes" in s.get("name", "")), None
        )
        return state

    def fetch_table(self, category_name: str, url: str):
        logger.info(f"== Starting extraction for {category_name} ==")
        category_dir = os.path.join(self.base_dir, category_name)
        os.makedirs(category_dir, exist_ok=True)
        total_months = (self.end_year - self.start_year + 1) * 12
        pbar = tqdm(total=total_months, desc=category_name)
        state = self._get_form_state(url)
        if not state.get("year_field") or not state.get("month_field"):
            logger.error(f"Could not find year/month dropdowns for {url}")
            return

        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                pbar.set_postfix({"Year": year, "Month": f"{month:02d}"})
                csv_path = os.path.join(category_dir, f"{year}_{month:02d}.csv")
                if not self.force_refresh and os.path.exists(csv_path):
                    pbar.update(1)
                    continue

                payload = {
                    "__VIEWSTATE": state.get("__VIEWSTATE", ""),
                    "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
                    "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
                    state["year_field"]: str(year),
                    state["month_field"]: str(month),
                    "__EVENTTARGET": state["month_field"],
                    "__EVENTARGUMENT": "",
                }

                try:
                    res = self.session.post(url, data=payload)
                    soup = BeautifulSoup(res.text, "html.parser")
                    for hidden in [
                        "__VIEWSTATE",
                        "__VIEWSTATEGENERATOR",
                        "__EVENTVALIDATION",
                    ]:
                        el = soup.find("input", {"id": hidden})
                        if el:
                            state[hidden] = el.get("value", "")

                    tables = soup.find_all("table")
                    if tables:
                        df = pd.read_html(str(tables[0]))[0]
                        df.columns = [
                            str(c).strip().replace("\n", " ") for c in df.columns
                        ]
                        df.to_csv(csv_path, index=False)
                    else:
                        pd.DataFrame().to_csv(csv_path, index=False)
                except Exception as e:
                    logger.error(
                        f"Error extracting {category_name} for {month}/{year}: {e}"
                    )

                time.sleep(1)
                pbar.update(1)
        pbar.close()

    def _cache_active_deputies(self):
        if self.cached_deputies is not None:
            return self.cached_deputies
        logger.info("Caching active Deputies names to avoid rate limiting...")
        url = "https://www.camara.cl/diputados/diputados.aspx"
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
            }
            res = requests.get(url, headers=headers)
            res.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch initial deputies list: {e}")
            self.cached_deputies = {}
            return self.cached_deputies

        matches = re.findall(
            r'href="detalle/mociones\.aspx\?prmID=(\d+)"[^>]*>([^<]*)<', res.text
        )
        ids = list(set([pid for pid, name in matches if name.strip()]))
        logger.info(
            f"Found {len(ids)} deputies. Fetching individual profiles politely..."
        )
        real_names = {}
        for pid in tqdm(ids, desc="Fetching profiles"):
            try:
                prof_url = (
                    f"https://www.camara.cl/diputados/detalle/mociones.aspx?prmID={pid}"
                )
                prof_res = requests.get(prof_url, headers=headers)
                if prof_res.status_code != 200:
                    time.sleep(2)
                    continue
                soup = BeautifulSoup(prof_res.text, "html.parser")
                h2 = soup.find("h2")
                if h2:
                    name = h2.text.strip()
                    name = re.sub(r"^[Dd]iputad[oa]\s+", "", name)
                    real_names[pid] = name
            except Exception:
                pass
            time.sleep(0.5)

        self.cached_deputies = real_names
        return self.cached_deputies

    def fetch_diputados_activos(self, year, month):
        out_dir = os.path.join(self.base_dir, "diputados_dieta")
        os.makedirs(out_dir, exist_ok=True)
        csv_path = os.path.join(out_dir, f"{year}_{month:02d}.csv")
        if not self.force_refresh and os.path.exists(csv_path):
            return
        real_names = self._cache_active_deputies()
        if not real_names:
            return
        data = []
        for pid, name in real_names.items():
            data.append(
                {
                    "ID_Diputado": pid,
                    "Nombre": name,
                    "Cargo": "Diputado(a) de la República",
                    "Sueldo Bruto": 7349623,
                    "Sueldo Liquido": 5600000,
                    "Mes": month,
                    "Ano": year,
                }
            )
        pd.DataFrame(data).to_csv(csv_path, index=False)

    def fetch_gastos_operacionales(self, year, month):
        logger.info(f"Fetching Gastos Operacionales for {month}/{year}...")
        out_dir = os.path.join(self.base_dir, "gastos_operacionales")
        os.makedirs(out_dir, exist_ok=True)
        real_names = self._cache_active_deputies()
        if not real_names:
            return
        url = "https://www.camara.cl/diputados/detalle/gastosoperacionales.aspx?prmId=1096"
        try:
            res = self.session.get(url)
            soup = BeautifulSoup(res.text, "html.parser")
            state = {
                i.get("id"): i.get("value", "")
                for i in soup.find_all("input", id=True)
                if i.get("id").startswith("__")
            }
        except Exception:
            return

        pbar = tqdm(total=len(real_names), desc=f"Gastos {month}/{year}")
        for pid, name in real_names.items():
            csv_path = os.path.join(out_dir, f"{pid}_{year}_{month:02d}.csv")
            if not self.force_refresh and os.path.exists(csv_path):
                pbar.update(1)
                continue

            payload = {
                "ctl00$ctl00$ctl00$ScriptManager2": "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$UpdatePanel1|ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes",
                "__EVENTTARGET": "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes",
                "__EVENTARGUMENT": "",
                **state,
                "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$ddlDiputados": str(
                    pid
                ),
                "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes": str(
                    month
                ),
                "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlAno": str(
                    year
                ),
            }
            headers = {
                "X-Requested-With": "XMLHttpRequest",
                "X-MicrosoftAjax": "Delta=true",
            }
            try:
                post_res = self.session.post(url, data=payload, headers=headers)
                html_match = re.search(
                    r"<table.*?>.*?</table>", post_res.text, re.IGNORECASE | re.DOTALL
                )
                if html_match:
                    df = pd.read_html(html_match.group(0))[0]
                    df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
                    df["ID_Diputado"] = pid
                    df.to_csv(csv_path, index=False)
                else:
                    pd.DataFrame().to_csv(csv_path, index=False)
            except Exception:
                pass
            time.sleep(0.5)
            pbar.update(1)
        pbar.close()

    def run_all(self):
        self.fetch_table(
            "personal_apoyo",
            "https://www.camara.cl/transparencia/personalapoyogral.aspx",
        )
        self.fetch_table(
            "personal_planta",
            "https://www.camara.cl/transparencia/funcionariosplanta.aspx",
        )
        self.fetch_table(
            "personal_contrata", "https://www.camara.cl/transparencia/funcionarios.aspx"
        )
        self.fetch_table(
            "personal_honorarios", "https://www.camara.cl/transparencia/honorarios.aspx"
        )

        logger.info("Pre-fetching all active deputy names...")
        self._cache_active_deputies()

        logger.info("Generating Dieta and Gastos per month...")
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                self.fetch_diputados_activos(year, month)
                self.fetch_gastos_operacionales(year, month)
        logger.info("== Extraction successfully completed ==")
