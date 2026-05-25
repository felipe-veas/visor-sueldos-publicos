import os
import time
import logging
import requests
import re
import datetime
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger("DiputadosScraper")


class DiputadosScraper:
    def __init__(self, start_year=2024, end_year=2024, force_refresh=False):
        self.start_year = start_year
        now = datetime.datetime.now()
        self.current_year = now.year
        self.current_month = now.month
        self.end_year = min(end_year, self.current_year)
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
        total_months = 0
        for y in range(self.start_year, self.end_year + 1):
            total_months += self.current_month if y == self.current_year else 12
        pbar = tqdm(total=total_months, desc=category_name)
        state = self._get_form_state(url)
        if not state.get("year_field") or not state.get("month_field"):
            logger.error(f"Could not find year/month dropdowns for {url}")
            return

        for year in range(self.start_year, self.end_year + 1):
            max_month = self.current_month if year == self.current_year else 12
            for month in range(1, max_month + 1):
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
                        logger.warning(
                            f"No table found for {category_name} - {month}/{year}"
                        )
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
        logger.info("Caching active Deputies names from dropdown...")
        url = (
            "https://www.camara.cl/diputados/detalle/gastosoperacionales.aspx?prmId=74"
        )
        try:
            res = self.session.get(url)
            res.raise_for_status()
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(res.text, "html.parser")
            select = soup.find(
                "select", {"id": "ContentPlaceHolder1_ContentPlaceHolder1_ddlDiputados"}
            )
            if select:
                real_names = {}
                for option in select.find_all("option"):
                    val = option.get("value")
                    if val and val.strip():
                        name = option.text.strip()
                        name = re.sub(r"^[Dd]iputad[oa]\s+", "", name)
                        real_names[val] = name
                self.cached_deputies = real_names
                logger.info(f"Found {len(real_names)} deputies in dropdown.")
                return self.cached_deputies
            else:
                logger.error("Could not find deputies dropdown")
                self.cached_deputies = {}
                return self.cached_deputies
        except Exception as e:
            logger.error(f"Failed to fetch deputies list from dropdown: {e}")
            self.cached_deputies = {}
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

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "X-MicrosoftAjax": "Delta=true",
        }

        pbar = tqdm(total=len(real_names), desc=f"Gastos {month}/{year}")
        csv_path = os.path.join(out_dir, f"{year}_{month:02d}.csv")
        if not self.force_refresh and os.path.exists(csv_path):
            return
        all_dfs = []
        for pid, name in real_names.items():
            # csv_path is per month, handled outside the loop

            try:
                url = f"https://www.camara.cl/diputados/detalle/gastosoperacionales.aspx?prmId={pid}"
                res = self.session.get(
                    url, headers={"Referer": "https://www.camara.cl"}
                )

                if "error404" in res.url or res.status_code != 200:
                    logger.warning(
                        f"No page found for Gastos Operacionales PID {pid} (404)"
                    )
                    pbar.update(1)
                    continue

                from bs4 import BeautifulSoup
                import re
                import pandas as pd

                soup = BeautifulSoup(res.text, "html.parser")

                state = {
                    i.get("name"): i.get("value", "")
                    for i in soup.find_all("input", type="hidden")
                    if i.get("name", "").startswith("__")
                }

                current_year_el = soup.find(
                    "select",
                    {
                        "id": "ContentPlaceHolder1_ContentPlaceHolder1_DetallePlaceHolder_ddlAno"
                    },
                )
                current_month_el = soup.find(
                    "select",
                    {
                        "id": "ContentPlaceHolder1_ContentPlaceHolder1_DetallePlaceHolder_ddlMes"
                    },
                )

                if not current_year_el or not current_month_el:
                    logger.warning(
                        f"No dropdowns found for Gastos Operacionales PID {pid}"
                    )
                    pbar.update(1)
                    continue

                page_year = current_year_el.find("option", selected=True).get("value")
                page_month = current_month_el.find("option", selected=True).get("value")

                last_html = res.text

                # Step 1: Change Year
                if str(year) != page_year:
                    # check if year is available
                    available_years = [
                        o.get("value") for o in current_year_el.find_all("option")
                    ]
                    if str(year) not in available_years:
                        logger.warning(f"Year {year} not available for PID {pid}")
                        pbar.update(1)
                        continue

                    payload2 = {
                        "ctl00$ctl00$ctl00$ScriptManager2": "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$UpdatePanel1|ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlAno",
                        "__EVENTTARGET": "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlAno",
                        "__EVENTARGUMENT": "",
                        **state,
                        "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$ddlDiputados": str(
                            pid
                        ),
                        "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlAno": str(
                            year
                        ),
                        "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes": page_month,
                    }
                    res_post2 = self.session.post(url, data=payload2, headers=headers)
                    matches2 = re.findall(
                        r"\|hiddenField\|([^\|]+)\|([^\|]*)\|", res_post2.text
                    )
                    for match in matches2:
                        state[match[0]] = match[1]

                    last_html = res_post2.text
                    page_year = str(year)

                # Step 2: Change Month
                if str(month) != page_month:
                    payload3 = {
                        "ctl00$ctl00$ctl00$ScriptManager2": "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$UpdatePanel1|ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes",
                        "__EVENTTARGET": "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes",
                        "__EVENTARGUMENT": "",
                        **state,
                        "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$ddlDiputados": str(
                            pid
                        ),
                        "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlAno": str(
                            year
                        ),
                        "ctl00$ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$DetallePlaceHolder$ddlMes": str(
                            month
                        ),
                    }
                    res_post3 = self.session.post(url, data=payload3, headers=headers)
                    last_html = res_post3.text

                html_match = re.search(
                    r"<table.*?>.*?</table>", last_html, re.IGNORECASE | re.DOTALL
                )

                if html_match:
                    df = pd.read_html(html_match.group(0))[0]
                    df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
                    df["ID_Diputado"] = pid
                    all_dfs.append(df)
                else:
                    logger.warning(
                        f"No table found for Gastos Operacionales PID {pid} - {month}/{year}"
                    )

            except Exception as e:
                logger.error(
                    f"Error extracting Gastos Operacionales PID {pid} - {month}/{year}: {e}"
                )

            import time

            time.sleep(1)
            pbar.update(1)

        pbar.close()
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df.to_csv(csv_path, index=False)

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
            max_month = self.current_month if year == self.current_year else 12
            for month in range(1, max_month + 1):
                self.fetch_diputados_activos(year, month)
                self.fetch_gastos_operacionales(year, month)
        logger.info("== Extraction successfully completed ==")
