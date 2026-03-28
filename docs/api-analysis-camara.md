# Technical Architecture & API Analysis: Chilean Chamber of Deputies Transparency

## 1. Executive Summary

This document outlines the technical architecture of the Chamber of Deputies transparency portal (`https://www.camara.cl/transparencia/transparencia_activa.aspx`) and details the strategy for programmatic data extraction, normalization, and analysis of financial records (remunerations, allowances, operational expenses, and per diems).

Unlike the Senate, the Chamber of Deputies does not offer modern JSON REST APIs for financial data. Instead, it relies on legacy web rendering techniques.

## 2. System Architecture

* **Frontend/Backend:** Legacy monolithic ASP.NET Web Forms (`.aspx`) with server-side rendered HTML and AJAX UpdatePanels.
* **Open Data Portal:** The Chamber provides an Open Data portal (`opendata.camara.cl`) and API endpoints, **however**, these are strictly limited to legislative and biographical data. Financial transparency data is generally excluded from the structured APIs.
* **Extraction Strategy:** **Stateful HTML scraping is strictly required**. Financial and staffing data is rendered dynamically as HTML tables (`<table class="tabla">`) or static PDF files. The crawler must manage complex ASP.NET postbacks to paginate and filter records.

## 3. Query Contract & State Management

Instead of simple REST API query parameters, filtering by year/month and pagination are controlled via HTTP `POST` requests containing hidden form fields.

**Base URL:** `https://www.camara.cl/transparencia/`

| Form Parameter | Description | Handling Strategy |
| :--- | :--- | :--- |
| `__VIEWSTATE` | Encoded page state. | Must be extracted via regex/BeautifulSoup from the initial `GET` request and included in subsequent `POST`s. |
| `__EVENTVALIDATION` | Security token. | Extract alongside `__VIEWSTATE`. |
| `ctl00$ctl00$...$ddlAno` | Year dropdown. | Set to desired year (e.g., `2024`). |
| `ctl00$ctl00$...$ddlMes` | Month dropdown. | Set to desired month index (`1`-`12`). |
| `prmId` | URL parameter for Deputy IDs. | Used in `GET` requests for specific deputy profiles. |

## 4. Core Endpoints (Financial Data)

These endpoints represent the primary sources for cross-referencing deputational financial data.

### 4.1. Parliamentary Allowances (Base Salary / Dieta)
* **Path:** `/transparencia/doc/dieta_actualizada.pdf`
* **Architecture:** Static PDF blob.
* **Constraint:** The site explicitly states deputies earn the equivalent of a Minister of State and provides a single static PDF rather than historical tables. The base salary will either need to be hardcoded per period based on the state scale or parsed from this single file.

### 4.2. Operational Expenses & Allowances (Deputies)
* **Path:** `/diputados/detalle/gastosoperacionales.aspx?prmId={ID}`
* **Architecture:** Server-side rendered HTML tables.
* **Usage:** Iterate through all known Deputy IDs (`prmId`). The panel contains separate sub-sections requiring individual queries:
  - Gastos operacionales
  - Asesorías externas (`asesoriaexterna.aspx`)
  - Pasajes aéreos nacionales (`pasajesaereos.aspx`)
  - Personal de apoyo (`personaldepoyo.aspx`)

### 4.3. Committee Staff Expenses
* **Path:** `/transparencia/comistesparlamentarios.aspx`
* **Architecture:** ASP.NET HTML Form with `__VIEWSTATE`.

## 5. Architectural Pivot: Remuneraciones vs. Dotations

Similar to the Senate, a critical architectural decision relies on avoiding the "Remuneraciones" frontend section (`/transparencia/RemuneracionDetalle.aspx`). This section often relies on opaque, scanned PDF blobs per month (e.g., `doc/remfunc_nov24.pdf`), which are extremely difficult to parse programmatically.

Instead, the pipeline **must** consume the "Personal" (Staffing) `.aspx` pages. These pages expose the exact same salary and ranking data as traversable HTML tables, bypassing the need for complex OCR heuristics.

**Staffing Target Pages:**
* **Permanent (Planta):** `/transparencia/funcionariosplanta.aspx`
* **Contract (Contrata):** `/transparencia/funcionarios.aspx`
* **Fee-based (Honorarios):** `/transparencia/honorarios.aspx`
* **Support Staff (Apoyo):** `/transparencia/personalapoyogral.aspx`

## 6. Implementation Strategy: ETL Pipeline

To calculate the financial footprint of a public figure in the Chamber of Deputies, the ETL pipeline should execute the following steps:

1. **Extraction (Stateful Scraper Bot):**
   * Implement a stateful HTTP client (e.g., `requests.Session()`) to maintain session cookies.
   * **Initialization:** For each target page, perform an initial `GET` to extract `__VIEWSTATE`, `__VIEWSTATEGENERATOR`, and `__EVENTVALIDATION`.
   * **Iteration:** Submit `POST` requests modifying the Year and Month dropdown form fields.
   * Parse the resulting `<table class="tabla">` elements using `BeautifulSoup` or `pd.read_html()` into Pandas DataFrames.

2. **Transform & Link (Normalization):**
   * The Chamber of Deputies data typically lacks the raw National ID (`RUT`) as a clean primary key in public tables.
   * **Fuzzy Linking:** Generate a normalized search vector: `LOWER(UNACCENT(Nombre + Apellido))`. Cross-reference this against the open legislative API (`http://opendata.camara.cl/camaradiputados/pages/diputado/retornarDiputados.aspx`) to map internal IDs (`prmId`) back to real names.

3. **Financial Aggregation:**
   * Compute standard metrics per entity per month:
     `Total_Cost = Hardcoded_Minister_Allowance + SUM(Operational_Expenses) + SUM(Domestic_Flights) + SUM(Staffing_Costs)`

4. **Storage (Data Lake/Warehouse):**
   * Persist raw HTML table outputs locally (or directly as CSV) for auditability.
   * Transform and load structured data into DuckDB/Parquet partitions optimized for analytical querying (OLAP), ensuring the schema matches the one generated by `src/etl/ingest.py`.
