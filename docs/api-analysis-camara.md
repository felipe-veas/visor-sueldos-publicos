# API Analysis: Chilean Chamber of Deputies

## Overview

This document covers the data extraction strategy for the Chamber of Deputies transparency portal (`https://www.camara.cl/transparencia/transparencia_activa.aspx`). We extract remunerations, allowances, operational expenses, and per diems.

The Chamber of Deputies does not expose financial data via REST APIs. The platform uses legacy ASP.NET Web Forms. Extraction requires stateful scraping.

## System Architecture

* **Stack:** Monolithic ASP.NET Web Forms (`.aspx`). HTML is server-side rendered, with state mutations handled via AJAX UpdatePanels.
* **Open Data Portal:** `opendata.camara.cl` exists but only serves legislative and biographical data. Financial records are missing.
* **Extraction:** We scrape HTML tables (`<table class="tabla">`). The crawler must maintain session state and handle ASP.NET postbacks to navigate pages and filter data.

## State Management and Querying

Filtering and pagination rely on HTTP `POST` requests carrying hidden form state, not query parameters.

**Base URL:** `https://www.camara.cl/transparencia/`

| Parameter | Purpose | Handling |
| :--- | :--- | :--- |
| `__VIEWSTATE` | Encodes UI state. | Extract from the initial `GET` response body. Inject into subsequent `POST` requests. |
| `__EVENTVALIDATION` | ASP.NET security token. | Extract alongside `__VIEWSTATE`. |
| `ctl00$ctl00$...$ddlAno` | Year selection. | Set to the target year (e.g., `2024`). |
| `ctl00$ctl00$...$ddlMes` | Month selection. | Set to the target month index (`1`-`12`). |
| `prmId` | Deputy ID. | Passed as a query parameter in `GET` requests to load specific profiles. |

## Financial Endpoints

We cross-reference data from these primary endpoints.

### Base Salary (Dieta)
* **Path:** `/transparencia/doc/dieta_actualizada.pdf`
* **Format:** Static PDF.
* **Extraction:** The site pins deputy salaries to the Minister of State pay scale. Since this is published as a single static PDF rather than historical tables, we hardcode the base salary per legislative period.

### Operational Expenses
* **Path:** `/diputados/detalle/gastosoperacionales.aspx?prmId={ID}`
* **Format:** Server-rendered HTML.
* **Extraction:** Iterate through known Deputy IDs (`prmId`). The page contains tabs that require distinct requests:
  - Gastos operacionales
  - Asesorías externas (`asesoriaexterna.aspx`)
  - Pasajes aéreos nacionales (`pasajesaereos.aspx`)
  - Personal de apoyo (`personaldepoyo.aspx`)

### Committee Staff Expenses
* **Path:** `/transparencia/comistesparlamentarios.aspx`
* **Format:** ASP.NET form requiring `__VIEWSTATE`.

## Bypassing PDFs for Remuneration Data

The frontend's "Remuneraciones" section (`/transparencia/RemuneracionDetalle.aspx`) serves scanned PDFs (e.g., `doc/remfunc_nov24.pdf`). Parsing these via OCR introduces unacceptable error rates.

We bypass this by targeting the "Personal" (Staffing) `.aspx` pages. These render the same salary and rank data as HTML tables, guaranteeing deterministic extraction.

**Target Paths:**
* **Permanent Staff:** `/transparencia/funcionariosplanta.aspx`
* **Contract Staff:** `/transparencia/funcionarios.aspx`
* **Fee-based Contractors:** `/transparencia/honorarios.aspx`
* **Deputy Support Staff:** `/transparencia/personalapoyogral.aspx`

## ETL Pipeline

The pipeline follows these steps to compute the financial footprint of each deputy:

1. **Extraction:**
   * Run a stateful HTTP client (e.g., `requests.Session()`) to persist cookies.
   * Send an initial `GET` to extract `__VIEWSTATE`, `__VIEWSTATEGENERATOR`, and `__EVENTVALIDATION`.
   * Iterate over target periods by issuing `POST` requests with updated Year and Month fields.
   * Parse the returned `<table class="tabla">` nodes into dataframes.

2. **Normalization:**
   * The Chamber's public tables do not include the National ID (`RUT`).
   * Generate a search vector to use as a composite key: `LOWER(UNACCENT(Nombre + Apellido))`.
   * Cross-reference this vector against the open legislative API (`http://opendata.camara.cl/camaradiputados/pages/diputado/retornarDiputados.aspx`) to resolve internal IDs (`prmId`) back to full names.

3. **Aggregation:**
   * Compute monthly costs per deputy:
     `Total_Cost = Hardcoded_Minister_Allowance + SUM(Operational_Expenses) + SUM(Domestic_Flights) + SUM(Staffing_Costs)`

4. **Storage:**
   * Write raw HTML table extracts to local storage to maintain an audit trail.
   * Transform and load the data into DuckDB/Parquet partitions for OLAP workloads, matching the schema defined in `src/etl/ingest.py`.
