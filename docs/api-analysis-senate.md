# Technical Architecture & API Analysis: Chilean Senate Transparency

## 1. Executive Summary

This document outlines the technical architecture of the Senate's transparency portal (`https://www.senado.cl/transparencia`) and details the strategy for programmatic data extraction, normalization, and analysis of financial records (remunerations, allowances, operational expenses, and per diems).

## 2. System Architecture

* **Frontend:** Next.js (React) SPA. Client-side dynamic data hydration.
* **Backend:** Headless CMS (Strapi or JSON:API enabled Drupal) resolving at `https://web-back.senado.cl/`.
* **Extraction Strategy:** **Zero HTML scraping is required**. All tabular data is exposed via unauthenticated, public REST APIs returning structured JSON payloads. This ensures high data integrity and stable extraction pipelines.

## 3. API Contract & Query Parameters

The REST APIs utilize a standard query parameter structure for filtering, sorting, and pagination.

**Base URL:** `https://web-back.senado.cl/api/transparency/`

| Parameter | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `filters[ano][$eq]` | Integer | Filters records by year. | `2024` |
| `filters[mes][$eq]` | Integer | Filters records by month (1-12). | `1` |
| `sort` | String | Comma-separated fields for sorting. | `appaterno,fecha_ida` |
| `pagination[pageSize]`| Integer | Limits the response payload size. | `500` |
| `pagination[page]` | Integer | Requests a specific page index. | `1` |

## 4. Core Endpoints (Financial Data)

These endpoints represent the primary sources for cross-referencing senatorial financial data.

### 4.1. Parliamentary Allowances (Base Salary)
* **Path:** `/diet`
* **Schema Highlights:** `rut` (National ID), `nombre`, `appaterno`, `apmaterno`, `dieta` (gross), `deducciones`, `saldo` (net).
* **Usage:** Provides the foundational baseline salary per senator and exposes the `rut` as the strongest Primary Key candidate.

### 4.2. Operational Expenses (Senators)
* **Path:** `/expenses/senator-Operational-expenses`
* **Schema Highlights:** `ano`, `mes`, `nombre`, `appaterno`, `gastos_operacionales` (enum-like categories e.g., "TELEFONIA CELULAR"), `monto`.

### 4.3. Travel & Logistics
* **Domestic Flights:** `/domestic-air-tickets`
* **Foreign Missions:** `/foreign-missions` (Includes international per diems and flights).

### 4.4. Executive & Committee Expenses
* **Committees:** `/expenses/committee-operational-expenses`
* **Presidency/Ex-Presidents:** `/president-vicepresident`, `/expenses/presidents-republic`.

## 5. Architectural Pivot: Remunerations vs. Dotations

A critical architectural decision relies on avoiding the "Remuneraciones" frontend section, which relies on opaque PDF blobs.

Instead, the pipeline **must** consume the "Dotación de Personal" (Staffing) JSON endpoints. These APIs expose the exact same salary data in machine-readable JSON formats, bypassing the need for complex OCR or PDF parsing heuristics.

**Staffing Target Endpoints:**
* **Contract:** `/dotation/staffing`
* **Permanent:** `/dotation/plant-equipment`
* **Fee-based:** `/dotation/fee`

**Schema Highlights:** Name, Surnames, Rank (`Escalafón`), Position (`Cargo`), Category, and `remuneracion` (Salary).

## 6. Implementation Strategy: ETL Pipeline

To calculate the Total Cost of Ownership (TCO) or financial footprint of a public figure, the ETL pipeline should execute the following steps:

1. **Extraction (Scraper Bot):**
   * Implement a polite HTTP client (e.g., `requests` with Tenacity for backoff).
   * Iterate over the defined `[year, month]` matrix.
   * Paginate through `/diet`, `/expenses/senator-Operational-expenses`, `/domestic-air-tickets`, and `/foreign-missions`.

2. **Transform & Link (Normalization):**
   * **Primary Linking:** Rely on the `RUT` provided in the `/diet` endpoint.
   * **Fuzzy Linking:** For endpoints lacking a `RUT`, generate a normalized search vector: `LOWER(UNACCENT(appaterno + apmaterno + nombre))`.

3. **Financial Aggregation:**
   * Compute standard metrics per entity per month:
     `Total_Cost = Gross_Allowance + SUM(Operational_Expenses) + SUM(Domestic_Flights) + SUM(International_Per_Diems)`

4. **Storage (Data Lake/Warehouse):**
   * Persist raw JSON payloads locally for auditability.
   * Transform and load structured data into DuckDB/Parquet partitions optimized for analytical querying (OLAP) by the Streamlit frontend.
