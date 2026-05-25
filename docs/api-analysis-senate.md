# API Analysis: Chilean Senate

## Overview

This document details the extraction strategy for the Senate's transparency portal (`https://www.senado.cl/transparencia`). We target financial records including remunerations, allowances, operational expenses, and per diems.

## System Architecture

* **Frontend:** Next.js SPA utilizing client-side hydration.
* **Backend:** Headless CMS (Strapi or Drupal) exposed at `https://web-back.senado.cl/`.
* **Extraction:** The platform exposes tabular data via unauthenticated REST APIs. We consume structured JSON payloads directly, avoiding HTML scraping entirely. This reduces pipeline fragility.

## Query Parameters

The API uses standard query parameters for filtering and pagination.

**Base URL:** `https://web-back.senado.cl/api/transparency/`

| Parameter | Type | Purpose | Example |
| :--- | :--- | :--- | :--- |
| `filters[ano][$eq]` | Integer | Filter by year. | `2024` |
| `filters[mes][$eq]` | Integer | Filter by month (1-12). | `1` |
| `sort` | String | Sort order. | `appaterno,fecha_ida` |
| `pagination[pageSize]`| Integer | Limit results per page. | `500` |
| `pagination[page]` | Integer | Request specific page. | `1` |

## Financial Endpoints

These endpoints provide the raw data required for financial aggregation.

### Base Salary (Dieta)
* **Path:** `/diet`
* **Schema:** `rut` (National ID), `nombre`, `appaterno`, `apmaterno`, `dieta` (gross), `deducciones`, `saldo` (net).
* **Usage:** Establishes the base salary per senator. The `rut` field serves as the primary key for joins.

### Operational Expenses
* **Path:** `/expenses/senator-Operational-expenses`
* **Schema:** `ano`, `mes`, `nombre`, `appaterno`, `gastos_operacionales` (category), `monto`.

### Travel
* **Domestic Flights:** `/domestic-air-tickets`
* **Foreign Missions:** `/foreign-missions` (Contains flights and international per diems).

### Committee and Executive Expenses
* **Committees:** `/expenses/committee-operational-expenses`
* **Presidency:** `/president-vicepresident`, `/expenses/presidents-republic`.

## Bypassing PDFs for Remuneration Data

The frontend's "Remuneraciones" view renders opaque PDF blobs. We do not attempt to parse them.

Instead, we target the "Dotación de Personal" (Staffing) JSON endpoints. These expose the same salary data in machine-readable JSON, eliminating the need for OCR.

**Target Endpoints:**
* **Contract Staff:** `/dotation/staffing`
* **Permanent Staff:** `/dotation/plant-equipment`
* **Fee-based Contractors:** `/dotation/fee`

**Schema:** Name, Surnames, Rank (`Escalafón`), Position (`Cargo`), Category, and `remuneracion` (Salary).

## ETL Pipeline

We execute the following steps to calculate total expenditures per senator:

1. **Extraction:**
   * Iterate over the required `[year, month]` ranges.
   * Send HTTP GET requests to `/diet`, `/expenses/senator-Operational-expenses`, `/domestic-air-tickets`, and `/foreign-missions`.
   * Implement retries with exponential backoff (e.g., using `tenacity`) to handle transient API failures.
   * Paginate until exhaustion.

2. **Normalization:**
   * **Primary Key:** Join datasets using `RUT` where available (primarily from `/diet`).
   * **Fuzzy Match:** When `RUT` is missing, generate a composite key using `LOWER(UNACCENT(appaterno + apmaterno + nombre))`.

3. **Aggregation:**
   * Calculate the total monthly expenditure per senator:
     `Total_Cost = Gross_Allowance + SUM(Operational_Expenses) + SUM(Domestic_Flights) + SUM(International_Per_Diems)`

4. **Storage:**
   * Dump raw JSON payloads to local disk for auditability.
   * Load the normalized data into DuckDB. Export as Parquet partitions to back the Streamlit frontend.
