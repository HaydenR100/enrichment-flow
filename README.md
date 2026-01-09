# Job Enrichment Pipeline

A sophisticated, multi-layered pipeline for enriching municipal job postings with compensation-critical data points. Designed for precision in municipal and county compensation studies.

## 🚀 Overview

This pipeline transforms raw job postings into a rich dataset by applying four distinct layers of analysis:

1.  **AI Extraction**: Leverages LLMs to extract structural data like DBM bands, complexity scores, pension types, and union indicators.
2.  **Municipality Metadata**: Rule-based detection of employer types (City, County, etc.) and Census-verified lookup of population and median household income.
3.  **Financial Context**: Direct integration with the 2023 Census Survey of Governments to retrieve verified "Total Expenditure" and "Per Capita" statistics.
4.  **Statistical Processing**: Applies ECI (Employment Cost Index) aging adjustments and normalizes salaries to a standard 40-hour work week.

## 📁 Project Structure

```text
├── pipeline.py                 # Core orchestration script
├── enrich_jobs.py              # LLM extraction layer
├── enrich_employer.py          # Rule-based classification
├── census_lookup.py            # Census Bureau API integration
├── budget_lookup.py            # Census Finance database lookup
├── statistical_processing.py    # ECI aging & salary normalization
├── budget_registry/            # Source data for municipal/county budgets
├── data/                       # Input/Output data directory
└── docs/                       # Project methodology and documentation
```

## 🛠️ Getting Started

1.  **Environment Setup**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2.  **Configuration**:
    Copy `.env.example` to `.env` and add your `OPENROUTER_API_KEY`. The Census integration uses ACS 5-year estimate public APIs.

3.  **Execution**:
    ```bash
    # Step 1: LLM Enrichment
    python3 enrich_jobs.py -i data/input/your_jobs.csv -o data/output/llm_enriched.csv

    # Step 2: Full Pipeline (Census, Budget, Stats)
    python3 pipeline.py -i data/output/llm_enriched.csv -o data/output/final_enriched.csv
    ```

## 📊 Design Philosophy

-   **Verified Data First**: We prioritize verified 2023 Census data over ML predictions for fiscal enrichment.
-   **Apples-to-Apples Comparison**: By normalizing for hours, population, and budget, we enable valid salary comparisons across jurisdictions.
-   **Transparency**: Every record includes a confidence score and details on the data source (e.g., "Census Survey of Governments").

## ⚖️ Standards Compliance

This project implements the techniques defined in:
-   *Comprehensive Methodology for Municipal Compensation Analysis*
-   *Modernizing Municipal Compensation*

Located in the `docs/` directory.
