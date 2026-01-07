# Municipal Job Enrichment Pipeline

LLM-powered pipeline that enriches municipal job postings with structured compensation factors for vector-based job matching.

## Features

- **21 structured columns** extracted from raw job descriptions
- **Constrained enums** for job families (19) and levels (8)
- **50+ parallel workers** for high-throughput processing
- **Streaming writes** - each row saved immediately (crash-safe)
- **Resume capability** - pick up exactly where you left off
- **Price-optimized routing** via OpenRouter

## Quick Start

```bash
# 1. Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Set API key
export OPENROUTER_API_KEY='sk-or-v1-...'

# 3. Run enrichment
python enrich_jobs.py --input data/input/jobs.csv --output data/output/enriched.csv
```

## Usage

```bash
# Basic enrichment
python enrich_jobs.py -i input.csv -o output.csv

# With 100 parallel workers
python enrich_jobs.py -i input.csv -o output.csv --workers 100

# Limit to first N rows (for testing)
python enrich_jobs.py -i input.csv -o output.csv --limit 100

# Resume after crash/interrupt
python enrich_jobs.py -i input.csv -o output.csv --resume

# Dry run (no API calls)
python enrich_jobs.py -i input.csv -o output.csv --dry-run
```

## Output Schema

### Classification (for hybrid search filtering)
| Column | Type | Description |
|--------|------|-------------|
| `job_family` | Enum (19) | Domain: "Public Works/Utilities/Infrastructure", "Finance/Budget/Accounting", etc. |
| `job_subfamily` | String | Specialty: "Water Treatment", "Payroll", "Youth Recreation" |
| `job_level` | Enum (8) | Authority: "Executive", "Director", "Manager", "Supervisor", "Individual Contributor", etc. |

### Vector-Optimized Summary
| Column | Description |
|--------|-------------|
| `compensation_summary` | Structured summary: `[DOMAIN]/[LEVEL]/[SCOPE]/[MANAGES]/[REQUIRES]/[CORE FUNCTION]/[DECIDES]/[RISK]` |

### Quantitative Factors
| Column | Description |
|--------|-------------|
| `fte_managed` | Headcount: "0", "1-5", "6-20", "21-50", "50+", "100+" |
| `budget_authority` | Dollar amount: "$500K operational", "$5M capital" |
| `scope_of_impact` | Reach: "Team", "Division", "Department", "City-wide", "Regional" |

### Requirements
| Column | Description |
|--------|-------------|
| `licenses_required` | JSON array: `["CDL Class A", "PE License"]` |
| `certifications_required` | JSON array: `["CPA", "Water Treatment Grade 4"]` |
| `education_minimum` | Level: "High School", "Associate", "Bachelor", "Master" |
| `education_field` | Field: "Engineering", "Accounting", "Social Work" |
| `years_experience` | Range: "0", "1-2", "3-5", "6-10", "10+" |
| `specialized_systems` | JSON array: `["SCADA", "Munis", "GIS"]` |
| `specialized_knowledge` | Domain expertise: "Municipal finance", "Water chemistry" |

### Context
| Column | Description |
|--------|-------------|
| `supervision_given` | Who they manage |
| `supervision_received` | Who manages them |
| `physical_context` | Contextualized physical demands |
| `flsa_likely` | "Exempt" or "Non-Exempt" |
| `work_schedule` | "Standard weekday", "Shift work", "On-call" |
| `consequence_of_error` | Risk level of mistakes |
| `decision_authority` | Types of decisions made |

## Project Structure

```
Enrichment Flow/
├── enrich_jobs.py      # Main pipeline script
├── config.py           # Configuration (workers, model, enums)
├── prompts.py          # LLM prompt templates
├── requirements.txt    # Python dependencies
├── data/
│   ├── input/          # Source CSV files
│   └── output/         # Enriched CSV files
└── .agent/
    └── workflows/      # Workflow documentation
```

## Crash Recovery

If the process crashes or is interrupted:

1. A progress file (`output.progress.json`) tracks exactly which rows are complete
2. Run with `--resume` to continue from the exact row where it stopped
3. Already-processed rows won't be re-processed or duplicated

## Performance

| Workers | Throughput | 10K rows |
|---------|------------|----------|
| 5 | ~1 row/sec | ~2.7 hours |
| 20 | ~0.5 row/sec | ~5.5 hours |
| 50 | ~0.3 row/sec | ~9 hours |

*Note: Throughput depends on API rate limits and model response time.*

## Model

Uses `openai/gpt-oss-120b` via OpenRouter - a 120B parameter open-weight model optimized for structured extraction.
