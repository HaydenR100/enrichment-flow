---
description: Enrich municipal job postings with LLM-extracted compensation factors
---

# Job Enrichment Workflow

This workflow processes CSV files of municipal job postings and enriches them with structured compensation factors using OpenRouter's GPT-OSS-120B model.

## Prerequisites

1. Set your OpenRouter API key:
   ```bash
   export OPENROUTER_API_KEY='sk-or-v1-...'
   ```

2. Install dependencies:
   // turbo
   ```bash
   cd "/Users/haydenrussell/Documents/Enrichment Flow"
   pip install -r requirements.txt
   ```

## Usage

### Basic Enrichment

Process an entire CSV file:
```bash
cd "/Users/haydenrussell/Documents/Enrichment Flow"
python enrich_jobs.py --input "Short Test.csv" --output "enriched_jobs.csv"
```

### Limited Test Run

Process only a subset (recommended for testing):
```bash
python enrich_jobs.py --input "Short Test.csv" --output "test_enriched.csv" --limit 10
```

### Dry Run (No API Calls)

Preview what would be processed:
// turbo
```bash
python enrich_jobs.py --input "Short Test.csv" --output "test.csv" --limit 5 --dry-run
```

### Resume from Checkpoint

If processing is interrupted, resume:
```bash
python enrich_jobs.py --input "Short Test.csv" --output "enriched_jobs.csv" --resume
```

## Output Columns

The enrichment adds these columns:

| Column | Description |
|--------|-------------|
| `compensation_summary` | Dense paragraph for vectorization |
| `job_family` | Primary domain (Public Works, Finance, etc.) |
| `job_subfamily` | Specific specialty |
| `job_level` | Authority level (Executive → Trainee) |
| `supervision_given` | Who they manage |
| `supervision_received` | Who manages them |
| `licenses_required` | JSON array of licenses |
| `certifications_required` | JSON array of certifications |
| `education_minimum` | Minimum degree level |
| `education_field` | Required field of study |
| `years_experience` | Experience range |
| `specialized_systems` | JSON array of domain software |
| `physical_context` | Contextualized physical demands |
| `flsa_likely` | Exempt/Non-Exempt prediction |
| `work_schedule` | Schedule type |
| `budget_authority` | Budget responsibility |

## Notes

- The script saves checkpoints every 10 rows (configurable in `config.py`)
- API calls include exponential backoff retry logic
- Large descriptions are automatically truncated to fit context window
