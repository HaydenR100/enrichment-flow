# Municipal Budget Enrichment

Look up municipal budgets for 15,500+ US cities using Verified Census data.
Includes a **Gradient Boosting Machine Learning Predictor** for unknown cities.

## Usage

```python
from src.enrichment.budget_enrichment import BudgetEnricher

enricher = BudgetEnricher()

# 1. Exact Match (Verified Census Data)
result = enricher.enrich("Los Angeles", "CA", population=3900000)
# {'total_expenditure': 37323309000, 'per_capita': 9570, ...}

# 2. ML Prediction (Unknown City)
# Uses Gradient Boosting model trained on 15,000 verified data points.
result = enricher.enrich("New City", "CA", population=50000, lat=34.05, lon=-118.24)
# {'total_expenditure': 103278000, 'per_capita': 2066, 'source': 'ML Predictor...'}
```

## How It Works

1.  **Verified Database**:
    *   **Budget**: 2022 Census Survey of Governments.
    *   **Population**: 2022 Census Population Estimates (Fixed).
    *   **Coverage**: ~15,000 US municipalities.

2.  **Machine Learning Fallback**:
    *   **Algorithm**: Gradient Boosting Regressor (sklearn).
    *   **Features**: Latitude, Longitude, Log(Population).
    *   **Target**: Per-Capita Spending.
    *   **Training**: Trained on the full verified dataset (15,000 clean records).

## File Structure

```
├── src/enrichment/budget_enrichment.py   # Main logic
├── data/processed/municipal_budgets.csv  # Verified Database
├── models/budget_predictor_gbm.pkl       # Trained ML Model
└── data/raw/                             # Source Census files
```
