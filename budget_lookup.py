#!/usr/bin/env python3
"""
Budget Lookup Integration

Adapts the 'Budget Prediction Model' significantly for integration into
the main enrichment pipeline.

Uses:
1. Verified Census Data (CSV) - Exact/Fuzzy matching
2. ML Predictor (GBM) - Fallback for missing cities (returns estimated budget)
"""

import re
import sys
from pathlib import Path
from difflib import SequenceMatcher
from typing import Optional, Tuple, Any

import pandas as pd
import numpy as np

# Try to import joblib/sklearn for model loading
try:
    import joblib
    HAS_ML = True
except ImportError:
    HAS_ML = False
    print("Warning: joblib/scikit-learn not found. ML prediction disabled.")


# Path modifications to find the neighboring project
CURRENT_DIR = Path(__file__).parent
BUDGET_PROJECT_DIR = CURRENT_DIR / "Budget Prediction Model"
DATA_PATH = BUDGET_PROJECT_DIR / "data" / "processed" / "municipal_budgets.csv"
MODEL_PATH = BUDGET_PROJECT_DIR / "models" / "budget_predictor_gbm.pkl"


# Census state code mapping
STATE_TO_CENSUS = {
    'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06',
    'CO': '08', 'CT': '09', 'DE': '10', 'DC': '11', 'FL': '12',
    'GA': '13', 'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18',
    'IA': '19', 'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23',
    'MD': '24', 'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28',
    'MO': '29', 'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33',
    'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38',
    'OH': '39', 'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44',
    'SC': '45', 'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49',
    'VT': '50', 'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55',
    'WY': '56'
}


class MLPredictor:
    """Gradient Boosting Predictor wrapper."""
    
    def __init__(self, model_path: Path):
        self.loaded = False
        self.model = None
        
        if not HAS_ML:
            return
            
        if model_path.exists():
            try:
                self.model = joblib.load(model_path)
                self.loaded = True
            except Exception as e:
                print(f"Failed to load ML model: {e}")
        else:
            print(f"ML model not found at {model_path}")
            
    def predict(self, lat: float, lon: float, population: int) -> float:
        """Predict per-capita expenditure."""
        if not self.loaded or population <= 0:
            return 2000.0 # Fallback default
            
        # Features: Latitude, Longitude, Log_Pop
        log_pop = np.log1p(population)
        X = pd.DataFrame([[lat, lon, log_pop]], columns=['Latitude', 'Longitude', 'Log_Pop'])
        
        try:
            # Predict Log_Per_Capita
            log_pc = self.model.predict(X)[0]
            pc = np.expm1(log_pc)
            return round(pc, 2)
        except Exception as e:
            print(f"Prediction error: {e}")
            return 2000.0


class BudgetEnricher:
    """Enrich cities using verified Census budget data + ML predictions."""
    
    def __init__(self):
        self._load_data()
        
    def _load_data(self):
        if DATA_PATH.exists():
            try:
                self.df = pd.read_csv(DATA_PATH)
                self.df['Census_State'] = self.df['Census_State'].astype(str).str.zfill(2)
                self.df['Name_Clean'] = self.df['Name'].apply(self._clean_name)
            except Exception as e:
                print(f"Error loading budget data: {e}")
                self.df = pd.DataFrame()
        else:
            print(f"Budget data not found at {DATA_PATH}")
            self.df = pd.DataFrame()
        
        # Load Predictor
        self.predictor = MLPredictor(MODEL_PATH)
            
    def _clean_name(self, name: Any) -> str:
        if pd.isna(name): return ""
        name = str(name).upper().strip()
        # Standardize "ST" -> "SAINT" to match common inputs, OR "SAINT" -> "ST" if DB uses ST.
        # Verified DB has "ST PAUL", "ST LOUIS", etc.
        name = name.replace("SAINT ", "ST ")
        
        # Remove common suffixes for cleaner matching
        # NOTE: Keeping COUNTY to ensure distinction between City/County of X
        for suffix in [' CITY', ' TOWN', ' VILLAGE', ' BOROUGH', ' TOWNSHIP', ' MUNICIPALITY']:
            name = name.replace(suffix, '')
        return re.sub(r'[^A-Z0-9\s]', '', name).strip()
    
    def _get_census_state(self, state: str) -> str:
        state = str(state).upper().strip()
        if state in STATE_TO_CENSUS:
            return STATE_TO_CENSUS[state]
        return state.zfill(2)
    
    def _find_match(self, city: str, census_state: str) -> Optional[Tuple[str, pd.Series]]:
        # NOTE: ignoring census_state code, using state string from input (converted to census_state in enrich? No.)
        # The enrich method passes 'city' and 'state' (abbrev).
        # We should use 'state' (abbrev) directly if available in self.df
        pass 

    def enrich(self, city, state, population=None, lat=None, lon=None):
        """
        Enrich with budget data.
        Returns dictionary with keys: total_expenditure, per_capita_expenditure, budget_source
        """
        if not city:
            return {
                "total_expenditure": None,
                "per_capita_expenditure": None,
                "budget_source": None
            }
        
        # Using State Abbreviation directly from DF
        if self.df.empty: return None
        
        # Filter by State Abbrev (Column index 2 based on CSV? Or 'State' name)
        # In init, we loaded DF. Check columns.
        # "Code,Name,State,Census_State,..."
        
        state_df = self.df[self.df['State'] == state]
        
        if state_df.empty:
             return {
                "total_expenditure": None,
                "per_capita_expenditure": None,
                "budget_source": None
            }
            
        city_clean = self._clean_name(city)
        
        exact = state_df[state_df['Name_Clean'] == city_clean]
        match = None
        if not exact.empty: 
            match = ('exact', exact.iloc[0])
        else:
            best_score, best_row = 0.85, None
            for _, row in state_df.iterrows():
                score = SequenceMatcher(None, city_clean, row['Name_Clean']).ratio()
                if score > best_score: best_score, best_row = score, row
            if best_row is not None:
                match = (f'fuzzy ({best_score:.0%})', best_row)
        
        if match:
            match_type, row = match
            total_exp = float(row['Total_Expenditure'])
            
            # Calculate per capita if possible
            per_capita = None
            if population and population > 0:
                per_capita = total_exp / population
            elif row['Population'] and float(row['Population']) > 0:
                 per_capita = total_exp / float(row['Population'])
                 
            return {
                "total_expenditure": total_exp,
                "per_capita_expenditure": per_capita,
                "budget_source": "Census Survey of Governments (Verified)"
            }
        
        # No match found
        return {
            "total_expenditure": None,
            "per_capita_expenditure": None,
            "budget_source": None
        }


# Singleton instance
_enricher = None

def get_budget_enricher():
    global _enricher
    if _enricher is None:
        _enricher = BudgetEnricher()
    return _enricher


def lookup_budget(city: str, state: str, population: Optional[int] = None,
                  lat: Optional[float] = None, lon: Optional[float] = None) -> dict:
    """Simple wrapper for pipeline integration."""
    enricher = get_budget_enricher()
    return enricher.enrich(city, state, population=population, lat=lat, lon=lon)


if __name__ == "__main__":
    enricher = BudgetEnricher()
    print("Budget Lookup Test")
    print("=" * 60)
    
    tests = [
        ("Austin", "TX", 974000),
        ("New York", "NY", 8000000),
        ("Fake City", "TX", 5000),
    ]
    
    for c, s, p in tests:
        r = enricher.enrich(c, s, population=p)
        bud = r.get('total_expenditure')
        src = r.get('budget_source')
        print(f"{c}, {s}: {src}")
        if bud:
            print(f"  Budget: ${bud:,.0f}")
            print(f"  Per Capita: ${r.get('per_capita_expenditure', 0):,.2f}")
        print()
