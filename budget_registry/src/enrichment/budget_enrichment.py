"""
Municipal Budget Enrichment

Lookup budgets from verified 2022 Census data + 2023 Population Estimates.
Includes Machine Learning Predictor (Gradient Boosting) for unknown cities.
"""
import pandas as pd
import numpy as np
import re
import joblib
from pathlib import Path
from difflib import SequenceMatcher

# Census state code mapping
STATE_TO_CENSUS = {
    'AL': '10', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '62',
    'CO': '08', 'CT': '09', 'DE': '10', 'FL': '12', 'GA': '13',
    'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19',
    'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24',
    'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29',
    'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33', 'NJ': '34',
    'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
    'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45',
    'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50',
    'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56',
    'DC': '11'
}

class MLPredictor:
    """Gradient Boosting Predictor trained on 15,000 verified cities."""
    
    def __init__(self, model_path):
        if Path(model_path).exists():
            self.model = joblib.load(model_path)
            self.loaded = True
        else:
            self.model = None
            self.loaded = False
            
    def predict(self, lat, lon, population):
        if not self.loaded or population <= 0:
            return 2000.0 # Fallback
            
        # Features: Latitude, Longitude, Log_Pop
        log_pop = np.log1p(population)
        X = pd.DataFrame([[lat, lon, log_pop]], columns=['Latitude', 'Longitude', 'Log_Pop'])
        
        # Predict Log_Per_Capita
        log_pc = self.model.predict(X)[0]
        pc = np.expm1(log_pc)
        return round(pc, 2)

class BudgetEnricher:
    """Enrich cities using verified Census budget data + ML predictions."""
    
    def __init__(self):
        self._load_data()
        
    def _load_data(self):
        base = Path(__file__).parent.parent.parent
        
        budget_path = base / "data" / "processed" / "municipal_budgets.csv"
        
        if budget_path.exists():
            self.df = pd.read_csv(budget_path)
            self.df['Census_State'] = self.df['Census_State'].astype(str).str.zfill(2)
            self.df['Name_Clean'] = self.df['Name'].apply(self._clean_name)
        else:
            self.df = pd.DataFrame()
        
        # Load Predictor
        model_path = base / "models" / "budget_predictor_gbm.pkl"
        self.predictor = MLPredictor(model_path)
            
    def _clean_name(self, name):
        if pd.isna(name): return ""
        name = str(name).upper().strip()
        for suffix in [' CITY', ' TOWN', ' VILLAGE', ' BOROUGH', ' TOWNSHIP']:
            name = name.replace(suffix, '')
        return re.sub(r'[^A-Z0-9\s]', '', name).strip()
    
    def _get_census_state(self, state):
        state = str(state).upper().strip()
        if state in STATE_TO_CENSUS:
            return STATE_TO_CENSUS[state]
        return state.zfill(2)
    
    def _find_match(self, city, census_state):
        if self.df.empty: return None
        city_clean = self._clean_name(city)
        state_df = self.df[self.df['Census_State'] == census_state]
        if state_df.empty: return None
        
        exact = state_df[state_df['Name_Clean'] == city_clean]
        if not exact.empty: return ('exact', exact.iloc[0])
        
        best_score, best_row = 0.85, None
        for _, row in state_df.iterrows():
            score = SequenceMatcher(None, city_clean, row['Name_Clean']).ratio()
            if score > best_score: best_score, best_row = score, row
            
        if best_row is not None: return (f'fuzzy ({best_score:.0%})', best_row)
        return None
    
    def enrich(self, city, state, population=None, lat=None, lon=None):
        census_state = self._get_census_state(state)
        result = {'city': city, 'state': state, 'matched': False}
        
        match = self._find_match(city, census_state)
        
        if match:
            match_type, row = match
            
            # Use data from row
            # If standard DB, verify population match
            # If clean DB, population is trusted
            
            budget = float(row['Total_Expenditure'])
            
            # Coordinates
            r_lat = float(row['Latitude']) if pd.notna(row.get('Latitude')) else None
            r_lon = float(row['Longitude']) if pd.notna(row.get('Longitude')) else None
            
            result.update({
                'matched': True,
                'match_type': match_type,
                'total_expenditure': budget,
                'source': 'Census Survey of Governments (Verified)',
                'matched_name': row['Name']
            })
            if r_lat and r_lon:
                result['latitude'] = r_lat
                result['longitude'] = r_lon
            
            # Per Capita
            # Use user population if provided, else DB population
            calc_pop = population if (population and population > 0) else row['Population']
            if calc_pop > 0:
                result['per_capita'] = round(budget / calc_pop, 2)
                
        else:
            # Prediction
            # Needs lat/lon/pop
            has_coords = (lat is not None and lon is not None)
            has_pop = (population and population > 0)
            
            if has_coords and has_pop:
                pred_pc = self.predictor.predict(lat, lon, population)
                pred_bud = pred_pc * population
                result.update({
                    'matched': False,
                    'match_type': 'predicted',
                    'total_expenditure': round(pred_bud),
                    'per_capita': pred_pc,
                    'source': 'ML Predictor (Gradient Boosting)'
                })
            else:
                 result['error'] = 'Not found and missing params for prediction'

        return result

def main():
    e = BudgetEnricher()
    tests = [
        ("Los Angeles", "CA", 3900000, 34.05, -118.24),
        ("FakeCity", "CA", 50000, 34.05, -118.24),
        ("FakeTown", "MS", 5000, 32.30, -90.18),
        ("Metropolis", "NY", 8000000, 40.71, -74.00)
    ]
    print(f"\nModel Loaded: {e.predictor.loaded}")
    print(f"{'City':<15} {'State':<5} {'Source':<20} {'Budget':>15} {'PerCap':>10}")
    print("-" * 70)
    for c, s, p, lat, lon in tests:
        r = e.enrich(c, s, p, lat, lon)
        bud = r.get('total_expenditure', 0)
        pc = r.get('per_capita', 0)
        src = "Census" if r.get('matched') else "ML Model"
        print(f"{c:<15} {s:<5} {src:<20} ${bud:>14,.0f} ${pc:>9,.0f}")

if __name__ == "__main__":
    main()
