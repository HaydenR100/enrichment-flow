import pandas as pd
import csv
import os

# Files
PID_PATH = "budget_registry/data/raw/Fin_PID_2023.txt"
FIN_PATH = "budget_registry/data/raw/2023_Finance_Data.txt"
CSV_PATH = "budget_registry/data/processed/municipal_budgets.csv"

# Load existing IDs to avoid duplicates
existing_ids = set()
state_map = {} # '01' -> 'AL'

print(f"Loading existing data from {CSV_PATH}...")
if os.path.exists(CSV_PATH):
    try:
        df = pd.read_csv(CSV_PATH, dtype=str)
        # Assuming header: GOV_ID,Name,State,Census_State,...
        if 'GOV_ID' in df.columns:
            existing_ids = set(df['GOV_ID'].dropna().unique())
        
        # Build State Map
        for _, row in df.iterrows():
            if pd.notna(row.get('Census_State')) and pd.notna(row.get('State')):
                code = row['Census_State'].zfill(2)
                abbrev = row['State']
                if len(code) == 2 and len(abbrev) == 2:
                    state_map[code] = abbrev
    except Exception as e:
        print(f"Error reading CSV: {e}")

# Fallback State Map if incomplete
DEFAULT_STATES = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY'
}
# Merge defaults
for k, v in DEFAULT_STATES.items():
    if k not in state_map:
        state_map[k] = v

print(f"Found {len(existing_ids)} existing records.")

# 1. Parse PID to get names of ALL Municipalities (Type 2) and Townships (Type 3)
entity_map = {} # ID -> Name
print(f"Scanning {PID_PATH}...")
count_found = 0
try:
    with open(PID_PATH, 'r', encoding='latin1') as f:
        for line in f:
            if len(line) < 12: continue
            gid = line[:12]
            
            # Filter by Type (3rd digit, index 2)
            # 1=County, 2=Muni, 3=Township
            gov_type = gid[2]
            if gov_type not in ['1', '2', '3']:
                continue
                
            # Extract Name (Chars 12-76 based on inspection, strip whitespace)
            name_raw = line[12:76]
            name = name_raw.strip()
            
            entity_map[gid] = name
            count_found += 1
except FileNotFoundError:
    print(f"PID file not found: {PID_PATH}")
    exit(1)

print(f"Identified {len(entity_map)} potential entities (Type 2/3).")

# 2. Scan Finance Data for Budgets
restorable = []
print(f"Scanning {FIN_PATH}...")
count_matches = 0

try:
    with open(FIN_PATH, 'r', encoding='latin1') as f:
        for line in f:
            line = line.strip()
            if len(line) < 20: continue
            
            gid = line[:12]
            
            # Skip if already in CSV
            if gid in existing_ids:
                continue
            
            # Skip if not a target entity
            if gid not in entity_map:
                continue
            
            # Parse Code
            code = line[12:15]
            if code != '49U': continue
            
            # Parse Value
            rest = line[15:]
            if len(rest) < 5: continue
            value_str = rest[:-5].strip()
            
            try:
                val = int(value_str) * 1000
                if val > 0:
                    name = entity_map[gid]
                    state_code = gid[:2]
                    state_abbrev = state_map.get(state_code, 'XX')
                    
                    # Columns: GOV_ID,Name,State,Census_State,Total_Expenditure,Population,Per_Capita,Latitude,Longitude
                    restorable.append([
                        gid, name, state_abbrev, state_code, val, "", "", "", ""
                    ])
                    existing_ids.add(gid) # Prevent dupes if multiples
                    count_matches += 1
            except:
                continue
except FileNotFoundError:
    print(f"Finance file not found: {FIN_PATH}")
    exit(1)

print(f"Found {count_matches} new budgets to restore.")

# 3. Append
# 3. Append / Create
if restorable:
    mode = 'a'
    write_header = not os.path.exists(CSV_PATH)
    
    print(f"Writing {len(restorable)} rows to {CSV_PATH}...")
    with open(CSV_PATH, mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        if write_header:
            writer.writerow(['GOV_ID', 'Name', 'State', 'Census_State', 'Total_Expenditure', 'Population', 'Per_Capita', 'Latitude', 'Longitude'])
        writer.writerows(restorable)
    print("Done.")
else:
    print("No new records found.")
