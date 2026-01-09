#!/usr/bin/env python3
"""
Census Population Lookup Module

Fetches population data from Census Bureau API for employer jurisdictions.
Uses fuzzy matching to handle name variations.
Caches results to minimize API calls.
"""

import json
import os
import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Optional

import requests

# Optional: rapidfuzz for better fuzzy matching, falls back to simple ratio
try:
    from rapidfuzz import fuzz
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    print("Warning: rapidfuzz not installed. Using basic string matching.")
    print("Install with: pip install rapidfuzz")


# State FIPS codes for Census API
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12", 
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18", 
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23", 
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28", 
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", 
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", 
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44", 
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49", 
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55", 
    "WY": "56",
}

# Cache directory
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DB = CACHE_DIR / "census_cache.db"


def init_cache_db():
    """Initialize SQLite cache database."""
    CACHE_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS census_places (
            state TEXT,
            name TEXT,
            population INTEGER,
            place_fips TEXT,
            PRIMARY KEY (state, name)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS state_loaded (
            state TEXT PRIMARY KEY,
            loaded_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def is_state_cached(state: str) -> bool:
    """Check if state data is already cached."""
    if not CACHE_DB.exists():
        return False
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM state_loaded WHERE state = ?", (state.upper(),))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def cache_state_places(state: str, places: list[dict]):
    """Cache Census places for a state."""
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    
    # Check if median_income column exists (migration)
    cursor.execute("PRAGMA table_info(census_places)")
    cols = [info[1] for info in cursor.fetchall()]
    if 'median_income' not in cols:
        print("Migrating cache DB: adding median_income column to census_places...")
        cursor.execute("ALTER TABLE census_places ADD COLUMN median_income INTEGER")
    
    for place in places:
        cursor.execute("""
            INSERT OR REPLACE INTO census_places (state, name, population, median_income, place_fips)
            VALUES (?, ?, ?, ?, ?)
        """, (state.upper(), place["name"], place["population"], place.get("median_income"), place["place_fips"]))
    
    cursor.execute("""
        INSERT OR REPLACE INTO state_loaded (state, loaded_at)
        VALUES (?, datetime('now'))
    """, (state.upper(),))
    
    conn.commit()
    conn.close()


def get_cached_places(state: str) -> list[dict]:
    """Get cached Census places for a state."""
    if not CACHE_DB.exists():
        return []
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, population, median_income, place_fips FROM census_places WHERE state = ?
    """, (state.upper(),))
    rows = cursor.fetchall()
    conn.close()
    return [{"name": r[0], "population": r[1], "median_income": r[2], "place_fips": r[3]} for r in rows]


def fetch_census_places(state_abbrev: str) -> list[dict]:
    """
    Fetch all places in a state from Census Bureau API.
    Uses ACS 5-year estimates (B01003_001E = Total Population).
    
    Args:
        state_abbrev: Two-letter state abbreviation (e.g., "TX")
    
    Returns:
        List of dicts with name, population, place_fips
    """
    fips = STATE_FIPS.get(state_abbrev.upper())
    if not fips:
        return []
    
    # Check cache first
    if is_state_cached(state_abbrev):
        return get_cached_places(state_abbrev)
    
    # Fetch from ACS 5-year estimates API (more stable than PEP)
    # B01003_001E = Total Population
    # B19013_001E = Median Household Income
    url = f"https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E,B19013_001E&for=place:*&in=state:{fips}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Census API error for {state_abbrev}: {e}")
        return []
    
    places = []
    for row in data[1:]:  # Skip header row
        # Clean name: "Austin city, Texas" -> "Austin"
        raw_name = row[0]
        name = raw_name.split(" city,")[0].split(" town,")[0].split(" village,")[0]
        name = name.split(" CDP,")[0]  # Census Designated Place
        name = name.split(" borough,")[0]
        name = name.split(" municipality,")[0]
        
        # Additional cleaning for "Boise City" -> "Boise"
        if name.endswith(" City"):
            name = name[:-5]
        
        # Parse population (may be None for some CDPs)
        try:
            pop = int(row[1]) if row[1] else 0
        except (ValueError, TypeError):
            pop = 0
            
        # Parse Median Household Income
        try:
            income = int(row[2]) if row[2] and int(row[2]) > 0 else None
        except (ValueError, TypeError):
            income = None
        
        # Get FIPS code (last element)
        place_fips = row[-1] if len(row) > 2 else ""
        
        places.append({
            "name": name,
            "population": pop,
            "median_income": income,
            "place_fips": place_fips,
        })
    
    # Cache for future use
    init_cache_db()
    cache_state_places(state_abbrev, places)
    
    return places


def simple_ratio(s1: str, s2: str) -> int:
    """Simple string similarity ratio (0-100) without rapidfuzz."""
    s1, s2 = s1.lower(), s2.lower()
    if s1 == s2:
        return 100
    
    # Levenshtein-ish: count matching chars
    shorter, longer = (s1, s2) if len(s1) <= len(s2) else (s2, s1)
    if len(longer) == 0:
        return 100
    
    matches = sum(1 for c in shorter if c in longer)
    return int((matches / len(longer)) * 100)


def match_score(city: str, place_name: str) -> int:
    """Calculate match score between city and census place name."""
    if HAS_RAPIDFUZZ:
        return fuzz.ratio(city.lower(), place_name.lower())
    else:
        return simple_ratio(city, place_name)


def lookup_population(city: str, state: str) -> dict:
    """
    Look up population for a city/place from Census data.
    
    Args:
        city: City name to look up
        state: Two-letter state abbreviation
    
    Returns:
        dict with:
            - census_population: Population or None
            - census_place_fips: FIPS code or None
            - census_match_confidence: Match score (0-100)
            - census_matched_name: Actual matched place name
    """
    if not city or not state or not isinstance(state, str):
        return {
            "census_population": None,
            "census_median_income": None,
            "census_place_fips": None,
            "census_match_confidence": 0,
            "census_matched_name": None,
        }
    
    places = fetch_census_places(state)
    if not places:
        return {
            "census_population": None,
            "census_median_income": None,
            "census_place_fips": None,
            "census_match_confidence": 0,
            "census_matched_name": None,
        }
    
    best_match = None
    best_score = 0
    
    for place in places:
        score = match_score(city, place["name"])
        if score > best_score:
            best_score = score
            best_match = place
    
    # Require at least 75% match confidence
    if best_score >= 75 and best_match:
        return {
            "census_population": best_match["population"],
            "census_median_income": best_match.get("median_income"),
            "census_place_fips": best_match["place_fips"],
            "census_match_confidence": best_score,
            "census_matched_name": best_match["name"],
        }
    
    return {
        "census_population": None,
        "census_median_income": None,
        "census_place_fips": best_match["place_fips"] if best_match else None,
        "census_match_confidence": best_score,
        "census_matched_name": best_match["name"] if best_match else None,
    }


# Quick test
if __name__ == "__main__":
    print("Census Population Lookup Test")
    print("=" * 60)
    
    test_cases = [
        ("Austin", "TX"),
        ("Houston", "TX"),
        ("Los Angeles", "CA"),
        ("Springfield", "IL"),  # Common name, should still work
        ("Fake City", "TX"),    # Should fail gracefully
    ]
    
    for city, state in test_cases:
        result = lookup_population(city, state)
        print(f"\n{city}, {state}:")
        print(f"  Population: {result['census_population']:,}" if result['census_population'] else "  Population: Not found")
        print(f"  FIPS: {result['census_place_fips']}")
        print(f"  Confidence: {result['census_match_confidence']}%")
        print(f"  Matched: {result['census_matched_name']}")
