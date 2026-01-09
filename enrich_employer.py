#!/usr/bin/env python3
"""
Employer Classification Module

Rule-based employer type detection and canonical name extraction.
Used for peer group filtering in compensation studies.
"""

import re
from typing import Optional


# Patterns for detecting employer type from name
# Order matters - more specific patterns first
EMPLOYER_PATTERNS = [
    # School Districts
    (r"(.+?)\s+(?:independent\s+school\s+district|isd)$", "School District (K-12)"),
    (r"(.+?)\s+(?:unified\s+school\s+district|usd)$", "School District (K-12)"),
    (r"(.+?)\s+(?:school\s+district)$", "School District (K-12)"),
    (r"(.+?)\s+(?:public\s+schools)$", "School District (K-12)"),
    
    # Community College / University
    (r"(.+?)\s+(?:community\s+college)$", "Community College/University"),
    (r"(.+?)\s+(?:college\s+district)$", "Community College/University"),
    (r"(?:university\s+of\s+)(.+)$", "Community College/University"),
    (r"(.+?)\s+(?:state\s+university)$", "Community College/University"),
    
    # Special Districts
    (r"(.+?)\s+(?:water\s+district|mwd|water\s+authority)$", "Special District (Water/Sewer/Fire)"),
    (r"(.+?)\s+(?:utility\s+district|mud|pud)$", "Special District (Water/Sewer/Fire)"),
    (r"(.+?)\s+(?:fire\s+district|fire\s+department)$", "Special District (Water/Sewer/Fire)"),
    (r"(.+?)\s+(?:sanitation\s+district|sewer\s+district)$", "Special District (Water/Sewer/Fire)"),
    
    # Transit
    (r"(.+?)\s+(?:transit\s+authority|transit\s+district|metro|mta)$", "Transit Authority"),
    (r"(.+?)\s+(?:transportation\s+authority)$", "Transit Authority"),
    
    # Hospital/Healthcare
    (r"(.+?)\s+(?:hospital\s+district|health\s+district|medical\s+center)$", "Hospital/Healthcare District"),
    
    # Housing
    (r"(.+?)\s+(?:housing\s+authority)$", "Housing Authority"),
    
    # County (check before city patterns)
    (r"^county\s+of\s+(.+)$", "County Government"),
    (r"^(.+?)\s+county(?:\s+government)?$", "County Government"),
    
    # State
    (r"^state\s+of\s+(.+)$", "State Government"),
    
    # City/Municipal (most common, check last)
    (r"^city\s+of\s+(.+)$", "City/Municipal Government"),
    (r"^town\s+of\s+(.+)$", "City/Municipal Government"),
    (r"^village\s+of\s+(.+)$", "City/Municipal Government"),
    (r"^borough\s+of\s+(.+)$", "City/Municipal Government"),
    (r"^municipality\s+of\s+(.+)$", "City/Municipal Government"),
]


def classify_employer(employer: str) -> dict:
    """
    Parse employer name to extract type and canonical name.
    
    Args:
        employer: Raw employer name from job posting (e.g., "City of Austin")
    
    Returns:
        dict with:
            - employer_type_detected: Employer category
            - canonical_employer_name: Cleaned name for matching (e.g., "Austin")
    """
    if not employer:
        return {
            "employer_type_detected": "Unknown",
            "canonical_employer_name": "",
        }
    
    employer_clean = employer.strip()
    
    for pattern, emp_type in EMPLOYER_PATTERNS:
        match = re.search(pattern, employer_clean, re.IGNORECASE)
        if match:
            canonical = match.group(1).strip()
            
            # 1. Strip parenthetical states: "Aubrey (TX)" -> "Aubrey"
            canonical = re.sub(r"\s*\([A-Za-z\s\.]+\)$", "", canonical)

            # 2. Strip state name suffix: "Roanoke, Virginia" -> "Roanoke" or "Janesville Wisconsin" -> "Janesville"
            # Matches ", State" or " State" where State is full definition
            states_full = r"Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming"
            canonical = re.sub(r"[,]?\s+(" + states_full + r")$", "", canonical, flags=re.IGNORECASE)

            # 3. Clean up state abbrevs: "Austin, TX" -> "Austin"
            states_abbrev = r"TX|CA|NY|FL|IL|PA|OH|GA|NC|MI|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|ID|WV|HI|NH|ME|MT|RI|DE|SD|ND|AK|DC|VT|WY"
            canonical = re.sub(r",\s+(" + states_abbrev + r")$", "", canonical, flags=re.IGNORECASE)
            
            return {
                "employer_type_detected": emp_type,
                "canonical_employer_name": canonical.strip(),
            }
    
    # No pattern matched - return as-is with Unknown type
    return {
        "employer_type_detected": "Unknown",
        "canonical_employer_name": employer_clean,
    }


def get_population_band(population: Optional[int]) -> str:
    """
    Classify population into bands for peer group filtering.
    
    Args:
        population: Census population or None
    
    Returns:
        Population band string for filtering
    """
    if population is None:
        return "Unknown"
    if population < 5000:
        return "Very Small (<5K)"
    elif population < 15000:
        return "Small (5K-15K)"
    elif population < 50000:
        return "Medium (15K-50K)"
    elif population < 150000:
        return "Large (50K-150K)"
    elif population < 500000:
        return "Very Large (150K-500K)"
    else:
        return "Major City (500K+)"


def enrich_employer_data(employer: str, population: Optional[int] = None) -> dict:
    """
    Complete employer enrichment combining classification and population band.
    
    Args:
        employer: Raw employer name
        population: Optional population from Census lookup
    
    Returns:
        dict with all employer metadata fields
    """
    classification = classify_employer(employer)
    classification["population_band"] = get_population_band(population)
    return classification


# Quick test
if __name__ == "__main__":
    test_cases = [
        "City of Austin",
        "Travis County",
        "Austin Independent School District",
        "Capital Metro Transit Authority",
        "Lower Colorado River Authority",
        "Harris County Hospital District",
        "San Antonio Water System",
        "State of Texas",
        "University of Texas at Austin",
    ]
    
    print("Employer Classification Test")
    print("=" * 60)
    for emp in test_cases:
        result = classify_employer(emp)
        print(f"{emp}")
        print(f"  → Type: {result['employer_type_detected']}")
        print(f"  → Name: {result['canonical_employer_name']}")
        print()
