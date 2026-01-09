#!/usr/bin/env python3
"""
Complete Enrichment Pipeline

Orchestrates all enrichment layers:
  Layer 1: LLM extraction (handled by enrich_jobs.py)
  Layer 2: Municipality metadata (employer classification, Census population)
  Layer 3: Statistical processing (ECI aging, normalization, confidence)

This script processes CSV files that have ALREADY been through LLM enrichment
and adds the additional metadata and statistical columns.

Usage:
    python pipeline.py --input enriched_jobs.csv --output final_enriched.csv
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from enrich_employer import classify_employer, get_population_band
from census_lookup import lookup_population
from budget_lookup import lookup_budget
from statistical_processing import process_statistical_enrichment


# Column renaming map for final output
OUTPUT_COLUMN_MAP = {
    "total_expenditure": "Total Expenditure",
    "per_capita_expenditure": "Per Capita Expenditure"
}

# New columns added by this pipeline
METADATA_COLUMNS = [
    # Employer classification
    "employer_type_detected",
    "canonical_employer_name",
    
    # Census data
    "census_population",
    "census_median_household_income",
    "census_place_fips",
    "census_match_confidence",
    "census_matched_name",
    "population_band",
    
    # Budget data (Layer 2.4)
    "total_expenditure",
    "per_capita_expenditure",
    "budget_source",
    "employer_lat",
    "employer_lon",
    
    # Statistical processing
    "data_age_months",
    "data_freshness",
    "salary_min_eci_adjusted",
    "salary_max_eci_adjusted",
    "eci_adjustment_pct",
    "effective_hourly_rate",
    "salary_40hr_equivalent",
    
    # Quality
    "data_confidence_score",
    "data_quality_issues",
]


def process_row(row: dict) -> dict:
    """
    Apply all Layer 2 and Layer 3 enrichment to a single row.
    
    Args:
        row: Dict containing LLM-enriched job data
    
    Returns:
        Dict with all new metadata columns added
    """
    result = dict(row)  # Copy original
    
    # Layer 2a: Employer classification
    employer = row.get("employer", "")
    emp_class = classify_employer(employer)
    result["employer_type_detected"] = emp_class["employer_type_detected"]
    result["canonical_employer_name"] = emp_class["canonical_employer_name"]
    
    # Layer 2b & 2c: Census Population and Budget Lookup
    # ONLY for City/Municipal Government. Non-municipal employers (State, County, etc.) should not have these stats.
    
    city = row.get("city", "")
    state = row.get("state", "")

    # Initialize defaults
    result["census_population"] = None
    result["census_median_household_income"] = None
    result["census_place_fips"] = None
    result["census_match_confidence"] = 0
    result["census_matched_name"] = None
    result["population_band"] = "Unknown"
    result["total_expenditure"] = None
    result["per_capita_expenditure"] = None
    result["budget_source"] = None
    result["employer_lat"] = None
    result["employer_lon"] = None

    if emp_class["employer_type_detected"] in ["City/Municipal Government", "County Government"]:
        lookup_city = emp_class["canonical_employer_name"] or city or ""
        
        # Adjust name for County lookup logic (DB has "X COUNTY")
        if emp_class["employer_type_detected"] == "County Government":
            # Check if we need to append suffix
            lower_name = lookup_city.lower()
            if not lower_name.endswith(" county") and not lower_name.endswith(" parish"):
                if state == "LA":
                    lookup_city += " Parish"
                else:
                    lookup_city += " County"

        # Census Lookup (Only for Cities? Or attempt for all?)
        # Standard Census Place lookup is designed for Cities/Towns. 
        # Counties might not match 'fetch_census_places' logic nicely unless we expand it.
        # But user asked to match budget data.
        
        # Let's keep Census Lookup logic as is for now (mainly city based), but ALLOW budget lookup.
        # But wait, budget_lookup NEEDS population to calc per capita properly if not in DB.
        # Verified DB has population.
        
        # Census Lookup wrapper
        if emp_class["employer_type_detected"] == "City/Municipal Government":
             census = lookup_population(lookup_city, state)
        else:
             # For Counties, we don't have a reliable 'lookup_county_population' yet.
             # We'll pass None and rely on Budget DB's internal population if available.
             census = {
                 "census_population": None,
                 "census_median_income": None,
                 "census_place_fips": None,
                 "census_match_confidence": 0,
                 "census_matched_name": None
             }

        result["census_population"] = census["census_population"]
        result["census_median_household_income"] = census.get("census_median_income")
        result["census_place_fips"] = census["census_place_fips"]
        result["census_match_confidence"] = census["census_match_confidence"]
        result["census_matched_name"] = census["census_matched_name"]
        
        # Population Band
        if census["census_population"]:
            result["population_band"] = get_population_band(census["census_population"])

        # Budget Lookup
        try:
            lat = float(row.get("latitude")) if row.get("latitude") else None
            lon = float(row.get("longitude")) if row.get("longitude") else None
        except (ValueError, TypeError):
            lat, lon = None, None

        budget = lookup_budget(
            lookup_city, 
            state, 
            population=census["census_population"],
            lat=lat,
            lon=lon
        )
        
        result["total_expenditure"] = budget.get("total_expenditure")
        result["per_capita_expenditure"] = budget.get("per_capita_expenditure")
        result["budget_source"] = budget.get("budget_source")
        result["employer_lat"] = budget.get("employer_lat")
        result["employer_lon"] = budget.get("employer_lon")
    
    # Layer 3: Statistical processing
    stats = process_statistical_enrichment(result)
    result.update(stats)
    
    # Convert data_quality_issues list to JSON string for CSV
    if isinstance(result.get("data_quality_issues"), list):
        result["data_quality_issues"] = json.dumps(result["data_quality_issues"])
    
    return result


def process_file(
    input_path: Path,
    output_path: Path,
    limit: Optional[int] = None,
) -> None:
    """
    Process an entire CSV file through the metadata/stats pipeline.
    
    Args:
        input_path: Path to LLM-enriched CSV
        output_path: Path for final output CSV
        limit: Optional row limit for testing
    """
    print(f"Loading {input_path}...")
    df = pd.read_csv(input_path)
    total_rows = len(df)
    print(f"Loaded {total_rows} rows")
    
    if limit:
        print(f"Limiting to first {limit} rows")
        df = df.head(limit)
        total_rows = len(df)
    
    # Determine output columns
    input_columns = list(df.columns)
    raw_output_cols = input_columns + [c for c in METADATA_COLUMNS if c not in input_columns]
    output_columns = [OUTPUT_COLUMN_MAP.get(c, c) for c in raw_output_cols]
    
    # Process rows
    print(f"\nProcessing {total_rows} rows...")
    processed_rows = []
    
    for idx, row in tqdm(df.iterrows(), total=total_rows, desc="Enriching metadata"):
        row_dict = row.to_dict()
        enriched = process_row(row_dict)
        
        # Rename columns as requested
        # Create a new dict with mapped keys
        final_row = {}
        for k, v in enriched.items():
            final_row[OUTPUT_COLUMN_MAP.get(k, k)] = v
        
        processed_rows.append(final_row)
    
    # Write output
    print(f"\nWriting output to {output_path}...")
    output_df = pd.DataFrame(processed_rows)
    
    # Ensure column order
    for col in output_columns:
        if col not in output_df.columns:
            output_df[col] = None
    output_df = output_df[output_columns]
    
    output_df.to_csv(output_path, index=False)
    
    # Summary stats
    print(f"\n{'='*60}")
    print("COMPLETE!")
    print(f"{'='*60}")
    print(f"Processed: {len(processed_rows)} rows")
    print(f"Output: {output_path}")
    
    # Data quality summary
    if "data_confidence_score" in output_df.columns:
        avg_confidence = output_df["data_confidence_score"].mean()
        print(f"Average confidence score: {avg_confidence:.1f}")
    
    if "census_population" in output_df.columns:
        pop_found = output_df["census_population"].notna().sum()
        print(f"Census matches: {pop_found}/{len(output_df)} ({100*pop_found/len(output_df):.1f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="Apply municipality metadata and statistical enrichment to LLM-enriched jobs"
    )
    parser.add_argument("--input", "-i", required=True, help="Input LLM-enriched CSV")
    parser.add_argument("--output", "-o", required=True, help="Output CSV path")
    parser.add_argument("--limit", "-l", type=int, help="Limit rows (for testing)")
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return
    
    process_file(input_path, output_path, args.limit)


if __name__ == "__main__":
    main()
