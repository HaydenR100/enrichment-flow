#!/usr/bin/env python3
"""
Statistical Processing Module

Implements data aging (ECI adjustment), salary normalization,
data freshness tracking, and confidence scoring.
"""

import json
from datetime import datetime, timezone
from typing import Optional, Any


# BLS Employment Cost Index for State/Local Government
# Source: https://www.bls.gov/eci/
# 2023-2024 average annual increase is approximately 4%
ECI_ANNUAL_RATE = 0.04


def calculate_data_age(
    posting_date: Optional[str] = None,
    closing_date: Optional[str] = None,
    enriched_at: Optional[str] = None,
) -> dict:
    """
    Calculate age of salary data in months.
    
    Uses best available date: posting_date > closing_date > enriched_at
    
    Args:
        posting_date: When job was posted (YYYY-MM-DD or ISO format)
        closing_date: Application deadline
        enriched_at: When enrichment ran (fallback)
    
    Returns:
        dict with:
            - data_age_months: Age in months (float)
            - data_freshness: "Fresh" (<6mo), "Aging" (6-12mo), "Stale" (>12mo)
    """
    date_str = posting_date or closing_date or enriched_at
    
    if not date_str:
        return {
            "data_age_months": None,
            "data_freshness": "Unknown",
        }
    
    try:
        # Handle various date formats
        if "T" in str(date_str):
            # ISO format with time
            data_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        elif "/" in str(date_str):
            # MM/DD/YYYY format
            data_date = datetime.strptime(str(date_str).split()[0], "%m/%d/%Y")
            data_date = data_date.replace(tzinfo=timezone.utc)
        else:
            # YYYY-MM-DD format
            data_date = datetime.strptime(str(date_str), "%Y-%m-%d")
            data_date = data_date.replace(tzinfo=timezone.utc)
        
        now = datetime.now(timezone.utc)
        age_days = (now - data_date).days
        age_months = age_days / 30.44  # Average days per month
        
        if age_months < 6:
            freshness = "Fresh"
        elif age_months < 12:
            freshness = "Aging"
        else:
            freshness = "Stale"
        
        return {
            "data_age_months": round(age_months, 1),
            "data_freshness": freshness,
        }
    except Exception as e:
        return {
            "data_age_months": None,
            "data_freshness": "Unknown",
        }


def apply_eci_aging(
    salary: Optional[float],
    data_age_months: Optional[float],
) -> dict:
    """
    Age salary data to current dollars using Employment Cost Index.
    
    Args:
        salary: Original salary value
        data_age_months: Age of data in months
    
    Returns:
        dict with:
            - salary_eci_adjusted: Salary in current dollars
            - eci_adjustment_pct: Percentage adjustment applied
    """
    if salary is None or data_age_months is None:
        return {
            "salary_eci_adjusted": None,
            "eci_adjustment_pct": 0.0,
        }
    
    try:
        salary = float(salary)
        age_years = float(data_age_months) / 12
        
        # Compound growth formula
        factor = (1 + ECI_ANNUAL_RATE) ** age_years
        adjusted = salary * factor
        
        return {
            "salary_eci_adjusted": round(adjusted, 2),
            "eci_adjustment_pct": round((factor - 1) * 100, 1),
        }
    except (ValueError, TypeError):
        return {
            "salary_eci_adjusted": None,
            "eci_adjustment_pct": 0.0,
        }


def parse_hours(hours_str: Optional[str]) -> float:
    """Parse hours per week string to float."""
    if not hours_str:
        return 40.0  # Default to standard full-time
    
    hours_str = str(hours_str).lower().strip()
    
    # Handle common formats
    if "standard" in hours_str:
        return 40.0
    
    # Try to extract first number
    import re
    match = re.search(r"(\d+\.?\d*)", hours_str)
    if match:
        return float(match.group(1))
    
    return 40.0  # Default


def normalize_salary(
    salary: Optional[float],
    hours_per_week: Optional[str] = None,
) -> dict:
    """
    Normalize salary to standard 40-hour week and calculate hourly rate.
    
    Args:
        salary: Annual salary
        hours_per_week: Reported hours (e.g., "40", "37.5", "Standard (40)")
    
    Returns:
        dict with:
            - effective_hourly_rate: Actual hourly rate
            - salary_40hr_equivalent: Normalized to 40-hour week
    """
    if salary is None:
        return {
            "effective_hourly_rate": None,
            "salary_40hr_equivalent": None,
        }
    
    try:
        salary = float(salary)
        hours = parse_hours(hours_per_week)
        
        # Calculate hourly rate
        annual_hours = hours * 52
        hourly = salary / annual_hours
        
        # Normalize to 40-hour week
        standard_annual = hourly * 40 * 52
        
        return {
            "effective_hourly_rate": round(hourly, 2),
            "salary_40hr_equivalent": round(standard_annual, 0),
        }
    except (ValueError, TypeError):
        return {
            "effective_hourly_rate": None,
            "salary_40hr_equivalent": None,
        }


def calculate_confidence_score(row: dict) -> dict:
    """
    Calculate overall data quality/confidence score (0-100).
    
    Penalizes:
        - Missing compensation summary (-25)
        - Low census match confidence (-15)
        - Stale data (-15)
        - Missing salary data (-20)
        - Missing employer classification (-10)
    
    Args:
        row: Enriched job record as dict
    
    Returns:
        dict with:
            - data_confidence_score: 0-100 score
            - data_quality_issues: List of identified issues
    """
    score = 100
    issues = []
    
    # Check compensation summary
    if not row.get("compensation_summary"):
        score -= 25
        issues.append("No compensation summary")
    
    # Check census match
    census_conf = row.get("census_match_confidence", 0)
    try:
        census_conf = float(census_conf) if census_conf else 0
    except:
        census_conf = 0
    
    if census_conf < 75:
        score -= 15
        issues.append(f"Low census match ({census_conf:.0f}%)")
    
    # Check data freshness
    if row.get("data_freshness") == "Stale":
        score -= 15
        issues.append("Stale data (>12 months)")
    elif row.get("data_freshness") == "Unknown":
        score -= 10
        issues.append("Unknown data age")
    
    # Check salary data
    if not row.get("salary_min") and not row.get("salary_max"):
        score -= 20
        issues.append("No salary data")
    
    # Check employer classification
    if row.get("employer_type_detected") == "Unknown":
        score -= 10
        issues.append("Unknown employer type")
    
    return {
        "data_confidence_score": max(0, score),
        "data_quality_issues": issues,
    }


def process_statistical_enrichment(row: dict) -> dict:
    """
    Apply all statistical processing to an enriched row.
    
    Args:
        row: Dict containing already-enriched job data
    
    Returns:
        dict with all statistical enrichment fields
    """
    result = {}
    
    # 1. Calculate data age
    age_info = calculate_data_age(
        posting_date=row.get("posting_date") or row.get("opening_date"),
        closing_date=row.get("closing_date"),
        enriched_at=row.get("enriched_at"),
    )
    result.update(age_info)
    
    # 2. Apply ECI aging to salaries
    if row.get("salary_min"):
        eci_min = apply_eci_aging(row.get("salary_min"), age_info.get("data_age_months"))
        result["salary_min_eci_adjusted"] = eci_min["salary_eci_adjusted"]
    else:
        result["salary_min_eci_adjusted"] = None
    
    if row.get("salary_max"):
        eci_max = apply_eci_aging(row.get("salary_max"), age_info.get("data_age_months"))
        result["salary_max_eci_adjusted"] = eci_max["salary_eci_adjusted"]
        result["eci_adjustment_pct"] = eci_max["eci_adjustment_pct"]
    else:
        result["salary_max_eci_adjusted"] = None
        result["eci_adjustment_pct"] = 0.0
    
    # 3. Normalize salary (use max or min)
    salary_for_norm = row.get("salary_max") or row.get("salary_min")
    hours = row.get("hours_per_week")
    norm_info = normalize_salary(salary_for_norm, hours)
    result.update(norm_info)
    
    # 4. Calculate confidence
    combined_row = {**row, **result}
    confidence = calculate_confidence_score(combined_row)
    result.update(confidence)
    
    return result


# Test
if __name__ == "__main__":
    print("Statistical Processing Test")
    print("=" * 60)
    
    # Test data age
    print("\n1. Data Age Calculation:")
    test_dates = [
        ("2025-12-01", None, None),  # ~1 month ago
        ("2025-06-01", None, None),  # ~7 months ago
        ("2024-01-01", None, None),  # ~2 years ago
        (None, None, "2026-01-01T00:00:00Z"),  # Fallback to enriched_at
    ]
    for pd, cd, ea in test_dates:
        result = calculate_data_age(pd, cd, ea)
        print(f"  {pd or cd or ea}: {result['data_age_months']} months ({result['data_freshness']})")
    
    # Test ECI aging
    print("\n2. ECI Adjustment:")
    test_salaries = [
        (50000, 6),   # 6 months old
        (50000, 12),  # 12 months old
        (50000, 24),  # 24 months old
    ]
    for salary, age in test_salaries:
        result = apply_eci_aging(salary, age)
        print(f"  ${salary:,} ({age}mo old) → ${result['salary_eci_adjusted']:,.2f} (+{result['eci_adjustment_pct']}%)")
    
    # Test normalization
    print("\n3. Salary Normalization:")
    test_norm = [
        (52000, "40"),
        (52000, "37.5"),
        (52000, "35"),
    ]
    for salary, hours in test_norm:
        result = normalize_salary(salary, hours)
        print(f"  ${salary:,} at {hours}hrs/wk → ${result['salary_40hr_equivalent']:,.0f} (40hr equiv), ${result['effective_hourly_rate']:.2f}/hr")
