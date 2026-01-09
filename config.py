"""
Configuration constants for the job enrichment pipeline.
"""
import os
from dotenv import load_dotenv

# Load .env variables globally
load_dotenv()

# OpenRouter API Configuration
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Model configuration - single model with retries (no fallbacks)
MODEL_NAME = "openai/gpt-oss-120b"
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 1536  # Reduced from 3072 to fit pgvector index limit (max 2000)

# Provider routing preferences
PROVIDER_PREFERENCES = {
    "sort": "price",            # Always use cheapest available provider
    "allow_fallbacks": True,    # Allow OpenRouter to try different providers for same model
}

# Processing Configuration
PARALLEL_WORKERS = 50           # Number of concurrent API calls (50 balances speed vs rate limits)
BATCH_SIZE = 1                  # Save after EVERY row for crash recovery
MAX_RETRIES = 5                 # Retry attempts per job
RETRY_DELAY_SECONDS = 2         # Base delay for exponential backoff
REQUEST_TIMEOUT_SECONDS = 180   # Increased timeout for reliability

# Standardized Job Families (19 options - LLM MUST choose from this list)
JOB_FAMILIES = [
    "Administration/Clerk",
    "Building/Code Enforcement",
    "Emergency Communications/Dispatch",
    "Engineering/Technical Services",
    "Executive/Leadership",
    "Finance/Budget/Accounting",
    "Fire/Emergency Services",
    "Human Resources/Personnel",
    "Human Services/Social Services",
    "Information Technology",
    "Legal/Court Services",
    "Library Services",
    "Parks/Recreation/Culture",
    "Planning/Community Development",
    "Police/Law Enforcement",
    "Public Health/Medical",
    "Public Works/Utilities/Infrastructure",
    "Transportation/Fleet/Traffic",
    "Other Municipal Services",
]

# Standardized Job Levels (8 options - LLM MUST choose from this list)
JOB_LEVELS = [
    "Executive",
    "Director",
    "Manager",
    "Supervisor",
    "Senior Individual Contributor",
    "Individual Contributor",
    "Trainee/Entry-Level",
    "Seasonal/Temporary",
]

# Employer Types (12 options - LLM MUST choose from this list)
EMPLOYER_TYPES = [
    "City/Municipal Government",
    "County Government",
    "State Government",
    "Federal Government",
    "School District (K-12)",
    "Community College/University",
    "Special District (Water/Sewer/Fire)",
    "Transit Authority",
    "Hospital/Healthcare District",
    "Housing Authority",
    "Tribal Government",
    "Other Public Entity",
]

# Decision Band Method - 6 bands for job complexity hierarchy
DBM_BANDS = [
    "A - Defined (follows instructions)",
    "B - Operational (coordinates processes)",
    "C - Process (diagnoses and selects solutions)",
    "D - Interpretive (translates policy to programs)",
    "E - Programming (strategic resource allocation)",
    "F - Policy (sets organizational mission)",
]

# Pension/retirement plan types
PENSION_TYPES = [
    "Defined Benefit",
    "Defined Contribution (401a/457)",
    "Hybrid",
    "Social Security Only",
    "Unknown/Not Specified",
]

# Output Schema - columns added by enrichment
ENRICHMENT_COLUMNS = [
    # Core classification (structured for hybrid search)
    "job_family",           # Constrained to JOB_FAMILIES enum
    "job_subfamily",        # Free text specialization within family
    "job_level",            # Constrained to JOB_LEVELS enum
    "employer_type",        # Constrained to EMPLOYER_TYPES enum
    
    # Numeric scoring for regression analysis (Phase 1)
    "dbm_band",             # Decision Band Method - constrained to DBM_BANDS enum
    "complexity_score",     # Integer 1-100 for job complexity
    
    # Vector-optimized summary
    "compensation_summary", # Structured format for embedding
    
    # Quantitative compensable factors
    "fte_managed",          # Headcount: "0", "1-5", "6-20", "20+", "100+"
    "budget_authority",     # Dollar amount or "None"
    "scope_of_impact",      # "Team", "Division", "Department", "City-wide", "Regional"
    
    # Hard requirements
    "licenses_required",    # JSON array of specific licenses
    "certifications_required",  # JSON array of certifications
    "education_minimum",    # Degree level
    "education_field",      # Field of study
    "years_experience",     # Experience range
    "specialized_systems",  # JSON array of domain software
    "specialized_knowledge", # Domain expertise required
    
    # Work context
    "supervision_given",    # Who they manage
    "supervision_received", # Who manages them
    "physical_context",     # Contextualized physical demands
    "flsa_likely",          # Exempt/Non-Exempt
    "work_schedule",        # Schedule type
    "hours_per_week",       # Weekly hours (Phase 5)
    
    # Risk/responsibility
    "consequence_of_error", # Risk level of mistakes
    "decision_authority",   # Types of decisions made
    
    # Benefits & total compensation (Phase 3)
    "pension_type",         # Constrained to PENSION_TYPES enum
    "retirement_system",    # Specific system name (CALPERS, TMRS, etc.)
    "benefits_mentioned",   # JSON array of benefits
    
    # Labor relations (Phase 4)
    "is_union_indicated",   # "true", "false", or "unknown"
    "bargaining_unit_name", # Union name if mentioned
    
    # Metadata (Phase 2)
    "enriched_at",          # ISO timestamp when enriched
]
