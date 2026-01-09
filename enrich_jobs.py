#!/usr/bin/env python3
"""
Job Enrichment Pipeline - High-Performance Version

Enriches municipal job postings with structured compensation factors.
Optimized for large-scale processing (tens of thousands of rows).

Features:
- 50+ parallel workers for high throughput
- Streaming writes - each row saved immediately to CSV
- Robust crash recovery - resume from exact row after any failure
- Progress tracking with ETA

Usage:
    python enrich_jobs.py --input "jobs.csv" --output "enriched.csv"
    python enrich_jobs.py --input "jobs.csv" --output "enriched.csv" --workers 100
    python enrich_jobs.py --input "jobs.csv" --output "enriched.csv" --resume
"""

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

from config import (
    OPENROUTER_BASE_URL,
    MODEL_NAME,
    PROVIDER_PREFERENCES,
    PARALLEL_WORKERS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    ENRICHMENT_COLUMNS,
)
from prompts import build_messages

# Thread-safe locks
write_lock = Lock()
progress_lock = Lock()


def get_api_key() -> str:
    """Get OpenRouter API key from environment."""
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("Error: OPENROUTER_API_KEY environment variable not set.")
        print("Set it with: export OPENROUTER_API_KEY='your-key-here'")
        sys.exit(1)
    return key


def create_client(api_key: str) -> OpenAI:
    """Create OpenRouter-compatible OpenAI client."""
    return OpenAI(
        base_url=OPENROUTER_BASE_URL,
        api_key=api_key,
        default_headers={
            "HTTP-Referer": "https://github.com/municipal-job-enrichment",
            "X-Title": "Municipal Job Enrichment Pipeline",
        },
    )


def parse_llm_response(content: str) -> dict:
    """Parse the LLM response as JSON, with fallback handling."""
    content = content.strip()
    
    if content.startswith("```"):
        lines = content.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines)
    
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        return {"_parse_error": str(e), "_raw_content": content[:1000]}


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_DELAY_SECONDS, min=2, max=60),
    retry=retry_if_exception_type((Exception,)),
    reraise=True,
)
def enrich_single_job(
    client: OpenAI,
    job_title: str,
    employer: str,
    description: str,
    department: str = "",
    job_type: str = "",
    city: str = "",
    state: str = "",
    salary_min: str = "",
    salary_max: str = "",
    salary_type: str = "",
) -> dict:
    """Call the LLM to enrich a single job posting with retries."""
    messages = build_messages(
        job_title=job_title,
        employer=employer,
        description=description,
        department=department,
        job_type=job_type,
        city=city,
        state=state,
        salary_min=salary_min,
        salary_max=salary_max,
        salary_type=salary_type,
    )
    
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        temperature=0.1,
        max_tokens=2500,
        timeout=REQUEST_TIMEOUT_SECONDS,
        extra_body={
            "provider": PROVIDER_PREFERENCES,
        },
    )
    
    content = response.choices[0].message.content
    return parse_llm_response(content)


def process_single_row(client: OpenAI, row: dict, idx: int) -> tuple[int, dict, str | None]:
    """Process a single row and return (index, enrichment_dict, error_message)."""
    job_title = str(row.get("job_title", "") or "")
    employer = str(row.get("employer", "") or "")
    description = str(row.get("description", "") or "")
    department = str(row.get("department", "") or "")
    job_type = str(row.get("job_type", "") or "")
    city = str(row.get("city", "") or "")
    state = str(row.get("state", "") or "")
    salary_min = str(row.get("salary_min", "") or "")
    salary_max = str(row.get("salary_max", "") or "")
    salary_type = str(row.get("salary_type", "") or "")
    
    try:
        enrichment = enrich_single_job(
            client=client,
            job_title=job_title,
            employer=employer,
            description=description,
            department=department,
            job_type=job_type,
            city=city,
            state=state,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_type=salary_type,
        )
        
        if "_parse_error" in enrichment:
            return (idx, {col: "" for col in ENRICHMENT_COLUMNS}, f"JSON parse error")
        
        # Add enriched_at timestamp (not from LLM)
        enrichment['enriched_at'] = datetime.now(timezone.utc).isoformat()
        
        return (idx, enrichment, None)
        
    except Exception as e:
        return (idx, {col: "" for col in ENRICHMENT_COLUMNS}, str(e)[:100])


def flatten_enrichment(enrichment: dict) -> dict:
    """Flatten enrichment dict, converting lists to JSON strings."""
    result = {}
    for col in ENRICHMENT_COLUMNS:
        value = enrichment.get(col)
        if isinstance(value, list):
            result[col] = json.dumps(value) if len(value) > 0 else ""
        elif value is None:
            result[col] = ""
        else:
            result[col] = str(value)
    return result


def get_progress_file(output_path: Path) -> Path:
    """Get progress tracking file path."""
    return output_path.with_suffix(".progress.json")


def load_progress(progress_file: Path) -> set:
    """Load set of already-processed row indices."""
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            data = json.load(f)
            return set(data.get("processed_indices", []))
    return set()


def save_progress(progress_file: Path, processed_indices: set, total: int, errors: int):
    """Save progress to file."""
    with progress_lock:
        with open(progress_file, 'w') as f:
            json.dump({
                "processed_indices": list(processed_indices),
                "total": total,
                "errors": errors,
                "last_updated": datetime.now().isoformat(),
            }, f)


def process_jobs(
    input_path: Path,
    output_path: Path,
    limit: int | None = None,
    resume: bool = False,
    dry_run: bool = False,
    workers: int = PARALLEL_WORKERS,
) -> None:
    """Main processing function with streaming writes."""
    print(f"Loading input file: {input_path}")
    
    # Read input CSV
    df = pd.read_csv(input_path)
    total_rows = len(df)
    print(f"Loaded {total_rows} rows")
    
    if limit:
        print(f"Limiting to first {limit} rows")
        df = df.head(limit)
        total_rows = len(df)
    
    # Get all column names for output
    input_columns = list(df.columns)
    output_columns = input_columns + [c for c in ENRICHMENT_COLUMNS if c not in input_columns]
    
    # Progress tracking
    progress_file = get_progress_file(output_path)
    processed_indices = set()
    
    if resume and progress_file.exists():
        processed_indices = load_progress(progress_file)
        print(f"Resuming: {len(processed_indices)} rows already processed")
    
    # Determine rows to process
    rows_to_process = [i for i in range(total_rows) if i not in processed_indices]
    
    if len(rows_to_process) == 0:
        print("All rows already processed!")
        return
    
    print(f"\nProcessing {len(rows_to_process)} rows with {workers} parallel workers...")
    print(f"Output: {output_path}")
    print(f"Progress file: {progress_file}")
    
    if dry_run:
        print("\n=== DRY RUN MODE ===")
        print(f"Would process {len(rows_to_process)} rows")
        return
    
    api_key = get_api_key()
    client = create_client(api_key)
    
    # Initialize output file if new run
    if not resume or not output_path.exists():
        # Write header with all columns
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_columns)
            writer.writeheader()
        
        # Write already-processed rows if resuming from a different output
        if processed_indices:
            print(f"Writing {len(processed_indices)} previously processed rows...")
    
    errors = []
    error_count = 0
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all jobs
        futures = {}
        for idx in rows_to_process:
            row_dict = df.iloc[idx].to_dict()
            future = executor.submit(process_single_row, client, row_dict, idx)
            futures[future] = (idx, row_dict)
        
        # Process results as they complete - with streaming writes
        with tqdm(total=len(rows_to_process), desc="Enriching") as pbar:
            for future in as_completed(futures):
                idx, original_row = futures[future]
                result_idx, enrichment, error = future.result()
                
                # Merge original row with enrichment
                flat = flatten_enrichment(enrichment)
                output_row = {**original_row, **flat}
                
                # Stream write to CSV immediately (thread-safe)
                with write_lock:
                    with open(output_path, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=output_columns)
                        writer.writerow(output_row)
                    
                    # Update progress
                    processed_indices.add(idx)
                
                if error:
                    errors.append({"idx": idx, "error": error})
                    error_count += 1
                
                pbar.update(1)
                
                # Save progress frequently
                if len(processed_indices) % 10 == 0:
                    save_progress(progress_file, processed_indices, total_rows, error_count)
                    
                    # Update ETA
                    elapsed = time.time() - start_time
                    rate = pbar.n / elapsed if elapsed > 0 else 0
                    remaining = len(rows_to_process) - pbar.n
                    eta = remaining / rate if rate > 0 else 0
                    pbar.set_postfix_str(f"ETA: {timedelta(seconds=int(eta))}")
    
    # Final progress save
    save_progress(progress_file, processed_indices, total_rows, error_count)
    
    # Summary
    elapsed = time.time() - start_time
    success_count = len(rows_to_process) - len(errors)
    rate = len(rows_to_process) / elapsed if elapsed > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"COMPLETE!")
    print(f"{'='*60}")
    print(f"Processed: {len(rows_to_process)} rows in {timedelta(seconds=int(elapsed))}")
    print(f"Success: {success_count} ({100*success_count/len(rows_to_process):.1f}%)")
    print(f"Errors: {len(errors)}")
    print(f"Rate: {rate:.1f} rows/sec")
    print(f"Output: {output_path}")
    
    if errors:
        print(f"\nFirst 10 errors:")
        for err in errors[:10]:
            print(f"  Row {err['idx']}: {err['error']}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich municipal job postings with compensation factors (high-performance)"
    )
    parser.add_argument("--input", "-i", required=True, help="Input CSV file path")
    parser.add_argument("--output", "-o", required=True, help="Output CSV file path")
    parser.add_argument("--limit", "-l", type=int, default=None, help="Limit number of rows")
    parser.add_argument("--resume", "-r", action="store_true", help="Resume from progress file")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Dry run mode")
    parser.add_argument("--workers", "-w", type=int, default=PARALLEL_WORKERS, 
                        help=f"Parallel workers (default: {PARALLEL_WORKERS})")
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"JOB ENRICHMENT PIPELINE")
    print(f"{'='*60}")
    print(f"Workers: {args.workers}")
    print(f"Resume: {args.resume}")
    print(f"{'='*60}\n")
    
    process_jobs(
        input_path=input_path,
        output_path=output_path,
        limit=args.limit,
        resume=args.resume,
        dry_run=args.dry_run,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
