#!/usr/bin/env python3
"""
Merge ALL output CSVs from all machines into one file,
then run enrichment on it.

Usage:
    python merge_all.py                  # just merge CSVs in output/ folder
    python merge_all.py --enrich         # merge + run enrichment
    python merge_all.py --enrich --workers 8
"""
import csv
import os
import sys
import glob
import argparse
import subprocess
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')

def merge_all(enrich=False, workers=8):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Find all CSV files in output folder (exclude merged/enriched ones)
    all_csvs = [
        f for f in glob.glob(os.path.join(OUTPUT_DIR, '*.csv'))
        if 'merged' not in os.path.basename(f).lower()
        and 'enriched' not in os.path.basename(f).lower()
    ]

    if not all_csvs:
        print("No CSV files found in output/ folder!")
        print("Copy all CSV files from your friend's laptop and Codespaces into the output/ folder first.")
        return

    print(f"\nFound {len(all_csvs)} CSV files to merge:")

    all_rows = []
    all_headers = set()

    for csv_file in sorted(all_csvs):
        try:
            with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if reader.fieldnames:
                    all_headers.update(reader.fieldnames)
                all_rows.extend(rows)
                print(f"  + {os.path.basename(csv_file):40s} → {len(rows):6,} rows")
        except Exception as e:
            print(f"  ✗ Error reading {csv_file}: {e}")

    if not all_rows:
        print("No rows found in any CSV!")
        return

    # Deduplicate by company_name (simple dedup)
    seen = set()
    deduped = []
    for row in all_rows:
        name = (row.get('company_name') or row.get('Company Name') or '').lower().strip()
        if name and name not in seen:
            seen.add(name)
            deduped.append(row)
        elif not name:
            deduped.append(row)  # keep rows without names too

    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    merged_file = os.path.join(OUTPUT_DIR, f'ALL_MERGED_{timestamp}.csv')

    # Define the standard 16 output columns
    OUTPUT_FIELDS = [
        'company_name', 'company_website', 'sector', 'company_size', 'hq_city',
        'office_locations', 'contact_email', 'contact_person', 'designation',
        'contact_linkedin', 'phone', 'linkedin_company_url', 'company_revenue',
        'has_international_hiring', 'estimated_travel_frequency', 'source_url',
    ]

    # Use output fields, plus any extra columns found
    headers = OUTPUT_FIELDS + [h for h in sorted(all_headers) if h not in OUTPUT_FIELDS]

    with open(merged_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(deduped)

    print(f"\n{'='*55}")
    print(f"  Raw rows collected:       {len(all_rows):,}")
    print(f"  After deduplication:      {len(deduped):,}")
    print(f"  Merged file:              {os.path.basename(merged_file)}")

    # Stats
    has_email = sum(1 for r in deduped if (r.get('contact_email') or '').strip())
    has_phone = sum(1 for r in deduped if (r.get('phone') or '').strip())
    print(f"  Has email:                {has_email:,}")
    print(f"  Has phone:                {has_phone:,}")
    print(f"{'='*55}\n")

    if enrich:
        enriched_file = os.path.join(OUTPUT_DIR, f'FINAL_ENRICHED_{timestamp}.csv')
        print(f"Starting enrichment with {workers} workers...")
        print(f"Output: {enriched_file}\n")
        subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), 'enrich_leads.py'),
            merged_file, enriched_file,
            '--workers', str(workers),
        ])

    print("\nDone! Check your output/ folder.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--enrich', action='store_true', help='Run enrichment after merging')
    parser.add_argument('--workers', type=int, default=8, help='Enrichment workers (default: 8)')
    args = parser.parse_args()
    merge_all(enrich=args.enrich, workers=args.workers)
