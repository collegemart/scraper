#!/usr/bin/env python3
"""
BharatFare Master Orchestrator — runs all spiders, merges CSVs,
runs enrichment, and generates summary statistics.

Usage:
    python run_master.py                    # Run all spiders + enrichment
    python run_master.py --spiders-only     # Run only the spiders (no enrichment)
    python run_master.py --enrich-only      # Run only enrichment on latest CSV
    python run_master.py --workers 4        # Set enrichment workers

Spiders are run in 4 phases:
  Phase 1: Indian Directories (indiamart, tradeindia, exportersindia, indianyellowpages, justdial, fundoodata)
  Phase 2: International Directories (clutch, goodfirms)
  Phase 3: Signal Enrichment (indeed, googlemaps)
  Phase 4: Email Enrichment (website_emails)

After all spiders finish, CSVs are merged and enrichment is run.
"""

import csv
import glob
import os
import subprocess
import sys
import argparse
from datetime import datetime


def get_paths():
    """Get project paths."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    venv_dir = os.path.normpath(os.path.join(script_dir, '..', '.venv', 'Scripts'))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)

    scrapy_exe = os.path.join(venv_dir, 'scrapy.exe')
    python_exe = os.path.join(venv_dir, 'python.exe')

    # Fallback: try system python/scrapy
    if not os.path.exists(scrapy_exe):
        scrapy_exe = 'scrapy'
    if not os.path.exists(python_exe):
        python_exe = sys.executable

    return script_dir, venv_dir, output_dir, scrapy_exe, python_exe


def run_spider(scrapy_exe, spider_name, output_csv, project_dir, log_file=None):
    """Run a single Scrapy spider."""
    cmd = [
        scrapy_exe, 'crawl', spider_name,
        '-o', f'{output_csv}:csv',
    ]
    if log_file:
        cmd.extend(['-s', f'LOG_FILE={log_file}'])

    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=project_dir,
        capture_output=True,
        text=True,
        timeout=3600,  # 1 hour max per spider
    )

    if result.returncode != 0:
        print(f"  ⚠ {spider_name} exited with code {result.returncode}")
        if result.stderr:
            # Print last few lines of stderr for debugging
            lines = result.stderr.strip().split('\n')
            for line in lines[-5:]:
                print(f"    {line}")
    else:
        print(f"  ✓ {spider_name} complete")

    return result.returncode


def merge_csvs(csv_files, output_file, output_fields=None):
    """Merge multiple CSV files, deduplicating headers."""
    print(f"\n  Merging {len(csv_files)} CSV files...")
    all_rows = []
    headers = None

    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            print(f"    ⚠ Skipping (not found): {csv_file}")
            continue

        try:
            with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if headers is None and reader.fieldnames:
                    headers = reader.fieldnames
                all_rows.extend(rows)
                print(f"    + {os.path.basename(csv_file)}: {len(rows)} rows")
        except Exception as e:
            print(f"    ⚠ Error reading {csv_file}: {e}")

    if not all_rows:
        print("  ⚠ No rows to merge!")
        return 0

    # Use specified output fields or headers from first file
    fields = output_fields or headers or list(all_rows[0].keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"  ✓ Merged: {len(all_rows)} total rows → {output_file}")
    return len(all_rows)


def count_csv_rows(csv_file):
    """Count rows in a CSV file."""
    if not os.path.exists(csv_file):
        return 0
    try:
        with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
            return sum(1 for _ in f) - 1  # Subtract header
    except Exception:
        return 0


def print_summary(merged_csv):
    """Print summary statistics of the merged CSV."""
    if not os.path.exists(merged_csv):
        return

    with open(merged_csv, 'r', encoding='utf-8', errors='replace') as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    has_email = sum(1 for r in rows if (r.get('contact_email') or '').strip())
    has_phone = sum(1 for r in rows if (r.get('phone') or '').strip())
    has_website = sum(1 for r in rows if (r.get('company_website') or '').strip())
    has_both = sum(
        1 for r in rows
        if (r.get('contact_email') or '').strip() and (r.get('phone') or '').strip()
    )
    has_linkedin = sum(1 for r in rows if (r.get('linkedin_company_url') or '').strip())

    # Count by sector
    sectors = {}
    for r in rows:
        sector = (r.get('sector') or 'Unknown').strip()
        sectors[sector] = sectors.get(sector, 0) + 1

    print(f"\n{'='*60}")
    print(f"LEAD SUMMARY")
    print(f"{'='*60}")
    print(f"Total leads:           {total}")
    print(f"Has email:             {has_email} ({has_email*100//max(total,1)}%)")
    print(f"Has phone:             {has_phone} ({has_phone*100//max(total,1)}%)")
    print(f"Has both:              {has_both} ({has_both*100//max(total,1)}%)")
    print(f"Has website:           {has_website} ({has_website*100//max(total,1)}%)")
    print(f"Has LinkedIn:          {has_linkedin} ({has_linkedin*100//max(total,1)}%)")

    print(f"\nBy Sector:")
    for sector, count in sorted(sectors.items(), key=lambda x: -x[1]):
        print(f"  {sector:<30} {count}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='BharatFare Master Lead Scraper')
    parser.add_argument('--spiders-only', action='store_true',
                        help='Only run spiders, skip enrichment')
    parser.add_argument('--enrich-only', action='store_true',
                        help='Only run enrichment on latest merged CSV')
    parser.add_argument('--workers', type=int, default=8,
                        help='Number of enrichment workers (default: 8)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit enrichment to first N leads')
    args = parser.parse_args()

    script_dir, venv_dir, output_dir, scrapy_exe, python_exe = get_paths()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M')

    # All spider phases
    PHASES = [
        ('Phase 1: Indian Directories', [
            'indiamart', 'tradeindia', 'exportersindia',
            'indianyellowpages', 'justdial', 'fundoodata',
        ]),
        ('Phase 2: International Directories', [
            'clutch', 'goodfirms',
        ]),
        ('Phase 3: Signal Enrichment', [
            'indeed', 'googlemaps',
        ]),
    ]

    merged_csv = os.path.join(output_dir, f'leads_merged_{timestamp}.csv')
    enriched_csv = os.path.join(output_dir, f'leads_enriched_{timestamp}.csv')

    if args.enrich_only:
        # Find latest merged CSV
        pattern = os.path.join(output_dir, 'leads_merged_*.csv')
        candidates = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        if not candidates:
            # Try fresh_merged fallback
            fallback = os.path.join(output_dir, 'fresh_merged.csv')
            if os.path.exists(fallback):
                candidates = [fallback]

        if not candidates:
            print("ERROR: No merged CSV found for enrichment!")
            sys.exit(1)

        merged_csv = candidates[0]
        print(f"Using existing merged CSV: {merged_csv}")
    else:
        # Run spiders
        print(f"\n{'='*60}")
        print(f"BharatFare Lead Scraper — Full Pipeline")
        print(f"{'='*60}")
        print(f"Timestamp: {timestamp}")
        print(f"Output:    {output_dir}")
        print()

        spider_csvs = []

        for phase_name, spiders in PHASES:
            print(f"\n=== {phase_name} ===\n")

            for spider in spiders:
                csv_file = os.path.join(output_dir, f'{spider}_{timestamp}.csv')
                log_file = os.path.join(output_dir, f'{spider}_{timestamp}.log')

                print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                      f"Starting {spider} spider...")

                try:
                    run_spider(scrapy_exe, spider, csv_file, script_dir, log_file)
                except subprocess.TimeoutExpired:
                    print(f"  ⚠ {spider} timed out after 1 hour!")
                except Exception as e:
                    print(f"  ⚠ {spider} error: {e}")

                if os.path.exists(csv_file):
                    rows = count_csv_rows(csv_file)
                    print(f"    → {rows} leads extracted")
                    spider_csvs.append(csv_file)
                else:
                    print(f"    → No output file generated")

                print()

        # Merge all CSVs
        print(f"\n=== Phase 3.5: Merge All CSVs ===")
        total_merged = merge_csvs(spider_csvs, merged_csv)

        if total_merged == 0:
            print("\n⚠ No leads were scraped! Check spider logs for errors.")
            sys.exit(1)

    if args.spiders_only:
        print_summary(merged_csv)
        print(f"\n✓ Spiders complete. Merged CSV: {merged_csv}")
        return

    # Run enrichment
    print(f"\n=== Phase 4: Enrichment ===")
    print(f"  Input:   {merged_csv}")
    print(f"  Output:  {enriched_csv}")
    print(f"  Workers: {args.workers}")

    enrich_cmd = [
        python_exe, os.path.join(script_dir, 'enrich_leads.py'),
        merged_csv, enriched_csv,
        '--workers', str(args.workers),
    ]
    if args.limit:
        enrich_cmd.extend(['--limit', str(args.limit)])

    try:
        subprocess.run(enrich_cmd, cwd=script_dir, timeout=7200)  # 2 hours max
    except subprocess.TimeoutExpired:
        print("⚠ Enrichment timed out after 2 hours!")
    except Exception as e:
        print(f"⚠ Enrichment error: {e}")

    # Print summary
    final_csv = enriched_csv if os.path.exists(enriched_csv) else merged_csv
    print_summary(final_csv)

    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"  Spider output:    {output_dir}")
    print(f"  Merged leads:     {merged_csv}")
    print(f"  Enriched leads:   {enriched_csv}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
