"""Cleanup script to fix data quality issues in the jobs database.

This script:
1. Cleans jobs with contaminated data (parses location, cleans title/posted_at)
2. Re-fetches empty jobs using backfill
3. Deletes jobs that remain empty after retry
4. Optionally updates Google Sheets with corrected data
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .scrape_linkedin import (
    authorise_sheet,
    clean_posted_at,
    clean_title,
    ensure_storage,
    get_db_connection,
    load_settings,
    parse_location_field,
    setup_logging,
)
from .backfill_job_details import backfill_missing_details


def fetch_jobs_from_today(db_path: str, days_back: int = 1) -> List[Dict[str, Any]]:
    """Fetch all jobs from today (or last N days)."""
    cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT job_id, title, company, location, posting_url, description,
                   posted_at, applicants, scraped_at, appended_at, work_type
            FROM jobs
            WHERE scraped_at >= ?
            ORDER BY scraped_at DESC
            """,
            (cutoff_timestamp,)
        ).fetchall()
        return [dict(row) for row in rows]


def fetch_empty_jobs(db_path: str, days_back: int = 1) -> List[Dict[str, Any]]:
    """Fetch jobs with empty title/company/location."""
    cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT job_id, title, company, location, posting_url
            FROM jobs
            WHERE scraped_at >= ?
              AND (title IS NULL OR title = ''
                   OR company IS NULL OR company = ''
                   OR location IS NULL OR location = '')
            ORDER BY scraped_at DESC
            """,
            (cutoff_timestamp,)
        ).fetchall()
        return [dict(row) for row in rows]


def fetch_contaminated_jobs(db_path: str, days_back: int = 1) -> List[Dict[str, Any]]:
    """Fetch jobs that need cleaning (title with newlines, location with work type, posted_at with extra lines)."""
    cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    with get_db_connection(db_path) as conn:
        # Use char(10) for newline in SQL LIKE patterns
        rows = conn.execute(
            """
            SELECT job_id, title, company, location, posting_url, description,
                   posted_at, applicants, scraped_at, appended_at, work_type
            FROM jobs
            WHERE scraped_at >= ?
              AND title IS NOT NULL AND title != ''
              AND (
                  title LIKE '%' || char(10) || '%'
                  OR title LIKE '%with verification%'
                  OR location LIKE '%·%'
                  OR location LIKE '%ago%'
                  OR location LIKE '%(Remote)%'
                  OR location LIKE '%(Hybrid)%'
                  OR location LIKE '%(On-site)%'
                  OR posted_at LIKE '%' || char(10) || '%'
              )
            ORDER BY scraped_at DESC
            """,
            (cutoff_timestamp,)
        ).fetchall()
        return [dict(row) for row in rows]


def clean_job_data(db_path: str, job: Dict[str, Any]) -> bool:
    """Clean a single job's data and update the database.

    Returns True if any changes were made.
    """
    job_id = job['job_id']
    changes = {}

    # Clean title
    raw_title = job.get('title') or ''
    cleaned_title = clean_title(raw_title)
    if cleaned_title != raw_title:
        changes['title'] = cleaned_title

    # Clean posted_at
    raw_posted_at = job.get('posted_at') or ''
    cleaned_posted_at = clean_posted_at(raw_posted_at)
    if cleaned_posted_at != raw_posted_at:
        changes['posted_at'] = cleaned_posted_at

    # Parse location field - check for work types or other metadata
    raw_location = job.get('location') or ''
    needs_location_parsing = (
        '·' in raw_location
        or 'ago' in raw_location.lower()
        or '(Remote)' in raw_location
        or '(Hybrid)' in raw_location
        or '(On-site)' in raw_location
    )
    if raw_location and needs_location_parsing:
        loc_parsed = parse_location_field(raw_location)

        if loc_parsed['location'] != raw_location:
            changes['location'] = loc_parsed['location']

        if loc_parsed['work_type'] and not job.get('work_type'):
            changes['work_type'] = loc_parsed['work_type']

        if loc_parsed['posted_at'] and not job.get('posted_at'):
            changes['posted_at'] = clean_posted_at(loc_parsed['posted_at'])

        if loc_parsed['applicants'] and not job.get('applicants'):
            changes['applicants'] = loc_parsed['applicants']

    if not changes:
        return False

    # Build and execute update query
    set_clauses = [f"{key} = ?" for key in changes.keys()]
    values = list(changes.values()) + [job_id]

    with get_db_connection(db_path) as conn:
        conn.execute(
            f"UPDATE jobs SET {', '.join(set_clauses)} WHERE job_id = ?",
            values
        )
        conn.commit()

    logging.info("Cleaned job %s: %s", job_id, list(changes.keys()))
    return True


def delete_empty_jobs(db_path: str, days_back: int = 1) -> int:
    """Delete jobs that still have empty title/company/location after cleanup."""
    cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    with get_db_connection(db_path) as conn:
        # Get job IDs before deleting for logging
        rows = conn.execute(
            """
            SELECT job_id FROM jobs
            WHERE scraped_at >= ?
              AND (title IS NULL OR title = '')
              AND (company IS NULL OR company = '')
              AND (location IS NULL OR location = '')
            """,
            (cutoff_timestamp,)
        ).fetchall()

        job_ids = [row['job_id'] for row in rows]

        if job_ids:
            placeholders = ','.join('?' * len(job_ids))
            conn.execute(f"DELETE FROM jobs WHERE job_id IN ({placeholders})", job_ids)
            conn.commit()
            logging.info("Deleted %s jobs with empty data: %s", len(job_ids), job_ids[:5])

        return len(job_ids)


def reset_appended_status(db_path: str, days_back: int = 1) -> int:
    """Reset appended_at for jobs from today so they can be re-appended to Sheets."""
    cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    with get_db_connection(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE jobs SET appended_at = NULL
            WHERE scraped_at >= ?
              AND appended_at IS NOT NULL
            """,
            (cutoff_timestamp,)
        )
        conn.commit()
        return cursor.rowcount


async def run_cleanup(
    days_back: int = 1,
    run_backfill: bool = True,
    delete_empty: bool = False,
    reset_sheets: bool = False
) -> None:
    """Main cleanup function.

    Args:
        days_back: Number of days to look back for jobs
        run_backfill: Whether to re-fetch empty jobs via backfill
        delete_empty: Whether to delete jobs that remain empty after cleanup
        reset_sheets: Whether to reset appended_at so jobs are re-appended to Sheets
    """
    setup_logging()
    settings = load_settings()
    db_path = settings["db_path"]

    ensure_storage(db_path)

    # Step 1: Report current state
    all_jobs = fetch_jobs_from_today(db_path, days_back)
    empty_jobs = fetch_empty_jobs(db_path, days_back)
    contaminated_jobs = fetch_contaminated_jobs(db_path, days_back)

    logging.info("=== INITIAL STATE ===")
    logging.info("Total jobs from last %s day(s): %s", days_back, len(all_jobs))
    logging.info("Jobs with empty title/company/location: %s", len(empty_jobs))
    logging.info("Jobs with contaminated location: %s", len(contaminated_jobs))
    logging.info("=====================")

    # Step 2: Clean contaminated jobs
    if contaminated_jobs:
        logging.info("Cleaning %s jobs with contaminated data...", len(contaminated_jobs))
        cleaned_count = 0
        for job in contaminated_jobs:
            if clean_job_data(db_path, job):
                cleaned_count += 1
        logging.info("Cleaned %s jobs", cleaned_count)

    # Step 3: Run backfill to re-fetch empty jobs
    if run_backfill and empty_jobs:
        logging.info("Running backfill for %s empty jobs...", len(empty_jobs))
        await backfill_missing_details(days_back=days_back, max_jobs=len(empty_jobs))

    # Step 4: Check remaining empty jobs
    remaining_empty = fetch_empty_jobs(db_path, days_back)
    logging.info("Jobs still empty after backfill: %s", len(remaining_empty))

    # Step 5: Optionally delete jobs that remain empty
    if delete_empty and remaining_empty:
        deleted = delete_empty_jobs(db_path, days_back)
        logging.info("Deleted %s jobs with empty data", deleted)

    # Step 6: Optionally reset appended_at to re-sync with Sheets
    if reset_sheets:
        reset_count = reset_appended_status(db_path, days_back)
        logging.info("Reset appended_at for %s jobs (will be re-appended to Sheets)", reset_count)
        logging.info("Run 'appendjobs' to re-append cleaned jobs to Google Sheets")

    # Final report
    final_jobs = fetch_jobs_from_today(db_path, days_back)
    final_empty = fetch_empty_jobs(db_path, days_back)
    final_contaminated = fetch_contaminated_jobs(db_path, days_back)

    logging.info("=== FINAL STATE ===")
    logging.info("Total jobs: %s", len(final_jobs))
    logging.info("Empty jobs: %s", len(final_empty))
    logging.info("Contaminated jobs: %s", len(final_contaminated))
    logging.info("===================")


def main() -> None:
    """CLI entry point for cleanup script."""
    import argparse

    parser = argparse.ArgumentParser(description="Clean up job data quality issues")
    parser.add_argument("--days", type=int, default=1, help="Days to look back (default: 1)")
    parser.add_argument("--no-backfill", action="store_true", help="Skip backfill step")
    parser.add_argument("--delete-empty", action="store_true", help="Delete jobs that remain empty")
    parser.add_argument("--reset-sheets", action="store_true", help="Reset appended_at to re-sync with Sheets")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()

    if args.dry_run:
        setup_logging()
        settings = load_settings()
        db_path = settings["db_path"]
        ensure_storage(db_path)

        all_jobs = fetch_jobs_from_today(db_path, args.days)
        empty_jobs = fetch_empty_jobs(db_path, args.days)
        contaminated_jobs = fetch_contaminated_jobs(db_path, args.days)

        logging.info("=== DRY RUN - Current State ===")
        logging.info("Total jobs from last %s day(s): %s", args.days, len(all_jobs))
        logging.info("Jobs with empty data: %s", len(empty_jobs))
        logging.info("Jobs with contaminated location: %s", len(contaminated_jobs))

        if contaminated_jobs:
            logging.info("\nSample contaminated locations:")
            for job in contaminated_jobs[:5]:
                logging.info("  Job %s: %s", job['job_id'], job['location'][:80])

        if empty_jobs:
            logging.info("\nSample empty jobs:")
            for job in empty_jobs[:5]:
                logging.info("  Job %s: title=%s, company=%s, location=%s",
                           job['job_id'],
                           bool(job.get('title')),
                           bool(job.get('company')),
                           bool(job.get('location')))
        return

    asyncio.run(run_cleanup(
        days_back=args.days,
        run_backfill=not args.no_backfill,
        delete_empty=args.delete_empty,
        reset_sheets=args.reset_sheets,
    ))


if __name__ == "__main__":
    main()
