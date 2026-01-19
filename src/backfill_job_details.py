"""Backfill missing job details (title, company, location) from job URLs."""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import BrowserContext, TimeoutError as PlaywrightTimeoutError, async_playwright
from playwright_stealth import stealth_async

from .scrape_linkedin import (
    clean_posted_at,
    clean_title,
    ensure_storage,
    extract_text,
    get_db_connection,
    is_masked,
    load_cookies,
    load_settings,
    parse_location_field,
    setup_logging,
)


def fetch_jobs_with_missing_details(db_path: str, days_back: int = 2) -> List[Dict[str, Any]]:
    """Fetch jobs from the last N days that are missing title, company, location, or description."""
    cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT job_id, title, company, location, posting_url, description,
                   posted_at, applicants, scraped_at, appended_at, work_type
            FROM jobs
            WHERE scraped_at >= ?
              AND (title IS NULL OR title = '' OR company IS NULL OR company = ''
                   OR location IS NULL OR location = ''
                   OR description IS NULL OR description = '')
              AND posting_url IS NOT NULL AND posting_url != ''
            ORDER BY scraped_at DESC
            """,
            (cutoff_timestamp,)
        ).fetchall()
        return [dict(row) for row in rows]


async def fetch_job_details_from_url(
    context: BrowserContext,
    job: Dict[str, Any],
    pause_seconds: float = 5.0
) -> Optional[Dict[str, str]]:
    """Visit a job URL and extract title, company, and location."""
    posting_url = job.get("posting_url")
    if not posting_url:
        logging.warning("Job %s has no posting_url, skipping", job.get("job_id"))
        return None

    page = await context.new_page()
    try:
        await stealth_async(page)
        logging.info("Fetching details for job %s from %s", job["job_id"], posting_url)
        await page.goto(posting_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(8)  # Wait longer for React to render

        # LinkedIn uses obfuscated class names, so we use JavaScript to extract by structure
        js_code = """() => {
            let title = '';
            let company = '';
            let location = '';
            let workType = '';
            let postedAt = '';
            let applicants = '';
            let description = '';

            // Find company by link to /company/
            const companyLinks = document.querySelectorAll('a[href*="/company/"]');
            for (const link of companyLinks) {
                const text = link.innerText.trim();
                if (text && !text.includes('follower') && text.length < 100) {
                    company = text;
                    break;
                }
            }

            // Get title from page title
            const pageTitle = document.title || '';
            const titleMatch = pageTitle.match(/^([^|]+)/);
            if (titleMatch) {
                title = titleMatch[1].trim();
            }

            // Find location and metadata from the body text
            const bodyText = document.body.innerText || '';

            // Look for pattern: "Location · X time ago · Y applicants"
            const metaMatch = bodyText.match(/([A-Za-z][A-Za-z ,]+(?:India|Area)?)\\s*[·•]\\s*(\\d+\\s*(?:hours?|days?|weeks?|months?|minutes?)\\s*ago)/i);
            if (metaMatch) {
                location = metaMatch[1].trim();
                postedAt = metaMatch[2].trim();
            }

            // Look for work type
            if (bodyText.includes('On-site')) workType = 'On-site';
            else if (bodyText.includes('Hybrid')) workType = 'Hybrid';
            else if (bodyText.includes('Remote')) workType = 'Remote';

            // Look for applicants
            const appMatch = bodyText.match(/(\\d+)\\s*applicants?/i);
            if (appMatch) {
                applicants = appMatch[1] + ' applicants';
            }

            // Extract job description - find content between "About the job" and next section
            const descMatch = bodyText.match(/About the job([\\s\\S]*?)(?:Show less|Set alert for similar jobs|About the company|Similar jobs)/i);
            if (descMatch) {
                description = descMatch[1].trim();
            }

            return {title, company, location, workType, postedAt, applicants, description};
        }"""
        extracted = await page.evaluate(js_code)

        title = extracted.get('title', '')
        company = extracted.get('company', '')
        location = extracted.get('location', '')

        # Fallback to old selectors if JS extraction failed
        if not title:
            title = await extract_text(
                page,
                [
                    "h1.top-card-layout__title",
                    "h1.topcard__title",
                    "h1.job-details-jobs-unified-top-card__job-title",
                    "h1.jobs-unified-top-card__job-title",
                    "h1",
                ],
            )

        if not company:
            company = await extract_text(
                page,
                [
                    "a.topcard__org-name-link",
                    "span.topcard__flavor--black-link",
                    "a.top-card-layout__card-btn",
                    "div.job-details-jobs-unified-top-card__company-name a",
                    "div.job-details-jobs-unified-top-card__company-name",
                    "a.jobs-unified-top-card__company-name",
                    "span.jobs-unified-top-card__company-name",
                ],
            )

        if not location:
            location = await extract_text(
                page,
                [
                    "span.topcard__flavor--bullet",
                    "span.top-card-layout__bullet",
                    "span.job-details-jobs-unified-top-card__bullet",
                    "div.job-details-jobs-unified-top-card__primary-description-container span",
                    "span.jobs-unified-top-card__bullet",
                ],
            )

        # Get additional extracted fields
        work_type = extracted.get('workType', '')
        posted_at = extracted.get('postedAt', '')
        applicants = extracted.get('applicants', '')
        description = extracted.get('description', '')

        # Clean and validate
        raw_title = title.strip()[:500] if title and not is_masked(title) else ""
        raw_location = location.strip()[:500] if location and not is_masked(location) else ""

        # Apply cleaning functions
        cleaned_title = clean_title(raw_title)
        loc_parsed = parse_location_field(raw_location)

        # Use directly extracted values if available, otherwise use parsed values
        details = {
            "title": cleaned_title,
            "company": company.strip()[:500] if company and not is_masked(company) else "",
            "location": loc_parsed['location'] if loc_parsed['location'] else raw_location,
            "work_type": work_type or loc_parsed['work_type'],
            "posted_at": clean_posted_at(posted_at) if posted_at else (clean_posted_at(loc_parsed['posted_at']) if loc_parsed['posted_at'] else None),
            "applicants": applicants or loc_parsed['applicants'],
            "description": description.strip() if description else "",
        }

        logging.info(
            "Extracted for job %s: title=%s, company=%s, location=%s",
            job["job_id"],
            details["title"][:50] if details["title"] else "(empty)",
            details["company"][:30] if details["company"] else "(empty)",
            details["location"][:30] if details["location"] else "(empty)",
        )

        await asyncio.sleep(pause_seconds)
        return details

    except PlaywrightTimeoutError:
        logging.warning("Timeout loading job %s at %s", job["job_id"], posting_url)
        return None
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to fetch details for job %s: %s", job["job_id"], exc)
        return None
    finally:
        await page.close()


def update_job_details(db_path: str, job_id: str, details: Dict[str, str], job: Dict[str, Any]) -> None:
    """Update job details in the database, only updating non-empty fields."""
    updates = []
    values = []

    # Only update fields that are currently empty and we have new data for
    if details.get("title"):
        updates.append("title = ?")
        values.append(details["title"])
    if details.get("company"):
        updates.append("company = ?")
        values.append(details["company"])
    if details.get("location"):
        updates.append("location = ?")
        values.append(details["location"])
    if details.get("work_type") and not job.get("work_type"):
        updates.append("work_type = ?")
        values.append(details["work_type"])
    if details.get("posted_at") and not job.get("posted_at"):
        updates.append("posted_at = ?")
        values.append(details["posted_at"])
    if details.get("applicants") and not job.get("applicants"):
        updates.append("applicants = ?")
        values.append(details["applicants"])
    if details.get("description") and not job.get("description"):
        updates.append("description = ?")
        values.append(details["description"])

    if not updates:
        logging.info("No new details to update for job %s", job_id)
        return

    values.append(job_id)
    query = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?"

    with get_db_connection(db_path) as conn:
        conn.execute(query, values)
        conn.commit()
        logging.info("Updated job %s with %s fields", job_id, len(updates))


async def backfill_missing_details(days_back: int = 2, max_jobs: Optional[int] = None) -> None:
    """Main function to backfill missing job details."""
    setup_logging()
    settings = load_settings()

    cookie_path = Path(settings["cookie_file"])
    if not cookie_path.exists():
        raise FileNotFoundError(f"Cookie file not found: {cookie_path}")

    ensure_storage(settings["db_path"])

    # Fetch jobs with missing details
    jobs_to_backfill = fetch_jobs_with_missing_details(settings["db_path"], days_back)

    if not jobs_to_backfill:
        logging.info("No jobs found with missing details from the last %s days", days_back)
        return

    logging.info(
        "Found %s jobs from last %s days with missing details",
        len(jobs_to_backfill),
        days_back
    )

    if max_jobs:
        jobs_to_backfill = jobs_to_backfill[:max_jobs]
        logging.info("Limited to %s jobs for processing", max_jobs)

    # Load cookies and start browser
    cookies = load_cookies(str(cookie_path))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies(cookies)

        updated_count = 0
        failed_count = 0

        for idx, job in enumerate(jobs_to_backfill, 1):
            logging.info(
                "Processing job %s/%s: %s (current: title=%s, company=%s, location=%s)",
                idx,
                len(jobs_to_backfill),
                job["job_id"],
                "✓" if job.get("title") else "✗",
                "✓" if job.get("company") else "✗",
                "✓" if job.get("location") else "✗",
            )

            details = await fetch_job_details_from_url(
                context,
                job,
                settings["job_pause_seconds"]
            )

            if details:
                # Only update fields that are currently missing
                filtered_details = {}
                if not job.get("title") and details.get("title"):
                    filtered_details["title"] = details["title"]
                if not job.get("company") and details.get("company"):
                    filtered_details["company"] = details["company"]
                if not job.get("location") and details.get("location"):
                    filtered_details["location"] = details["location"]
                if not job.get("work_type") and details.get("work_type"):
                    filtered_details["work_type"] = details["work_type"]
                if not job.get("posted_at") and details.get("posted_at"):
                    filtered_details["posted_at"] = details["posted_at"]
                if not job.get("applicants") and details.get("applicants"):
                    filtered_details["applicants"] = details["applicants"]
                if not job.get("description") and details.get("description"):
                    filtered_details["description"] = details["description"]

                if filtered_details:
                    update_job_details(settings["db_path"], job["job_id"], filtered_details, job)
                    updated_count += 1
                else:
                    logging.info("Job %s: No new details extracted", job["job_id"])
            else:
                failed_count += 1

        await context.close()
        await browser.close()

    logging.info("=== BACKFILL SUMMARY ===")
    logging.info("Jobs processed: %s", len(jobs_to_backfill))
    logging.info("Successfully updated: %s", updated_count)
    logging.info("Failed: %s", failed_count)
    logging.info("========================")


def main() -> None:
    """CLI entry point for backfilling job details."""
    import sys

    days_back = 2
    max_jobs = None

    # Parse simple command line arguments
    if len(sys.argv) > 1:
        try:
            days_back = int(sys.argv[1])
        except ValueError:
            print(f"Usage: {sys.argv[0]} [days_back] [max_jobs]")
            sys.exit(1)

    if len(sys.argv) > 2:
        try:
            max_jobs = int(sys.argv[2])
        except ValueError:
            print(f"Usage: {sys.argv[0]} [days_back] [max_jobs]")
            sys.exit(1)

    asyncio.run(backfill_missing_details(days_back, max_jobs))


if __name__ == "__main__":
    main()
