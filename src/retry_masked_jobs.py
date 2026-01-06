"""Retry scraping jobs that have masked fields (***) in title/company/location."""
import asyncio
import logging
from typing import Any, Dict, List

from playwright.async_api import async_playwright
from playwright_stealth import stealth_async

from .scrape_linkedin import (
    ensure_storage,
    extract_text,
    get_db_connection,
    is_masked,
    load_cookies,
    load_settings,
    setup_logging,
)


def fetch_masked_jobs(db_path: str) -> List[Dict[str, Any]]:
    """Fetch jobs that have masked fields."""
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT job_id, title, company, location, posting_url
            FROM jobs
            WHERE title LIKE '%***%' OR company LIKE '%***%' OR location LIKE '%***%'
            """
        ).fetchall()
        return [dict(row) for row in rows]


def update_job(db_path: str, job_id: str, title: str, company: str, location: str) -> None:
    """Update job fields in database."""
    with get_db_connection(db_path) as conn:
        conn.execute(
            "UPDATE jobs SET title = ?, company = ?, location = ? WHERE job_id = ?",
            (title, company, location, job_id),
        )
        conn.commit()


async def scrape_job_details(page, job: Dict[str, Any]) -> Dict[str, str]:
    """Extract title, company, location from job detail page."""
    result = {
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
    }

    try:
        await page.goto(job["posting_url"], wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)

        # Extract title
        if is_masked(result["title"]):
            detail_title = await extract_text(
                page,
                [
                    "h1.top-card-layout__title",
                    "h1.topcard__title",
                    "h1.job-details-jobs-unified-top-card__job-title",
                    "h1",
                ],
            )
            if detail_title and not is_masked(detail_title):
                result["title"] = detail_title[:500]

        # Extract company
        if is_masked(result["company"]):
            detail_company = await extract_text(
                page,
                [
                    "a.topcard__org-name-link",
                    "span.topcard__flavor--black-link",
                    "a.top-card-layout__card-btn",
                    "div.job-details-jobs-unified-top-card__company-name a",
                    "div.job-details-jobs-unified-top-card__company-name",
                ],
            )
            if detail_company and not is_masked(detail_company):
                result["company"] = detail_company[:500]

        # Extract location
        if is_masked(result["location"]):
            detail_location = await extract_text(
                page,
                [
                    "span.topcard__flavor--bullet",
                    "span.top-card-layout__bullet",
                    "span.job-details-jobs-unified-top-card__bullet",
                    "div.job-details-jobs-unified-top-card__primary-description-container span",
                ],
            )
            if detail_location and not is_masked(detail_location):
                result["location"] = detail_location[:500]

    except Exception as exc:
        logging.warning("Failed to scrape %s: %s", job["posting_url"], exc)

    return result


async def retry_masked_jobs() -> None:
    setup_logging()
    settings = load_settings()
    ensure_storage(settings["db_path"])

    masked_jobs = fetch_masked_jobs(settings["db_path"])
    if not masked_jobs:
        logging.info("No masked jobs found")
        return

    logging.info("Found %s masked jobs to retry", len(masked_jobs))

    cookies = load_cookies(settings["cookie_file"])

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        await stealth_async(page)

        fixed = 0
        for i, job in enumerate(masked_jobs, 1):
            logging.info("[%s/%s] Retrying job %s", i, len(masked_jobs), job["job_id"])

            result = await scrape_job_details(page, job)

            # Check if we recovered any fields
            recovered = []
            if is_masked(job["title"]) and not is_masked(result["title"]):
                recovered.append(f"title={result['title'][:30]}")
            if is_masked(job["company"]) and not is_masked(result["company"]):
                recovered.append(f"company={result['company'][:30]}")
            if is_masked(job["location"]) and not is_masked(result["location"]):
                recovered.append(f"location={result['location'][:30]}")

            if recovered:
                update_job(
                    settings["db_path"],
                    job["job_id"],
                    result["title"],
                    result["company"],
                    result["location"],
                )
                logging.info("  Fixed: %s", ", ".join(recovered))
                fixed += 1
            else:
                logging.warning("  Could not recover masked fields")

            await asyncio.sleep(settings["job_pause_seconds"])

        await context.close()
        await browser.close()

    logging.info("Done. Fixed %s/%s jobs", fixed, len(masked_jobs))


if __name__ == "__main__":
    asyncio.run(retry_masked_jobs())
