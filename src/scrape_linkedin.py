import asyncio
import json
import logging
import os
import sqlite3
import time
import re
from datetime import datetime
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from playwright.async_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, async_playwright
from playwright_stealth import stealth_async
import gspread
from gspread.exceptions import APIError

SEARCH_URL = "https://www.linkedin.com/jobs/search/?keywords={query}&location={location}&f_TPR=r86400"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )




def parse_search_urls(raw: str) -> List[str]:
    if not raw:
        return []
    cleaned = raw.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
        cleaned = cleaned[1:-1]
    urls: List[str] = []
    buffer = ""
    for line in cleaned.splitlines():
        fragment = line.strip()
        if not fragment or fragment.startswith('#'):
            continue
        if fragment.startswith(('http://', 'https://')):
            if buffer:
                urls.append(buffer)
            buffer = fragment
        else:
            if buffer:
                buffer += fragment
            else:
                logging.warning("Ignoring orphaned fragment in LINKEDIN_SEARCH_URLS: %s", fragment)
    if buffer:
        urls.append(buffer)
    return [url for url in urls if url.startswith(('http://', 'https://'))]



def load_settings() -> Dict[str, Any]:
    load_dotenv()
    settings = {
        "query": os.environ.get("LINKEDIN_QUERY", "software engineer"),
        "location": os.environ.get("LINKEDIN_LOCATION", "Remote"),
        "max_scroll_rounds": max(0, int(os.environ.get("SCROLL_ITERATIONS", "0"))),
        "job_pause_seconds": float(os.environ.get("JOB_PAUSE_SECONDS", "5")),
        "cookie_file": os.environ.get("COOKIE_FILE", "secrets/linkedin_cookies.json"),
        "service_account_file": os.environ.get("SERVICE_ACCOUNT_FILE", "secrets/service-account.json"),
        "spreadsheet_name": os.environ.get("SPREADSHEET_NAME", "LinkedIn Jobs"),
        "db_path": os.environ.get("DATABASE_PATH", "data/jobs.sqlite"),
        "search_urls": parse_search_urls(os.environ.get("LINKEDIN_SEARCH_URLS", "")),
    }
    return settings


def ensure_storage(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                title TEXT,
                company TEXT,
                location TEXT,
                posting_url TEXT,
                description TEXT,
                posted_at TEXT,
                applicants TEXT,
                scraped_at INTEGER,
                appended_at TEXT
            );
            """
        )
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        migrations = [
            ("posted_at", "TEXT"),
            ("applicants", "TEXT"),
            ("scraped_at", "INTEGER"),
            ("appended_at", "TEXT"),
        ]
        for name, col_type in migrations:
            if name not in columns:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {col_type}")
        conn.commit()


def get_db_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def already_seen(conn: sqlite3.Connection, job_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    return row is not None


def persist(conn: sqlite3.Connection, job: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO jobs
            (job_id, title, company, location, posting_url, description, posted_at, applicants, scraped_at, appended_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job["job_id"],
            job.get("title", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("posting_url", ""),
            job.get("description", ""),
            job.get("posted_at"),
            job.get("applicants"),
            int(time.time()),
            job.get("appended_at"),
        ),
    )


def load_cookies(cookie_path: str) -> List[Dict[str, Any]]:
    def normalize_same_site(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value_lower = value.lower()
        mapping = {
            "lax": "Lax",
            "strict": "Strict",
            "none": "None",
            "no_restriction": "None",
            "unspecified": None,
        }
        return mapping.get(value_lower, None)

    with open(cookie_path, "r", encoding="utf-8") as fh:
        raw_cookies = json.load(fh)
    if not isinstance(raw_cookies, list):
        raise ValueError("Cookie file must contain a JSON array of cookie dictionaries")

    normalized: List[Dict[str, Any]] = []
    for entry in raw_cookies:
        name = entry.get("name")
        value = entry.get("value")
        domain = entry.get("domain")
        path_value = entry.get("path") or "/"
        if not all([name, value, domain]):
            continue
        cookie: Dict[str, Any] = {
            "name": name,
            "value": value,
            "domain": domain,
            "path": path_value,
            "httpOnly": bool(entry.get("httpOnly")),
            "secure": bool(entry.get("secure")),
        }
        expires = entry.get("expirationDate")
        if expires:
            cookie["expires"] = float(expires)
        same_site = normalize_same_site(entry.get("sameSite"))
        if same_site:
            cookie["sameSite"] = same_site
        normalized.append(cookie)
    if not normalized:
        raise ValueError("No valid cookies were found in the cookie file")
    return normalized


def authorise_sheet(service_account_file: str, sheet_name: str) -> gspread.Worksheet:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        service_account_file,
        scopes=scopes,
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open(sheet_name)
    return spreadsheet.sheet1

def is_masked(value: str) -> bool:
    """Check if a field value is masked by LinkedIn (contains ***)."""
    if not value:
        return False
    return "***" in value


def parse_applicants_count(raw: str) -> Optional[int]:
    if not raw:
        return None
    digits = re.findall(r"(\d+)", raw.replace(',', ''))
    if digits:
        try:
            return int(digits[-1])
        except ValueError:
            return None
    return None


async def extract_text(page: Page, selectors: List[str]) -> str:
    for selector in selectors:
        try:
            element = await page.query_selector(selector)
        except PlaywrightTimeoutError:
            continue
        if element:
            try:
                value = (await element.inner_text()).strip()
            except Exception:  # noqa: BLE001
                continue
            if value:
                return value
    return ""


async def scroll_results_to_end(page: Page, max_scroll_rounds: int) -> None:
    # Try multiple selectors - LinkedIn changes their HTML frequently
    selector = "ul.jobs-search__results-list li"
    cards = await page.query_selector_all(selector)
    if len(cards) == 0:
        selector = "li[data-occludable-job-id]"
        cards = await page.query_selector_all(selector)
    if len(cards) == 0:
        selector = "li.jobs-search-results__list-item"
        cards = await page.query_selector_all(selector)

    last_count = len(cards)
    logging.info("Using selector: %s (found %s cards initially)", selector, last_count)
    idle_rounds = 0
    scroll_round = 0

    logging.info("Starting scroll with %s job cards visible", last_count)

    while True:
        scroll_round += 1
        logging.info("Scroll round %s: %s job cards currently loaded", scroll_round, last_count)

        # Scroll the job results container specifically (not the main page)
        scroll_result = await page.evaluate(
            """
() => {
  // Find the scrollable job results container
  const container = document.querySelector('.jobs-search-results-list')
                 || document.querySelector('.jobs-search__results-list')
                 || document.querySelector('div.scaffold-layout__list');

  if (container) {
    container.scrollTop = container.scrollHeight;
    return {scrolled: true, container: 'found', scrollTop: container.scrollTop, scrollHeight: container.scrollHeight};
  }

  // Fallback to main page scroll
  window.scrollTo(0, document.body.scrollHeight);
  return {scrolled: true, container: 'fallback', scrollTop: window.scrollY, scrollHeight: document.body.scrollHeight};
}
            """
        )
        logging.debug("Scroll result: %s", scroll_result)

        await asyncio.sleep(3 + min(scroll_round, 6) * 0.5)

        cards = await page.query_selector_all(selector)
        count = len(cards)

        if count > last_count:
            logging.info("Found %s new cards (total now: %s)", count - last_count, count)
            last_count = count
            idle_rounds = 0
        else:
            idle_rounds += 1
            logging.info("No new cards loaded (idle round %s/3)", idle_rounds)

        # Check if the job results container is at the bottom
        at_bottom = await page.evaluate(
            """
() => {
  const container = document.querySelector('.jobs-search-results-list')
                 || document.querySelector('.jobs-search__results-list')
                 || document.querySelector('div.scaffold-layout__list');

  if (container) {
    return (container.scrollTop + container.clientHeight) >= (container.scrollHeight - 10);
  }

  // Fallback to main page
  const scroller = document.scrollingElement || document.body;
  return (scroller.scrollTop + window.innerHeight) >= (scroller.scrollHeight - 10);
}
            """
        )

        if at_bottom:
            logging.info("Reached bottom of results container")

        if (at_bottom and idle_rounds >= 2) or idle_rounds >= 4:
            logging.info("Stopping scroll: at_bottom=%s, idle_rounds=%s", at_bottom, idle_rounds)
            break
        if max_scroll_rounds and scroll_round >= max_scroll_rounds:
            logging.info("Stopping scroll: reached max_scroll_rounds=%s", max_scroll_rounds)
            break



async def fetch_search_results(page: Page, query: str, location: str, max_scroll_rounds: int, search_urls: List[str]) -> List[Dict[str, str]]:
    from urllib.parse import quote_plus

    targets: List[str] = []
    # Check if URLs have manual pagination (start= parameter)
    using_manual_pagination = False
    if search_urls:
        targets = search_urls
        # Check if any URL has manual pagination parameter
        using_manual_pagination = any('start=' in url for url in search_urls)
    else:
        encoded_query = quote_plus(query)
        encoded_location = quote_plus(location)
        targets = [SEARCH_URL.format(query=encoded_query, location=encoded_location)]

    jobs: List[Dict[str, str]] = []
    seen_keys: Set[str] = set()

    for index, url in enumerate(targets, start=1):
        if not url or not url.startswith("http"):
            logging.warning("Skipping invalid search URL: %s", url)
            continue
        logging.info("Opening search results (%s/%s): %s", index, len(targets), url)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except PlaywrightTimeoutError:
            logging.warning("Timed out loading %s", url)
            continue
        await asyncio.sleep(5)

        page_number = 1
        while True:
            try:
                await page.wait_for_selector("ul.jobs-search__results-list li", timeout=20000)
            except PlaywrightTimeoutError:
                logging.warning("No job cards detected on %s (page %s)", url, page_number)
            logging.info(
                "Scrolling results list to the end on page %s (max rounds=%s)",
                page_number,
                max_scroll_rounds or "∞",
            )
            await scroll_results_to_end(page, max_scroll_rounds)

            # Try multiple selectors - LinkedIn changes their HTML
            cards = await page.query_selector_all("ul.jobs-search__results-list li")
            active_selector = "ul.jobs-search__results-list li"

            if len(cards) == 0:
                cards = await page.query_selector_all("li[data-occludable-job-id]")
                active_selector = "li[data-occludable-job-id]"
                logging.info("Fallback to selector: li[data-occludable-job-id]")

            if len(cards) == 0:
                cards = await page.query_selector_all("li.jobs-search-results__list-item")
                active_selector = "li.jobs-search-results__list-item"
                logging.info("Fallback to selector: li.jobs-search-results__list-item")

            logging.info("Found %s job cards on page %s after scrolling (using %s)", len(cards), page_number, active_selector)

            js_code = """
() => {
  const cards = Array.from(document.querySelectorAll('""" + active_selector + """'));
  return cards.map((card) => {
    // Updated selectors based on LinkedIn's current HTML structure (2026)
    const linkEl = card.querySelector('a.job-card-list__title--link') ||
                   card.querySelector('a.disabled') ||
                   card.querySelector('a.job-card-container__link') ||
                   card.querySelector("a[href*='/jobs/view/']");
    const titleEl = linkEl || card.querySelector('h3');
    // Updated company and location selectors for new LinkedIn layout
    const companyEl = card.querySelector('.artdeco-entity-lockup__subtitle') ||
                      card.querySelector('h4');
    const locationEl = card.querySelector('.artdeco-entity-lockup__caption') ||
                       card.querySelector('.job-search-card__location');
    const rawHref = linkEl ? linkEl.getAttribute('href') : '';
    let href = rawHref || '';
    if (href.startsWith('/')) {
      href = 'https://www.linkedin.com' + href;
    }
    if (href.includes('?')) {
      href = href.split('?')[0];
    }
    let jobId = card.getAttribute('data-occludable-job-id') || card.getAttribute('data-entity-urn') || '';
    if (jobId && jobId.includes(':')) {
      const parts = jobId.split(':');
      jobId = parts[parts.length - 1];
    }
    if (!jobId && href) {
      const match = href.match(/\/jobs\/view\/(\d+)/);
      if (match) {
        jobId = match[1];
      }
    }
    return {
      job_id: jobId,
      title: titleEl ? titleEl.innerText.trim() : '',
      company: companyEl ? companyEl.innerText.trim() : '',
      location: locationEl ? locationEl.innerText.trim() : '',
      posting_url: href,
      posted_label: (card.querySelector('time') ? card.querySelector('time').innerText.trim() : ''),
      insights: Array.from(card.querySelectorAll('.job-search-card__insight')).map(el => el.innerText.trim()),
    };
  });
}
            """
            raw_jobs = await page.evaluate(js_code)
            logging.info("Extracted %s candidate job entries from page %s", len(raw_jobs), page_number)
            if raw_jobs:
                logging.debug("Sample raw job: %s", raw_jobs[0])

            jobs_without_ids = 0
            jobs_without_urls = 0
            duplicates_in_page = 0

            for item in raw_jobs:
                job_id = (item.get("job_id") or "").strip()
                posting_url = (item.get("posting_url") or "").strip()
                if not job_id and posting_url:
                    digits = re.findall(r"(\d+)", posting_url)
                    if digits:
                        job_id = digits[-1]
                if not job_id and posting_url:
                    key = posting_url
                else:
                    key = job_id or posting_url
                if not job_id:
                    jobs_without_ids += 1
                if not posting_url:
                    jobs_without_urls += 1
                if not key or not posting_url:
                    continue
                if key in seen_keys:
                    duplicates_in_page += 1
                    continue
                seen_keys.add(key)
                insights = item.get("insights") or []
                applicants_hint = ""
                for insight in insights:
                    if "applicant" in insight.lower():
                        applicants_hint = insight
                        break
                jobs.append(
                    {
                        "job_id": job_id or key,
                        "title": item.get("title", "")[:500],
                        "company": item.get("company", "")[:500],
                        "location": item.get("location", "")[:500],
                        "posting_url": posting_url,
                        "posted_hint": item.get("posted_label", "")[:200],
                        "applicants_hint": applicants_hint[:200],
                    }
                )

            logging.info("Page %s stats - Cards: %s, Extracted: %s, No ID: %s, No URL: %s, Duplicates: %s, Added: %s",
                        page_number, len(cards), len(raw_jobs), jobs_without_ids, jobs_without_urls,
                        duplicates_in_page, len(raw_jobs) - jobs_without_urls - duplicates_in_page)

            # attempt pagination (skip if using manual pagination parameters)
            if using_manual_pagination:
                logging.info("Skipping automatic pagination (manual pagination URLs with start= parameter)")
                break

            logging.info("Checking for Next button to paginate...")
            next_button = await page.query_selector("button[aria-label='Next']")
            if not next_button:
                next_button = await page.query_selector("a[aria-label='Next']")
            if not next_button:
                # Try alternative selectors
                next_button = await page.query_selector("button[aria-label='View next page']")
            if not next_button:
                next_button = await page.query_selector("button.jobs-search-pagination__button--next")

            if not next_button:
                logging.info("No Next button found - this appears to be the last page")
                break

            aria_disabled = await next_button.get_attribute("aria-disabled")
            disabled_attr = await next_button.get_attribute("disabled")
            logging.info("Next button found: aria-disabled=%s, disabled=%s", aria_disabled, disabled_attr)

            if (aria_disabled and aria_disabled.lower() == "true") or disabled_attr is not None:
                logging.info("Next button is disabled - no more pages available")
                break
            page_number += 1
            logging.info("Moving to page %s for %s", page_number, url)
            try:
                await next_button.click()
            except Exception as click_exc:  # noqa: BLE001
                logging.warning("Failed to click next on %s page %s: %s", url, page_number - 1, click_exc)
                break
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=60000)
            except PlaywrightTimeoutError:
                logging.warning("Timed out waiting for next page on %s page %s", url, page_number)
                break
            await asyncio.sleep(4)

    logging.info("Collected %s jobs across all searches (%s with URLs)", len(jobs), sum(1 for job in jobs if job.get("posting_url")))

    return jobs


async def enrich_job(context: BrowserContext, job: Dict[str, Any], conn: sqlite3.Connection, pause_seconds: float) -> Optional[Dict[str, Any]]:
    if already_seen(conn, job["job_id"]):
        logging.info("Skipping already-seen job %s (%s at %s)", job["job_id"], job.get("title", "")[:50], job.get("company", "")[:30])
        return None
    page = await context.new_page()
    try:
        await stealth_async(page)
        await page.goto(job["posting_url"], wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)

        # Extract title from detail page if masked in search results
        if is_masked(job.get("title", "")):
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
                logging.info("Recovered masked title for job %s: %s", job["job_id"], detail_title[:50])
                job["title"] = detail_title[:500]

        # Extract company from detail page if masked
        if is_masked(job.get("company", "")):
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
                logging.info("Recovered masked company for job %s: %s", job["job_id"], detail_company[:50])
                job["company"] = detail_company[:500]

        # Extract location from detail page if masked
        if is_masked(job.get("location", "")):
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
                logging.info("Recovered masked location for job %s: %s", job["job_id"], detail_location[:50])
                job["location"] = detail_location[:500]

        # Updated description selectors for new LinkedIn layout
        description_el = await page.query_selector(".jobs-description__content")
        if not description_el:
            description_el = await page.query_selector("div.jobs-box__html-content")
        if not description_el:
            description_el = await page.query_selector("section.description")
        if not description_el:
            description_el = await page.query_selector("div.show-more-less-html__markup")
        description = ""
        if description_el:
            description = (await description_el.inner_text()).strip()
        posted_at = job.get("posted_hint", "")
        detail_posted = await extract_text(
            page,
            [
                "span.jobs-unified-top-card__posted-date",
                "span.job-details-jobs-unified-top-card__posted-date",
                "span.jobs-unified-top-card__bullet",
            ],
        )
        if detail_posted:
            posted_at = detail_posted
        applicants_text = job.get("applicants_hint", "")
        detail_applicants = await extract_text(
            page,
            [
                "span.jobs-unified-top-card__applicant-count",
                "span.jobs-unified-top-card__subtitle-secondary-grouping li:nth-of-type(2)",
                "span.jobs-unified-top-card__bullet",
            ],
        )
        if detail_applicants:
            applicants_text = detail_applicants
        applicants_count = parse_applicants_count(applicants_text)
        applicants_value: Optional[str] = None
        if applicants_count is not None:
            applicants_value = str(applicants_count)
        elif applicants_text:
            applicants_value = applicants_text
        job["description"] = description
        job["posted_at"] = posted_at or None
        job["applicants"] = applicants_value
        job["applicants_source"] = applicants_text or None
        logging.info("Job %s posted label=%s applicants_label=%s parsed=%s", job["job_id"], posted_at, applicants_text, applicants_value)
        persist(conn, job)
        conn.commit()
        logging.info("Captured job %s", job["job_id"])
        await asyncio.sleep(pause_seconds)
        return job
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to scrape job %s: %s", job.get("posting_url"), exc)
        return None
    finally:
        await page.close()


def prepare_sheet_rows(jobs: List[Dict[str, Any]]) -> Tuple[List[List[str]], List[Tuple[str, str]]]:
    rows: List[List[str]] = []
    updates: List[Tuple[str, str]] = []
    for job in jobs:
        appended_at = datetime.utcnow().isoformat()
        applicants_value = job.get("applicants") or ""
        rows.append([
            job.get("job_id", ""),
            job.get("title", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("posting_url", ""),
            job.get("posted_at", ""),
            applicants_value,
            job.get("description", ""),
            appended_at,
        ])
        job["appended_at"] = appended_at
        updates.append((appended_at, job.get("job_id", "")))
    return rows, updates



def mark_jobs_appended(db_path: str, updates: List[Tuple[str, str]]) -> None:
    cleaned = [(ts, job_id) for ts, job_id in updates if job_id]
    if not cleaned:
        return
    with get_db_connection(db_path) as conn:
        conn.executemany("UPDATE jobs SET appended_at = ? WHERE job_id = ?", cleaned)
        conn.commit()



def _is_retryable_api_error(exc: APIError) -> bool:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    try:
        code_int = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        code_int = None
    if code_int is not None:
        if code_int >= 500 or code_int == 429:
            return True
    if response is not None:
        text = getattr(response, "text", "") or ""
        if "Error 502" in text or "Please try again in 30 seconds" in text:
            return True
    return False


def append_to_sheet(rows: List[List[str]], worksheet: gspread.Worksheet, max_attempts: int = 5, base_backoff_seconds: float = 5.0) -> None:
    attempt = 0
    while True:
        attempt += 1
        try:
            worksheet.append_rows(rows, value_input_option="RAW")
            return
        except APIError as exc:
            if attempt >= max_attempts or not _is_retryable_api_error(exc):
                logging.exception("Failed to append rows to Google Sheets on attempt %s/%s", attempt, max_attempts)
                raise
            sleep_for = base_backoff_seconds * attempt
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            logging.warning("Google Sheets append failed with status %s (attempt %s/%s); retrying in %.1f seconds", status_code or "?", attempt, max_attempts, sleep_for)
            time.sleep(sleep_for)


async def run_scraper() -> None:
    setup_logging()
    settings = load_settings()

    cookie_path = Path(settings["cookie_file"])
    service_account_path = Path(settings["service_account_file"])
    if not cookie_path.exists():
        raise FileNotFoundError(f"Cookie file not found: {cookie_path}")
    if not service_account_path.exists():
        raise FileNotFoundError(f"Service account file not found: {service_account_path}")

    ensure_storage(settings["db_path"])
    worksheet = authorise_sheet(str(service_account_path), settings["spreadsheet_name"])
    cookies = load_cookies(str(cookie_path))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        await stealth_async(page)

        jobs = await fetch_search_results(
            page,
            query=settings["query"],
            location=settings["location"],
            max_scroll_rounds=settings["max_scroll_rounds"],
            search_urls=settings["search_urls"],
        )
        await page.close()

        new_jobs: List[Dict[str, Any]] = []
        skipped_count = 0
        with get_db_connection(settings["db_path"]) as conn:
            for job in jobs:
                if not job.get("posting_url"):
                    continue
                enriched = await enrich_job(context, job, conn, settings["job_pause_seconds"])
                if enriched:
                    new_jobs.append(enriched)
                else:
                    skipped_count += 1

        logging.info("=== SCRAPE SUMMARY ===")
        logging.info("Total jobs found on LinkedIn: %s", len(jobs))
        logging.info("Jobs skipped (already in database): %s", skipped_count)
        logging.info("New jobs to append to sheet: %s", len(new_jobs))
        logging.info("======================")


        await context.close()
        await browser.close()

    if new_jobs:
        rows, updates = prepare_sheet_rows(new_jobs)
        logging.info("Appending %s new jobs to Google Sheets", len(rows))
        append_to_sheet(rows, worksheet)
        mark_jobs_appended(settings["db_path"], updates)
    else:
        logging.info("No new jobs found on this run")


if __name__ == "__main__":
    asyncio.run(run_scraper())
