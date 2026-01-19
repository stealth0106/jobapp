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

# Match score threshold for "Recommended" shortlist status
SHORTLIST_THRESHOLD = 30

SEARCH_URL = "https://www.linkedin.com/jobs/search/?keywords={query}&location={location}&f_TPR=r86400"


# Profile-based matching criteria
PROFILE_CRITERIA = {
    "target_roles": [
        "product manager", "product owner", "product lead", "product director",
        "director of product", "head of product", "vp product", "vp of product",
        "chief product", "cpo", "growth manager", "growth lead", "growth product",
    ],
    "exclude_keywords": [
        "intern", "internship", "fresher", "entry level",
        "associate product manager", "apm", "junior", "jr.", "trainee",
    ],
    "high_seniority": ["director", "head", "vp", "chief", "cpo", "avp"],
    "mid_seniority": ["senior", "sr.", "lead", "principal", "staff"],
    "strong_domains": [
        "healthcare", "healthtech", "health tech", "medical", "telehealth",
        "edtech", "ed-tech", "education", "learning",
        "ecommerce", "e-commerce", "d2c", "dtc", "direct to consumer",
    ],
    "good_domains": ["consumer", "b2b saas", "saas", "marketplace", "retail", "fintech"],
    "ai_keywords": ["ai product", "ml product", "genai", "generative ai", "llm", "machine learning"],
    "growth_keywords": [
        "growth", "plg", "product-led", "experimentation", "a/b test",
        "conversion", "retention", "monetization", "pricing", "freemium",
    ],
}


def calculate_match_score(job: Dict[str, Any]) -> Tuple[int, List[str]]:
    """Calculate a relevance score for the job based on profile fit.

    Returns (score, reasons) where score >= SHORTLIST_THRESHOLD means recommended.
    """
    title = (job.get("title") or "").lower()
    description = (job.get("description") or "").lower()
    company = (job.get("company") or "").lower()
    combined = f"{title} {description} {company}"

    score = 0
    reasons = []

    # Exclusion check
    for exclude in PROFILE_CRITERIA["exclude_keywords"]:
        if exclude in title:
            return -100, [f"Excluded: '{exclude}' in title"]

    # Must be a product role
    is_product_role = any(role in title for role in PROFILE_CRITERIA["target_roles"])
    if not is_product_role:
        return -100, ["Not a product management role"]

    # Seniority scoring
    if any(s in title for s in PROFILE_CRITERIA["high_seniority"]):
        score += 30
        reasons.append("Director/Head level")
    elif any(s in title for s in PROFILE_CRITERIA["mid_seniority"]):
        score += 20
        reasons.append("Senior level")
    else:
        score -= 10
        reasons.append("Entry/mid level")

    # Domain scoring
    strong_matches = [d for d in PROFILE_CRITERIA["strong_domains"] if d in combined]
    good_matches = [d for d in PROFILE_CRITERIA["good_domains"] if d in combined]

    if strong_matches:
        score += 25
        reasons.append(f"Domain: {', '.join(strong_matches[:2])}")
    elif good_matches:
        score += 10
        reasons.append(f"Domain: {', '.join(good_matches[:2])}")

    # AI/ML scoring
    ai_matches = [k for k in PROFILE_CRITERIA["ai_keywords"] if k in combined]
    if ai_matches:
        score += 20
        reasons.append(f"AI/ML: {', '.join(ai_matches[:2])}")

    # Growth scoring
    growth_matches = [k for k in PROFILE_CRITERIA["growth_keywords"] if k in combined]
    if growth_matches:
        score += 15
        reasons.append(f"Growth: {', '.join(growth_matches[:2])}")

    return score, reasons


def sort_jobs_by_match_score(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort jobs by match score (highest first) and add score/shortlist fields."""
    for job in jobs:
        score, reasons = calculate_match_score(job)
        job["match_score"] = score
        job["match_reasons"] = reasons
        job["shortlist"] = "Recommended" if score >= SHORTLIST_THRESHOLD else "Ignore"

    # Sort by score descending
    return sorted(jobs, key=lambda x: x.get("match_score", 0), reverse=True)


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
            ("work_type", "TEXT"),
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
            (job_id, title, company, location, posting_url, description, posted_at, applicants, scraped_at, appended_at, work_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            job.get("work_type"),
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


def clean_title(raw: str) -> str:
    """Remove duplicate text and 'with verification' suffix."""
    if not raw:
        return ""
    lines = raw.strip().split('\n')
    title = lines[0].strip()
    return re.sub(r'\s+with verification$', '', title, flags=re.I)


def clean_posted_at(raw: str) -> str:
    """Extract just the time ago text."""
    if not raw:
        return ""
    return raw.strip().split('\n')[0].strip()


def parse_location_field(raw: str) -> Dict[str, Any]:
    """Parse contaminated location into components.

    Returns dict with keys: location, posted_at, applicants, work_type
    """
    if not raw:
        return {'location': '', 'posted_at': None, 'applicants': None, 'work_type': None}

    lines = raw.split('\n')
    main_line = lines[0].strip()

    # Split by middle dot (·) first
    parts = re.split(r'\s*·\s*', main_line)
    location = parts[0].strip() if parts else ""
    posted_at = None
    applicants = None
    work_type = None

    # Extract work type from location: (Remote), (Hybrid), (On-site)
    work_match = re.search(r'\s*\((Remote|Hybrid|On-site)\)\s*$', location, re.I)
    if work_match:
        work_type = work_match.group(1)
        location = location[:work_match.start()].strip()

    for part in parts[1:]:
        part = part.strip()
        if re.search(r'(\d+\s*(hour|day|week|month|minute)s?\s*ago|reposted)', part, re.I):
            posted_at = part
        elif re.search(r'applicant|people clicked|clicked apply', part, re.I):
            match = re.search(r'(over\s+)?(\d+)', part, re.I)
            if match:
                prefix = "Over " if match.group(1) else ""
                applicants = f"{prefix}{match.group(2)} applicants"

    return {'location': location, 'posted_at': posted_at, 'applicants': applicants, 'work_type': work_type}


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

            # First extract basic info from all cards (without URLs)
            js_code_basic = """
() => {
  const cards = Array.from(document.querySelectorAll('""" + active_selector + """'));
  return cards.map((card, index) => {
    const titleEl = card.querySelector('a.job-card-list__title--link') ||
                    card.querySelector('h3') ||
                    card.querySelector('a');
    // Updated company and location selectors for new LinkedIn layout
    const companyEl = card.querySelector('.artdeco-entity-lockup__subtitle') ||
                      card.querySelector('h4');
    const locationEl = card.querySelector('.artdeco-entity-lockup__caption') ||
                       card.querySelector('.job-search-card__location');
    let jobId = card.getAttribute('data-occludable-job-id') || card.getAttribute('data-entity-urn') || '';
    if (jobId && jobId.includes(':')) {
      const parts = jobId.split(':');
      jobId = parts[parts.length - 1];
    }
    return {
      job_id: jobId,
      title: titleEl ? titleEl.innerText.trim() : '',
      company: companyEl ? companyEl.innerText.trim() : '',
      location: locationEl ? locationEl.innerText.trim() : '',
      card_index: index,
      posted_label: (card.querySelector('time') ? card.querySelector('time').innerText.trim() : ''),
      insights: Array.from(card.querySelectorAll('.job-search-card__insight')).map(el => el.innerText.trim()),
    };
  });
}
            """
            raw_jobs = await page.evaluate(js_code_basic)
            logging.info("Extracted basic info for %s candidate job entries from page %s", len(raw_jobs), page_number)

            # Now capture job URLs - try multiple methods
            logging.info("Capturing job URLs using job_id and click fallback...")
            for job_data in raw_jobs:
                card_index = job_data.get("card_index", 0)
                job_id = job_data.get("job_id", "").strip()

                # Method 1: If we have a job_id, construct the URL directly (fastest and most reliable)
                if job_id and job_id.isdigit():
                    posting_url = f"https://www.linkedin.com/jobs/view/{job_id}"
                    job_data['posting_url'] = posting_url
                    logging.debug("Card %s: Constructed URL from job_id: %s", card_index + 1, posting_url)
                    continue

                # Method 2: Try to extract href from the card's link element
                try:
                    card = cards[card_index]
                    link_element = await card.query_selector('a[href*="/jobs/view/"]')

                    if link_element:
                        href = await link_element.get_attribute('href')
                        if href and '/jobs/view/' in href:
                            # Normalize the URL
                            if href.startswith('/'):
                                href = 'https://www.linkedin.com' + href
                            posting_url = href.split('?')[0]
                            job_data['posting_url'] = posting_url

                            # Extract job_id if not present
                            if not job_id:
                                match = re.search(r'/jobs/view/(\d+)', posting_url)
                                if match:
                                    job_data['job_id'] = match.group(1)

                            logging.debug("Card %s: Extracted URL from href: %s", card_index + 1, posting_url)
                            continue

                    # Method 3: Click the card and capture URL from page navigation or detail panel
                    logging.debug("Card %s: No job_id or href found, trying click method...", card_index + 1)

                    # Scroll the card into view first
                    await card.scroll_into_view_if_needed()
                    await asyncio.sleep(0.3)

                    # Try to find and click the link within the card (not the card itself)
                    link_element = await card.query_selector('a.job-card-list__title--link, a.job-card-container__link, a')

                    if link_element:
                        # Click the link element specifically
                        await link_element.click()
                        await asyncio.sleep(2)  # Wait for URL to update and detail panel to load

                        # Get the current page URL (LinkedIn updates it when you click a job)
                        current_url = page.url

                        # Extract job URL from the current page URL
                        if '/jobs/view/' in current_url:
                            # Clean the URL
                            posting_url = current_url.split('?')[0]
                            job_data['posting_url'] = posting_url

                            # Extract job_id from URL if not already present
                            if not job_data.get('job_id'):
                                match = re.search(r'/jobs/view/(\d+)', posting_url)
                                if match:
                                    job_data['job_id'] = match.group(1)

                            logging.debug("Card %s: Captured URL %s", card_index + 1, posting_url)
                        else:
                            # URL didn't update - try extracting from the detail panel
                            logging.debug("Card %s: URL didn't update (current: %s), trying detail panel extraction", card_index + 1, current_url)

                            # Try to get URL from the job detail panel's share button or apply button
                            detail_panel_url = await page.evaluate("""
                                () => {
                                    // Try to find the job URL in the detail panel
                                    const applyButton = document.querySelector('a[href*="/jobs/view/"]');
                                    if (applyButton) {
                                        const href = applyButton.getAttribute('href');
                                        if (href && href.includes('/jobs/view/')) {
                                            return href.startsWith('http') ? href : 'https://www.linkedin.com' + href;
                                        }
                                    }
                                    // Try alternative: look in the detail panel container
                                    const detailPanel = document.querySelector('.jobs-details, .job-details');
                                    if (detailPanel) {
                                        const link = detailPanel.querySelector('a[href*="/jobs/view/"]');
                                        if (link) {
                                            const href = link.getAttribute('href');
                                            return href.startsWith('http') ? href : 'https://www.linkedin.com' + href;
                                        }
                                    }
                                    return null;
                                }
                            """)

                            if detail_panel_url:
                                posting_url = detail_panel_url.split('?')[0]
                                job_data['posting_url'] = posting_url

                                if not job_data.get('job_id'):
                                    match = re.search(r'/jobs/view/(\d+)', posting_url)
                                    if match:
                                        job_data['job_id'] = match.group(1)

                                logging.debug("Card %s: Captured URL from detail panel %s", card_index + 1, posting_url)
                            else:
                                logging.warning("Card %s: Could not capture URL from URL or detail panel", card_index + 1)
                                job_data['posting_url'] = ''
                    else:
                        logging.warning("Card %s: No clickable link found in card", card_index + 1)
                        job_data['posting_url'] = ''

                except Exception as e:
                    logging.warning("Card %s: Failed to click and capture URL: %s", card_index + 1, e)
                    job_data['posting_url'] = ''

            logging.info("Captured URLs for %s/%s cards on page %s",
                        sum(1 for j in raw_jobs if j.get('posting_url')), len(raw_jobs), page_number)
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

                # Clean extracted data
                raw_title = item.get("title", "")
                raw_location = item.get("location", "")

                cleaned_title = clean_title(raw_title)
                location_parsed = parse_location_field(raw_location)

                jobs.append(
                    {
                        "job_id": job_id or key,
                        "title": cleaned_title[:500],
                        "company": item.get("company", "")[:500],
                        "location": location_parsed['location'][:500] if location_parsed['location'] else "",
                        "posting_url": posting_url,
                        "posted_hint": location_parsed['posted_at'] or item.get("posted_label", "")[:200],
                        "applicants_hint": location_parsed['applicants'] or applicants_hint[:200],
                        "work_type": location_parsed['work_type'],
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
        await asyncio.sleep(8)  # Wait longer for React to render

        # Use JS-based extraction for all fields (LinkedIn uses obfuscated class names)
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

        # Use JS-extracted values, falling back to search results data
        js_title = extracted.get('title', '')
        js_company = extracted.get('company', '')
        js_location = extracted.get('location', '')
        js_work_type = extracted.get('workType', '')
        js_posted_at = extracted.get('postedAt', '')
        js_applicants = extracted.get('applicants', '')
        description = extracted.get('description', '')

        # Update job with JS-extracted data if current data is empty/masked
        if (is_masked(job.get("title", "")) or not job.get("title", "").strip()) and js_title and not is_masked(js_title):
            logging.info("Recovered title for job %s: %s", job["job_id"], js_title[:50])
            job["title"] = js_title[:500]

        if (is_masked(job.get("company", "")) or not job.get("company", "").strip()) and js_company and not is_masked(js_company):
            logging.info("Recovered company for job %s: %s", job["job_id"], js_company[:50])
            job["company"] = js_company[:500]

        if (is_masked(job.get("location", "")) or not job.get("location", "").strip()) and js_location and not is_masked(js_location):
            logging.info("Recovered location for job %s: %s", job["job_id"], js_location[:50])
            job["location"] = js_location[:500]

        # Set work_type if not already set
        if not job.get("work_type") and js_work_type:
            job["work_type"] = js_work_type

        # Use JS-extracted posted_at and applicants
        posted_at = js_posted_at or job.get("posted_hint", "")
        applicants_text = js_applicants or job.get("applicants_hint", "")

        # Fallback to old selectors if JS extraction failed for description
        if not description:
            description_el = await page.query_selector(".jobs-description__content")
            if not description_el:
                description_el = await page.query_selector("div.jobs-box__html-content")
            if not description_el:
                description_el = await page.query_selector("section.description")
            if not description_el:
                description_el = await page.query_selector("div.show-more-less-html__markup")
            if description_el:
                description = (await description_el.inner_text()).strip()

        # Parse applicants count from JS-extracted text
        applicants_count = parse_applicants_count(applicants_text)
        applicants_value: Optional[str] = None
        if applicants_count is not None:
            applicants_value = str(applicants_count)
        elif applicants_text:
            applicants_value = applicants_text

        # Apply cleaning functions to extracted data
        job["title"] = clean_title(job.get("title", ""))
        job["description"] = description
        job["posted_at"] = clean_posted_at(posted_at) or None
        job["applicants"] = applicants_value
        job["applicants_source"] = applicants_text or None

        # Parse location if it looks contaminated (contains middle dots or extra metadata)
        current_location = job.get("location", "")
        if current_location and ('·' in current_location or 'ago' in current_location.lower()):
            loc_parsed = parse_location_field(current_location)
            job["location"] = loc_parsed['location']
            if not job.get("work_type") and loc_parsed['work_type']:
                job["work_type"] = loc_parsed['work_type']
            if not job.get("posted_at") and loc_parsed['posted_at']:
                job["posted_at"] = clean_posted_at(loc_parsed['posted_at'])
            if not applicants_value and loc_parsed['applicants']:
                job["applicants"] = loc_parsed['applicants']

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
    """Prepare job data for Google Sheets, including match score and shortlist status.

    Jobs should be pre-sorted by match score using sort_jobs_by_match_score().
    Columns: job_id, title, company, location, posting_url, posted_at, applicants,
             description, appended_at, match_score, shortlist, work_type
    """
    rows: List[List[str]] = []
    updates: List[Tuple[str, str]] = []
    for job in jobs:
        appended_at = datetime.utcnow().isoformat()
        applicants_value = job.get("applicants") or ""

        # Ensure match score is calculated if not already present
        if "match_score" not in job:
            score, reasons = calculate_match_score(job)
            job["match_score"] = score
            job["shortlist"] = "Recommended" if score >= SHORTLIST_THRESHOLD else "Ignore"

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
            str(job.get("match_score", 0)),
            job.get("shortlist", "Ignore"),
            job.get("work_type", ""),
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
        # Sort jobs by match score (highest first) before appending
        sorted_jobs = sort_jobs_by_match_score(new_jobs)
        recommended_count = sum(1 for j in sorted_jobs if j.get("shortlist") == "Recommended")
        logging.info("Match scoring complete: %s recommended, %s to ignore", recommended_count, len(sorted_jobs) - recommended_count)

        rows, updates = prepare_sheet_rows(sorted_jobs)
        logging.info("Appending %s new jobs to Google Sheets (sorted by match score)", len(rows))
        append_to_sheet(rows, worksheet)
        mark_jobs_appended(settings["db_path"], updates)
    else:
        logging.info("No new jobs found on this run")


if __name__ == "__main__":
    asyncio.run(run_scraper())
