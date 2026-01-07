"""Test what data is actually extracted from job cards."""
import asyncio
import json
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async


def load_cookies(cookie_path: str):
    """Load cookies from file."""
    def normalize_same_site(value):
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

    normalized = []
    for entry in raw_cookies:
        name = entry.get("name")
        value = entry.get("value")
        domain = entry.get("domain")
        if not all([name, value, domain]):
            continue

        cookie = {
            "name": name,
            "value": value,
            "domain": domain,
            "path": entry.get("path") or "/",
            "httpOnly": bool(entry.get("httpOnly")),
            "secure": bool(entry.get("secure")),
        }

        same_site = normalize_same_site(entry.get("sameSite"))
        if same_site:
            cookie["sameSite"] = same_site

        expires = entry.get("expirationDate")
        if expires:
            cookie["expires"] = float(expires)

        normalized.append(cookie)

    return normalized


async def test():
    """Test the actual card extraction logic."""
    load_dotenv()

    cookie_path = os.environ.get("COOKIE_FILE", "secrets/linkedin_cookies.json")
    test_url = "https://www.linkedin.com/jobs/search/?distance=25&f_E=4%2C5%2C6&f_TPR=r304800&geoId=102713980&keywords=director%20vp%20product%20management&sortBy=DD"

    cookies = load_cookies(cookie_path)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        await stealth_async(page)

        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(8)

        # Use the EXACT same JavaScript code from the scraper
        active_selector = "li[data-occludable-job-id]"
        js_code = """
() => {
  const cards = Array.from(document.querySelectorAll('""" + active_selector + """'));
  return cards.map((card) => {
    // Updated selectors based on LinkedIn's current HTML structure
    const linkEl = card.querySelector('a.job-card-list__title--link') ||
                   card.querySelector('a.disabled') ||
                   card.querySelector('a.job-card-container__link') ||
                   card.querySelector("a[href*='/jobs/view/']");
    const titleEl = linkEl || card.querySelector('h3');
    const companyEl = card.querySelector('h4');
    const locationEl = card.querySelector('.job-search-card__location');
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

        print(f"Total jobs extracted: {len(raw_jobs)}")
        print(f"\nFirst 3 jobs:")
        for i, job in enumerate(raw_jobs[:3], 1):
            print(f"\n--- Job {i} ---")
            print(f"job_id: {job['job_id']}")
            print(f"title: '{job['title']}'")
            print(f"company: '{job['company']}'")
            print(f"location: '{job['location']}'")
            print(f"posting_url: {job['posting_url']}")
            print(f"posted_label: {job['posted_label']}")

        # Count empty fields
        empty_titles = sum(1 for j in raw_jobs if not j['title'])
        empty_companies = sum(1 for j in raw_jobs if not j['company'])
        empty_locations = sum(1 for j in raw_jobs if not j['location'])
        empty_urls = sum(1 for j in raw_jobs if not j['posting_url'])

        print(f"\n\nEmpty field statistics (out of {len(raw_jobs)} jobs):")
        print(f"  Empty titles: {empty_titles}")
        print(f"  Empty companies: {empty_companies}")
        print(f"  Empty locations: {empty_locations}")
        print(f"  Empty URLs: {empty_urls}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(test())
