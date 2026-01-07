"""Test which selector actually works for job links."""
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


async def test_selectors():
    """Test different selectors to find job links."""
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

        # Test the actual selector logic from the scraper
        results = await page.evaluate("""
() => {
  const cards = Array.from(document.querySelectorAll('li[data-occludable-job-id]'));
  console.log(`Total cards: ${cards.length}`);

  return cards.map((card, idx) => {
    // Try different selectors
    const link1 = card.querySelector('a.job-card-list__title--link');
    const link2 = card.querySelector('a.disabled');
    const link3 = card.querySelector('a.job-card-container__link');
    const link4 = card.querySelector("a[href*='/jobs/view/']");

    // Use the same logic as the scraper
    const linkEl = link1 || link2 || link3 || link4;
    const rawHref = linkEl ? linkEl.getAttribute('href') : '';
    let href = rawHref || '';
    if (href.startsWith('/')) {
      href = 'https://www.linkedin.com' + href;
    }
    if (href.includes('?')) {
      href = href.split('?')[0];
    }

    return {
      index: idx,
      job_id: card.getAttribute('data-occludable-job-id'),
      link1_found: !!link1,
      link2_found: !!link2,
      link3_found: !!link3,
      link4_found: !!link4,
      final_href: href,
      has_url: !!href,
    };
  });
}
        """)

        total = len(results)
        with_urls = sum(1 for r in results if r['has_url'])
        without_urls = total - with_urls

        print(f"Total cards: {total}")
        print(f"Cards WITH URLs: {with_urls}")
        print(f"Cards WITHOUT URLs: {without_urls}")

        print(f"\nSelector success rates:")
        print(f"  a.job-card-list__title--link: {sum(1 for r in results if r['link1_found'])}/{total}")
        print(f"  a.disabled: {sum(1 for r in results if r['link2_found'])}/{total}")
        print(f"  a.job-card-container__link: {sum(1 for r in results if r['link3_found'])}/{total}")
        print(f"  a[href*='/jobs/view/']: {sum(1 for r in results if r['link4_found'])}/{total}")

        print(f"\nFirst 3 cards (detailed):")
        for result in results[:3]:
            print(f"\nCard {result['index'] + 1}:")
            print(f"  job_id: {result['job_id']}")
            print(f"  link1 (a.job-card-list__title--link): {result['link1_found']}")
            print(f"  link2 (a.disabled): {result['link2_found']}")
            print(f"  link3 (a.job-card-container__link): {result['link3_found']}")
            print(f"  link4 (a[href*='/jobs/view/']): {result['link4_found']}")
            print(f"  Final URL: {result['final_href']}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(test_selectors())
