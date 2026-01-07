"""Diagnostic script to inspect job detail page HTML structure."""
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


async def diagnose():
    """Inspect job detail page structure."""
    load_dotenv()

    cookie_path = os.environ.get("COOKIE_FILE", "secrets/linkedin_cookies.json")
    # Use a known job URL
    test_url = "https://www.linkedin.com/jobs/view/4351952072/"

    cookies = load_cookies(cookie_path)
    print(f"Loaded {len(cookies)} cookies")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        await stealth_async(page)

        print(f"Navigating to: {test_url}")
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(8)

        # Test all the selectors used in enrich_job
        selectors_test = await page.evaluate("""
() => {
    // Title selectors
    const titleSelectors = [
        'h1.top-card-layout__title',
        'h1.topcard__title',
        'h1.job-details-jobs-unified-top-card__job-title',
        'h1',
    ];

    // Company selectors
    const companySelectors = [
        'a.topcard__org-name-link',
        'span.topcard__flavor--black-link',
        'a.top-card-layout__card-btn',
        'div.job-details-jobs-unified-top-card__company-name a',
        'div.job-details-jobs-unified-top-card__company-name',
    ];

    // Location selectors
    const locationSelectors = [
        'span.topcard__flavor--bullet',
        'span.top-card-layout__bullet',
        'span.job-details-jobs-unified-top-card__bullet',
        'div.job-details-jobs-unified-top-card__primary-description-container span',
    ];

    // Description selectors
    const descriptionSelectors = [
        'section.description',
        'div.show-more-less-html__markup',
    ];

    const testSelector = (selectors, name) => {
        for (const selector of selectors) {
            const el = document.querySelector(selector);
            if (el) {
                return {
                    selector,
                    found: true,
                    text: el.textContent.trim().substring(0, 100),
                };
            }
        }
        return {found: false};
    };

    return {
        title: testSelector(titleSelectors, 'title'),
        company: testSelector(companySelectors, 'company'),
        location: testSelector(locationSelectors, 'location'),
        description: testSelector(descriptionSelectors, 'description'),
        // Also check what elements ARE present
        h1_tags: Array.from(document.querySelectorAll('h1')).map(h => ({
            class: h.className,
            text: h.textContent.trim().substring(0, 50),
        })),
        page_classes: document.body.className,
    };
}
        """)

        print("\n" + "="*80)
        print("TITLE:")
        if selectors_test['title']['found']:
            print(f"  ✓ Found with: {selectors_test['title']['selector']}")
            print(f"  Text: {selectors_test['title']['text']}")
        else:
            print("  ✗ NOT FOUND with any selector")
            print(f"  Available h1 tags: {len(selectors_test['h1_tags'])}")
            for h1 in selectors_test['h1_tags']:
                print(f"    - class: {h1['class']}")
                print(f"      text: {h1['text']}")

        print("\nCOMPANY:")
        if selectors_test['company']['found']:
            print(f"  ✓ Found with: {selectors_test['company']['selector']}")
            print(f"  Text: {selectors_test['company']['text']}")
        else:
            print("  ✗ NOT FOUND with any selector")

        print("\nLOCATION:")
        if selectors_test['location']['found']:
            print(f"  ✓ Found with: {selectors_test['location']['selector']}")
            print(f"  Text: {selectors_test['location']['text']}")
        else:
            print("  ✗ NOT FOUND with any selector")

        print("\nDESCRIPTION:")
        if selectors_test['description']['found']:
            print(f"  ✓ Found with: {selectors_test['description']['selector']}")
            print(f"  Text: {selectors_test['description']['text']}...")
        else:
            print("  ✗ NOT FOUND with any selector")

        print(f"\nPage body classes: {selectors_test['page_classes']}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(diagnose())
