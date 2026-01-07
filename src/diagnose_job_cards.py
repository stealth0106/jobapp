"""Diagnostic script to inspect LinkedIn job card HTML structure."""
import asyncio
import json
import os
from pathlib import Path

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
    """Inspect job card structure."""
    load_dotenv()

    cookie_path = os.environ.get("COOKIE_FILE", "secrets/linkedin_cookies.json")
    test_url = "https://www.linkedin.com/jobs/search/?distance=25&f_E=4%2C5%2C6&f_TPR=r304800&geoId=102713980&keywords=director%20vp%20product%20management&sortBy=DD"

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

        # Find job cards
        cards = await page.query_selector_all("li[data-occludable-job-id]")
        print(f"\nFound {len(cards)} job cards")

        if cards:
            # Get HTML structure of first 3 job cards
            card_structures = await page.evaluate("""
() => {
    const cards = Array.from(document.querySelectorAll('li[data-occludable-job-id]')).slice(0, 3);
    return cards.map((card, idx) => {
        // Get all links in the card
        const links = Array.from(card.querySelectorAll('a'));

        return {
            card_index: idx,
            card_classes: card.className,
            data_attributes: {
                'data-occludable-job-id': card.getAttribute('data-occludable-job-id'),
                'data-entity-urn': card.getAttribute('data-entity-urn'),
            },
            links: links.map(link => ({
                href: link.getAttribute('href'),
                classes: link.className,
                text: link.textContent.trim().substring(0, 50),
                aria_label: link.getAttribute('aria-label'),
            })),
            // Try to find title element
            title_candidates: [
                {selector: 'a.job-card-list__title', found: !!card.querySelector('a.job-card-list__title'), text: card.querySelector('a.job-card-list__title')?.textContent.trim()},
                {selector: 'h3', found: !!card.querySelector('h3'), text: card.querySelector('h3')?.textContent.trim()},
                {selector: 'a.disabled', found: !!card.querySelector('a.disabled'), text: card.querySelector('a.disabled')?.textContent.trim()},
                {selector: 'span.sr-only', found: !!card.querySelector('span.sr-only'), text: card.querySelector('span.sr-only')?.textContent.trim()},
            ],
        };
    });
}
            """)

            print("\n" + "="*80)
            for card in card_structures:
                print(f"\n--- CARD {card['card_index'] + 1} ---")
                print(f"Classes: {card['card_classes']}")
                print(f"Data attributes: {card['data_attributes']}")

                print(f"\nLinks found ({len(card['links'])}):")
                for i, link in enumerate(card['links'], 1):
                    print(f"  Link {i}:")
                    print(f"    href: {link['href'][:80] if link['href'] else 'None'}")
                    print(f"    classes: {link['classes']}")
                    print(f"    text: {link['text']}")
                    if link['aria_label']:
                        print(f"    aria-label: {link['aria_label']}")

                print(f"\nTitle candidates:")
                for candidate in card['title_candidates']:
                    if candidate['found']:
                        print(f"  ✓ {candidate['selector']}: {candidate['text'][:50] if candidate['text'] else 'empty'}")
                    else:
                        print(f"  ✗ {candidate['selector']}: not found")

                print("\n" + "-"*80)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(diagnose())
