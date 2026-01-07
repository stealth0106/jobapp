"""Diagnostic script to understand LinkedIn's current state."""
import asyncio
import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


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
    """Run diagnostic on LinkedIn search page."""
    setup_logging()
    load_dotenv()

    cookie_path = os.environ.get("COOKIE_FILE", "secrets/linkedin_cookies.json")
    test_url = "https://www.linkedin.com/jobs/search/?distance=25&f_E=4%2C5%2C6&f_TPR=r304800&geoId=102713980&keywords=director%20vp%20product%20management&sortBy=DD"

    if not Path(cookie_path).exists():
        logging.error("Cookie file not found: %s", cookie_path)
        return

    cookies = load_cookies(cookie_path)
    logging.info("Loaded %s cookies", len(cookies))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Run with UI visible
        context = await browser.new_context()
        await context.add_cookies(cookies)
        page = await context.new_page()
        await stealth_async(page)

        logging.info("Navigating to: %s", test_url)
        await page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(8)  # Wait for page to fully load

        # Take screenshot
        screenshot_path = "data/linkedin_screenshot.png"
        Path(screenshot_path).parent.mkdir(parents=True, exist_ok=True)
        await page.screenshot(path=screenshot_path, full_page=True)
        logging.info("Screenshot saved to: %s", screenshot_path)

        # Check for job cards
        job_cards = await page.query_selector_all("ul.jobs-search__results-list li")
        logging.info("Found %s job cards", len(job_cards))

        # Check for pagination buttons
        pagination_info = await page.evaluate("""
() => {
    const buttons = Array.from(document.querySelectorAll('button, a'));
    const pagination_buttons = buttons.filter(btn =>
        btn.textContent?.includes('Next') ||
        btn.getAttribute('aria-label')?.includes('Next') ||
        btn.getAttribute('aria-label')?.includes('next') ||
        btn.className?.includes('pagination')
    );

    return {
        total_buttons: buttons.length,
        pagination_buttons: pagination_buttons.map(btn => ({
            tag: btn.tagName,
            text: btn.textContent?.trim().substring(0, 50),
            aria_label: btn.getAttribute('aria-label'),
            class: btn.className,
            disabled: btn.disabled || btn.getAttribute('aria-disabled'),
        })),
        pagination_container: !!document.querySelector('.jobs-search-pagination') ||
                            !!document.querySelector('[class*="pagination"]'),
    };
}
        """)

        logging.info("Pagination info:")
        logging.info("  - Total buttons on page: %s", pagination_info['total_buttons'])
        logging.info("  - Pagination container exists: %s", pagination_info['pagination_container'])
        logging.info("  - Pagination buttons found: %s", len(pagination_info['pagination_buttons']))

        for i, btn in enumerate(pagination_info['pagination_buttons'], 1):
            logging.info("  Button %s: %s", i, btn)

        # Check if user is logged in
        login_status = await page.evaluate("""
() => {
    const profileButton = document.querySelector('[data-control-name="identity_profile_photo"]');
    const signInButton = document.querySelector('a[href*="login"], a[href*="sign-in"]');
    return {
        logged_in: !!profileButton,
        sign_in_visible: !!signInButton,
    };
}
        """)

        logging.info("Login status:")
        logging.info("  - Logged in: %s", login_status['logged_in'])
        logging.info("  - Sign in button visible: %s", login_status['sign_in_visible'])

        if not login_status['logged_in']:
            logging.warning("NOT LOGGED IN! Your cookies may have expired.")
        else:
            logging.info("Successfully logged in with cookies")

        logging.info("\nDiagnostic complete. Check %s to see what LinkedIn shows.", screenshot_path)
        logging.info("Press Ctrl+C to close browser...")

        try:
            await asyncio.sleep(30)  # Keep browser open for manual inspection
        except KeyboardInterrupt:
            logging.info("Closing browser...")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(diagnose())
