# LinkedIn Scraper

This project automates collecting new LinkedIn job postings and appending them to a Google Sheet. It uses Playwright to reuse your authenticated LinkedIn browser session, stores scraped jobs in SQLite to avoid duplicates, and pushes only unseen listings to Sheets.

⚠️ **Important:** Scraping LinkedIn can violate their Terms of Service. Use this project only if you have permission to automate requests and throttle executions to remain human-like.

## 1. Prerequisites

1. Python 3.9+ (already bundled in the repo).
2. Google Sheet that will receive job rows. Create the sheet and note its exact name.
3. Google Cloud service account with the Sheets API enabled. Download the JSON key and place it at `secrets/service-account.json`. Share the target Google Sheet with the service-account email.
4. LinkedIn session cookies exported to JSON: open LinkedIn in Chrome, log in, open DevTools → Application → Cookies, right-click any cookie row, choose *Export*. Save the JSON array at `secrets/linkedin_cookies.json`. Refresh this file whenever LinkedIn invalidates the session.

## 2. Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

Copy `.env.example` to `.env` and adjust values:

```bash
cp .env.example .env
```

If you set `LINKEDIN_SEARCH_URLS` (newline-separated list of job search URLs copied from LinkedIn), the script will ignore `LINKEDIN_QUERY` and `LINKEDIN_LOCATION` and crawl exactly those pages.

| Variable | Description |
| --- | --- |
| `LINKEDIN_QUERY` | Search keywords (fallback when no custom URLs). |
| `LINKEDIN_SEARCH_URLS` | Optional newline-separated LinkedIn job search URLs (overrides query/location). |
| `LINKEDIN_LOCATION` | Location filter when using the query fallback. |
| `SCROLL_ITERATIONS` | Number of slow scrolls on the results page to load more jobs. |
| `JOB_PAUSE_SECONDS` | Delay between fetching individual job pages to mimic human browsing. |
| `COOKIE_FILE` | Path to exported LinkedIn cookies JSON. |
| `SERVICE_ACCOUNT_FILE` | Path to Google service account credentials. |
| `DATABASE_PATH` | SQLite file used to track already-processed job IDs. |
| `SPREADSHEET_NAME` | Name of the Google Sheet (must match exactly). |

## 3. Running the Scraper

```bash
source .venv/bin/activate
python -m src.scrape_linkedin
```

The script will:
- Load cookies and open LinkedIn search results.
- Scroll and collect job cards. Each new job detail page is opened individually.
- Store job details in `data/jobs.sqlite` for deduplication.
- Append any newly captured jobs to the first worksheet of the Google Sheet.

Logs are printed to stdout and include what was captured or skipped.

## 4. Scheduling with Cron

Create a wrapper script (e.g. `/usr/local/bin/linkedin_jobs.sh`):

```bash
#!/bin/bash
cd /path/to/linkedin-scraper || exit 1
source .venv/bin/activate
python -m src.scrape_linkedin >> logs/cron.log 2>&1
```

Make it executable (`chmod +x`) and ensure the `logs/` directory exists. Add a cron entry such as:

```
*/60 9-18 * * 1-5 /usr/local/bin/linkedin_jobs.sh
```

This runs hourly on weekdays during business hours. Adjust frequency conservatively to avoid LinkedIn rate-limiting.

## 5. Maintenance Tips

- Replace `secrets/linkedin_cookies.json` whenever you log out of LinkedIn or change your password.
- If Google Sheets API calls fail, check that the service account still has access to the sheet.
- Periodically archive or vacuum `data/jobs.sqlite` if it grows large.
- Keep delays (`JOB_PAUSE_SECONDS`) generous to lower the risk of detection.

## 6. Troubleshooting

- **LinkedIn prompts for login**: The cookies likely expired. Export fresh cookies and retry.
- **`gspread` errors about permissions**: Share the sheet with the service account email displayed in the JSON credentials file.
- **Cron runs but nothing is appended**: Inspect `logs/cron.log` to confirm the run had new jobs. If none, the scraper may have found duplicates already stored in SQLite.

