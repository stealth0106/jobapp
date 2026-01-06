# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands

```bash
# Setup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
playwright install chromium

# Run the main scraper
python -m src.scrape_linkedin

# Push pending jobs to Google Sheets (after editable install)
appendjobs
```

## Project Architecture

This is a LinkedIn job scraper that collects job postings and pushes them to Google Sheets. It consists of two main modules:

**src/scrape_linkedin.py** - Main scraper that:
- Uses Playwright with stealth mode to navigate LinkedIn job search results
- Loads session cookies from `secrets/linkedin_cookies.json` for authentication
- Scrolls through search results and paginates to collect job cards
- Enriches each job by visiting its detail page to extract description, posted date, and applicant count
- Stores jobs in SQLite (`data/jobs.sqlite`) for deduplication
- Appends new jobs to Google Sheets via gspread

**src/append_pending_jobs.py** - Standalone utility (`appendjobs` CLI) that pushes any jobs in SQLite that haven't been appended to Sheets yet. Processes in batches of 25.

## Key Configuration

All settings come from environment variables (loaded via `.env`):
- `LINKEDIN_SEARCH_URLS` - If provided (newline-separated URLs), overrides query/location-based search
- `COOKIE_FILE` - Path to exported LinkedIn cookies JSON
- `SERVICE_ACCOUNT_FILE` - Google service account credentials for Sheets API
- `DATABASE_PATH` - SQLite file for job deduplication
- `JOB_PAUSE_SECONDS` - Delay between fetching individual job pages

## Dependencies

- playwright + playwright-stealth for browser automation
- gspread + google-auth for Google Sheets API
- python-dotenv for configuration
