"""Utility for appending pending LinkedIn jobs to Google Sheets."""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

from .scrape_linkedin import (
    authorise_sheet,
    ensure_storage,
    append_to_sheet,
    get_db_connection,
    load_settings,
    mark_jobs_appended,
    prepare_sheet_rows,
    setup_logging,
)

BATCH_SIZE = 25


def chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def fetch_pending_jobs(db_path: str) -> List[Dict[str, Any]]:
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT job_id, title, company, location, posting_url, description, posted_at, applicants, scraped_at "
            "FROM jobs WHERE appended_at IS NULL ORDER BY scraped_at"
        ).fetchall()
        return [dict(row) for row in rows]


def append_pending_jobs() -> None:
    setup_logging()
    settings = load_settings()

    ensure_storage(settings["db_path"])
    pending_jobs = fetch_pending_jobs(settings["db_path"])
    if not pending_jobs:
        logging.info("No pending jobs found; nothing to append")
        return

    logging.info("Preparing to append %s pending jobs", len(pending_jobs))

    worksheet = authorise_sheet(settings["service_account_file"], settings["spreadsheet_name"])

    appended = 0
    for batch in chunked(pending_jobs, BATCH_SIZE):
        rows, updates = prepare_sheet_rows(batch)
        try:
            append_to_sheet(rows, worksheet)
        except Exception as exc:  # noqa: BLE001
            logging.exception("Failed to append batch starting with job %s", batch[0]["job_id"], exc_info=exc)
            break
        mark_jobs_appended(settings["db_path"], updates)
        appended += len(rows)
        logging.info("Appended %s/%s pending jobs", appended, len(pending_jobs))
    else:
        logging.info("Successfully appended all pending jobs")


if __name__ == "__main__":
    append_pending_jobs()
