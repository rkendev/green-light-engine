#!/usr/bin/env python
"""
NYT full-overview fetcher

• **One-Monday mode (default)** – grabs exactly one snapshot
  (the most recent Monday in UTC) and saves it to
  `data/raw/nyt/YYYY-MM-DD.json`.

• **Range mode** – `--start YYYY-MM-DD --end YYYY-MM-DD`
  keeps the original behaviour for bulk back-fills.

Usage examples
--------------
poetry run python flows/nyt_ingest.py
poetry run python flows/nyt_ingest.py --date 2025-06-02
poetry run python flows/nyt_ingest.py --start 2024-01-01 --end 2024-03-25
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── env / paths ────────────────────────────────────────────────
load_dotenv()
API_KEY = os.environ["NYT_API_KEY"]

RAW_DIR = Path(__file__).parents[1] / "data" / "raw" / "nyt"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# ── helpers ────────────────────────────────────────────────────
def last_monday_utc() -> datetime:
    """Return the most recent Monday 00:00 UTC (always ≥ 7 days ago)."""
    today = datetime.now(timezone.utc).date()
    return datetime.combine(
        today - timedelta(days=today.weekday() + 7), datetime.min.time()
    )


def fetch_week(date_str: str, retries: int = 3) -> dict:
    """Call NYT Books API once; retry up to *retries* on transient errors."""
    url = (
        "https://api.nytimes.com/svc/books/v3/lists/full-overview.json"
        f"?api-key={API_KEY}&published_date={date_str}"
    )
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 (tiny util script)
            if attempt == retries:
                raise
            print(f"[warn] {date_str} failed ({exc}); retrying…")


def save_json(payload: dict, date_str: str) -> Path:
    fp = RAW_DIR / f"{date_str}.json"
    fp.write_text(json.dumps(payload))
    return fp


def ingest_range(start: str, end: str) -> None:
    """Fetch every Monday between *start* and *end* (inclusive)."""
    dt, stop = map(datetime.fromisoformat, (start, end))
    while dt <= stop:
        iso = dt.strftime("%Y-%m-%d")
        print("Fetching", iso)
        save_json(fetch_week(iso), iso)
        dt += timedelta(days=7)
    print("Done!")


# ── CLI entry-point ────────────────────────────────────────────
if __name__ == "__main__":
    from argparse import ArgumentParser

    p = ArgumentParser()
    # one-shot mode
    default_start = last_monday_utc().strftime("%Y-%m-%d")
    p.add_argument(
        "--date",
        default=default_start,
        help="ISO date (Monday) to fetch; default = last Monday UTC",
    )
    # legacy range mode
    p.add_argument("--start", help="range mode: first Monday (inclusive)")
    p.add_argument("--end", help="range mode: last Monday (inclusive)")
    args = p.parse_args()

    if args.start or args.end:
        if not (args.start and args.end):
            p.error("--start and --end must be given together")
        ingest_range(args.start, args.end)
    else:
        iso = args.date
        print("Fetching", iso)
        out = save_json(fetch_week(iso), iso)
        print(f"✓  saved {out.relative_to(Path.cwd())}")
