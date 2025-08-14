# flows/nyt_ingest.py
#!/usr/bin/env python
import json
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

RAW_DIR = Path("data/raw/nyt")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def last_monday_utc() -> datetime:
    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=(now.weekday() + 7))
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return monday

def fetch_one_overview(api_key: str, monday_iso: str) -> dict:
    url = (
        "https://api.nytimes.com/svc/books/v3/lists/full-overview.json"
        f"?api-key={api_key}&published_date={monday_iso}"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def save_snapshot(payload: dict, monday_iso: str) -> Path:
    out = RAW_DIR / f"{monday_iso}.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out

def iter_mondays(start_iso: str, end_iso: str):
    dt = datetime.fromisoformat(start_iso)
    end = datetime.fromisoformat(end_iso)
    while dt <= end:
        yield dt.strftime("%Y-%m-%d")
        dt += timedelta(days=7)

def ingest_range(start_iso: str, end_iso: str):
    api_key = os.environ["NYT_API_KEY"]
    for monday_iso in iter_mondays(start_iso, end_iso):
        print(f"▶ Fetching NYT snapshot for {monday_iso}")
        payload = fetch_one_overview(api_key, monday_iso)
        out = save_snapshot(payload, monday_iso)
        print(f"✓ Saved {out}")

if __name__ == "__main__":
    load_dotenv()
    default_date = last_monday_utc().strftime("%Y-%m-%d")
    p = ArgumentParser()
    p.add_argument("--date", default=default_date,
                   help="Fetch exactly one Monday (default: last Monday UTC)")
    p.add_argument("--start", help="Range mode: first Monday (inclusive)")
    p.add_argument("--end", help="Range mode: last Monday (inclusive)")
    args = p.parse_args()

    if args.start and args.end:
        ingest_range(args.start, args.end)
    else:
        ingest_range(args.date, args.date)
