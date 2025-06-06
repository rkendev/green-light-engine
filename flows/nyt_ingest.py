# flows/nyt_ingest.py  (pure Python version)
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["NYT_API_KEY"]

RAW_DIR = Path(__file__).parents[1] / "data" / "raw" / "nyt"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_week(date_str: str, retries: int = 3) -> dict:
    url = (
        "https://api.nytimes.com/svc/books/v3/lists/full-overview.json"
        f"?api-key={API_KEY}&published_date={date_str}"
    )
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == retries:
                raise
            print(f"[warn] {date_str} failed ({exc}); retrying…")


def save_json(payload: dict, date_str: str) -> Path:
    fp = RAW_DIR / f"{date_str}.json"
    fp.write_text(json.dumps(payload))
    return fp


def ingest_range(start: str, end: str):
    dt, stop = map(datetime.fromisoformat, (start, end))
    while dt <= stop:
        ds = dt.strftime("%Y-%m-%d")
        print("Fetching", ds)
        save_json(fetch_week(ds), ds)
        dt += timedelta(days=7)
    print("Done!")


if __name__ == "__main__":
    from argparse import ArgumentParser

    p = ArgumentParser()
    p.add_argument("--start", default="2018-01-01")
    p.add_argument("--end", default="2024-12-30")
    args = p.parse_args()

    ingest_range(args.start, args.end)
