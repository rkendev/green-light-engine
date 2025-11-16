from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import requests


DEFAULT_RAW_DIR = Path("data/raw/nyt")


@dataclass(frozen=True)
class NytIngestConfig:
    """
    Configuration for New York Times full overview ingestion.
    """

    api_key: str
    raw_dir: Path = DEFAULT_RAW_DIR
    timeout_seconds: float = 15.0


def ensure_raw_dir(path: Path) -> Path:
    """
    Ensure the target directory exists and return it.
    """

    path.mkdir(parents=True, exist_ok=True)
    return path


def last_monday_utc(reference: Optional[datetime] = None) -> datetime:
    """
    Return the previous Monday in UTC relative to the given reference.

    If no reference is provided the current time in UTC is used.
    The result is truncated to midnight.
    """

    if reference is None:
        reference = datetime.now(timezone.utc)

    # Python monday is weekday zero
    days_since_monday = reference.weekday()
    monday = reference - timedelta(days=days_since_monday + 7)
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return monday


def iter_mondays(start_iso: str, end_iso: str) -> Iterable[str]:
    """
    Yield ISO dates for mondays from start to end inclusive.

    Both start_iso and end_iso must be dates in YYYY minus MM minus DD format.
    """

    start = datetime.fromisoformat(start_iso)
    end = datetime.fromisoformat(end_iso)

    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current = current + timedelta(days=7)


def fetch_one_overview(config: NytIngestConfig, monday_iso: str) -> Dict:
    """
    Fetch the New York Times full overview for a given monday.

    Raises requests.HTTPError if the remote endpoint returns an error.
    """

    url = (
        "https://api.nytimes.com/svc/books/v3/lists/full-overview.json"
        f"?api-key={config.api_key}&published_date={monday_iso}"
    )

    response = requests.get(url, timeout=config.timeout_seconds)
    response.raise_for_status()
    return response.json()


def save_snapshot(payload: Dict, monday_iso: str, raw_dir: Path) -> Path:
    """
    Save a single snapshot payload to disk under the given directory.

    The file is named YYYY minus MM minus DD dot json.
    """

    ensure_raw_dir(raw_dir)
    output_path = raw_dir / f"{monday_iso}.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def ingest_range(config: NytIngestConfig, start_iso: str, end_iso: str) -> None:
    """
    Fetch one snapshot per monday from start_iso to end_iso inclusive
    and write each payload to disk.
    """

    for monday_iso in iter_mondays(start_iso, end_iso):
        print(f"Fetching New York Times snapshot for {monday_iso}")
        payload = fetch_one_overview(config, monday_iso)
        output_path = save_snapshot(payload, monday_iso, config.raw_dir)
        print(f"Saved snapshot to {output_path}")


def ingest_one_monday(config: NytIngestConfig, monday_iso: str) -> None:
    """
    Convenience wrapper to fetch one monday only.
    """

    ingest_range(config, monday_iso, monday_iso)
