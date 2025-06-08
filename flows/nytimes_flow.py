#!/usr/bin/env python
"""
Prefect flow: pull the *latest* NYT “full-overview” snapshot
and save it under data/raw/nyt/…  (exactly the same side-effect
as running `flows/nyt_ingest.py` with --date ⟨last-Monday⟩).

• Runs locally in < 30 s.
• Logs every print() so you can see API-call feedback in the Prefect UI.

You can trigger it ad-hoc:

    poetry run prefect run python flows/nytimes_flow.py:pull_latest_nyt

…or schedule it inside Prefect Cloud/Server later (the CI job already
takes care of weekly GitHub Actions artefacts).
"""
import pathlib
import sys
from pathlib import Path

from prefect import flow, task

sys.path.append(str(pathlib.Path(__file__).parent.parent))

# Local module (the updated script you just refactored)
from flows.nyt_ingest import ingest_range, last_monday_utc

RAW_DIR = Path(__file__).parents[1] / "data" / "raw" / "nyt"


@task(retries=2, retry_delay_seconds=10)
def fetch_latest() -> Path:
    """Call the existing ingest utility for exactly one Monday."""
    monday_iso = last_monday_utc().strftime("%Y-%m-%d")
    print(f"▶ Fetching NYT snapshot for {monday_iso}")
    ingest_range(monday_iso, monday_iso)  # one-day “range”
    out = RAW_DIR / f"{monday_iso}.json"
    if not out.exists():
        raise FileNotFoundError(out)
    print(f"✓ Saved {out.relative_to(Path.cwd())}")
    return out


@flow(name="pull_latest_nyt", log_prints=True)
def pull_latest_nyt() -> None:  # no return needed; side-effect only
    """
    Prefect entry-point – wraps `fetch_latest()` so we can register /
    schedule / observe it in the Prefect UI.
    """
    fetch_latest()


# Allow `python flows/nytimes_flow.py` to run the flow directly
if __name__ == "__main__":
    pull_latest_nyt()
