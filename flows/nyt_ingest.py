#!/usr/bin/env python
"""
Command line entry point for New York Times ingestion.

This script reads the NYT_API_KEY from the environment or a dot env file
and then calls the reusable functions from gle.ingest_nyt.
"""

from __future__ import annotations

import os
from argparse import ArgumentParser
from datetime import datetime, timezone

from dotenv import load_dotenv

from gle.ingest_nyt import (
    NytIngestConfig,
    ingest_one_monday,
    ingest_range,
    last_monday_utc,
)


def get_required_env(name: str) -> str:
    """
    Read a required environment variable or raise a clear error.
    """

    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Environment variable {name} is required but not set. "
            "Create a dot env file with NYT_API_KEY or export it in your shell."
        )
    return value


def parse_args() -> ArgumentParser:
    parser = ArgumentParser(description="Fetch New York Times full overview snapshots.")
    default_date = last_monday_utc().strftime("%Y-%m-%d")

    parser.add_argument(
        "--date",
        default=default_date,
        help=f"Fetch exactly one monday (default last monday UTC which is {default_date})",
    )
    parser.add_argument(
        "--start",
        help="Range mode first monday inclusive in YYYY minus MM minus DD format",
    )
    parser.add_argument(
        "--end",
        help="Range mode last monday inclusive in YYYY minus MM minus DD format",
    )

    return parser


def main() -> None:
    load_dotenv()

    api_key = get_required_env("NYT_API_KEY")
    config = NytIngestConfig(api_key=api_key)

    parser = parse_args()
    args = parser.parse_args()

    if args.start and args.end:
        ingest_range(config, args.start, args.end)
    else:
        ingest_one_monday(config, args.date)


if __name__ == "__main__":
    main()
