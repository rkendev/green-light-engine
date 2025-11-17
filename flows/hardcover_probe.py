"""
hardcover_probe.py
------------------
• Reads unique ISBN-13s from NYT JSON files (data/raw/nyt).
• Looks each ISBN up with Hardcover’s client wrapper.
• Writes every hit to data/raw/hardcover/{isbn}.json.
• Reports join hit-rate.
"""

import json
import time
from pathlib import Path

from dotenv import load_dotenv

from flows.hardcover_client import fetch_book
from flows.models import BookDoc  # same package                   # <-- Pydantic model

load_dotenv(".env")

# -------------------------- config -----------------------------------------
NYT_DIR = Path("data/raw/nyt")
HC_DIR = Path("data/raw/hardcover")
HC_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------- helpers ----------------------------------------
def iter_nyt_isbns(limit: int = 1000):
    """Yield up to `limit` distinct ISBN-13s in date-order."""
    seen = set()
    for fp in sorted(NYT_DIR.glob("*.json")):
        data = json.loads(fp.read_text())
        for lst in data["results"]["lists"]:
            for book in lst["books"]:
                isbn = book["primary_isbn13"]
                if isbn and isbn not in seen:
                    seen.add(isbn)
                    yield isbn
                    if len(seen) == limit:
                        return


def query_hardcover(isbn: str) -> BookDoc | None:
    """Thin wrapper kept for symmetry with older code."""
    return fetch_book(isbn)


# -------------------------- main -------------------------------------------
def main(n: int = 1000, delay: float = 0.4):
    hits = misses = 0
    for idx, isbn in enumerate(iter_nyt_isbns(n), 1):
        try:
            book = query_hardcover(isbn)
            if book:
                hits += 1
                # Pydantic serialises itself, so no json.dumps() needed.
                (HC_DIR / f"{isbn}.json").write_text(book.json())
            else:
                misses += 1
        except Exception as exc:
            print(f"[warn] {isbn} ? {exc}")
            misses += 1

        if idx % 100 == 0:
            print(f"Progress {idx}/{n} — hit-rate: {hits/idx:.1%}")
        time.sleep(delay)  # polite pacing

    total = hits + misses
    print("\n=== Hardcover join-probe summary ===")
    print(f"Total looked-up  : {total}")
    print(f"Matches (hits)   : {hits}")
    print(f"No match (misses): {misses}")
    print(f"Hit-rate         : {hits/total:.1%}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1000, help="ISBNs to probe")
    ap.add_argument("--delay", type=float, default=0.4, help="sleep seconds")
    args = ap.parse_args()
    main(args.n, args.delay)
