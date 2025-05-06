# flows/hardcover_client.py
import os, requests
import json
from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv
from .models import BookDoc

load_dotenv(Path(".env"))

TOKEN   = os.environ["HARDCOVER_AUTH_TOKEN"]
AUTH    = TOKEN if TOKEN.lower().startswith("bearer ") else f"Bearer {TOKEN}"
HEADERS = {"Authorization": AUTH}
URL     = "https://api.hardcover.app/v1/graphql"

# ⬇️  no subselection under `stats` – bring the two numbers up a level
QUERY = """
query ($isbn: String!) {
  search(query_type: "ISBN", query: $isbn, per_page: 1, page: 1) {
    results          # ?- just the JSON scalar, no nested braces
  }
}
"""

@lru_cache(maxsize=4096)
def fetch_book(isbn: str) -> BookDoc | None:
    payload = {"query": QUERY, "variables": {"isbn": isbn}}
    resp    = requests.post(URL, json=payload, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()


    if "errors" in data:
        raise RuntimeError(data["errors"][0]["message"])


    # results is now a dict like:
    # { "found": 1, "facet_counts": [], "hits": [ { "document": { … } } ] }
    search_blob = data["data"]["search"]["results"]
    hits        = (search_blob or {}).get("hits", [])
    return BookDoc(**hits[0]["document"]) if hits else None

