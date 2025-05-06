#!/usr/bin/env python
"""
Load the 23 Goodreads *book-chunk* CSVs into DuckDB (`goodreads` table).

• Converts ISBN-10 → ISBN-13 (Python UDF)
• Strips a trailing “.” from Authors
• Keeps the optional Series column (any spelling ‘Series’ or ‘series’);
  if it isn’t present in the files we still create an empty string column.
• De-duplicates – one row per ISBN-13 (most ratings → best rating → lowest id)
• Adds indexes:  UNIQUE(isbn13)  +  authors  +  series
Schema
──────
book_id · isbn13 · title · authors · series · average_rating · ratings_count
"""
# ── stdlib ─────────────────────────────────────────────────────────────
import argparse, csv, glob, pathlib, re, sys, time
# ── 3rd-party ──────────────────────────────────────────────────────────
import duckdb

# ── CLI ────────────────────────────────────────────────────────────────
cli = argparse.ArgumentParser()
cli.add_argument("--reset", action="store_true",
                 help="drop the table before (re)loading")
args = cli.parse_args()

# ── paths ──────────────────────────────────────────────────────────────
HERE     = pathlib.Path(__file__).resolve().parent
RAW_DIR  = HERE.parent / "data" / "raw" / "goodreads"
DB_FILE  = HERE.parent / "data" / "green_light.duckdb"

FILES = sorted(fp for fp in glob.glob(str(RAW_DIR / "book*csv"))
               if "-" in pathlib.Path(fp).stem)
if not FILES:
    sys.exit("❌  no book-chunk CSVs found under data/raw/goodreads")

# ── find (or not) a Series column name ────────────────────────────────
with open(FILES[0], newline='', encoding='utf-8', errors='ignore') as f:
    header = next(csv.reader(f))
header_lower = [h.lower() for h in header]
if   "series"  in header_lower: SERIES_COL = header[header_lower.index("series")]
elif "series." in header_lower: SERIES_COL = header[header_lower.index("series.")]
else:                           SERIES_COL = None        # not present at all

# expression used in SQL ↓↓↓
if SERIES_COL:
    SERIES_EXPR = f'COALESCE("{SERIES_COL}", \'\') AS series_raw'
else:                        # create an empty column so schema is stable
    SERIES_EXPR = "''::VARCHAR                AS series_raw"

# ── helper UDF  (ISBN-10 → ISBN-13) ───────────────────────────────────
def isbn10_to13(isbn10: str | None) -> str | None:
    if not isbn10:
        return None
    d = re.sub(r"[^0-9Xx]", "", isbn10)
    if len(d) != 10:
        return None
    body  = "978" + d[:9]
    chk   = (10 - sum((1,3)[i & 1] * int(x) for i, x in enumerate(body)) % 10) % 10
    return body + str(chk)

# ── ingest ────────────────────────────────────────────────────────────
print(f"=== Goodreads ingest started  ({len(FILES)} chunks) ===")
t0  = time.time()
con = duckdb.connect(DB_FILE)
con.create_function("isbn10_to13", isbn10_to13)

if args.reset:
    con.execute("DROP TABLE IF EXISTS goodreads")
    print("• table dropped (--reset)")

sql = f"""
CREATE OR REPLACE TABLE goodreads AS
WITH raw AS (
    SELECT *
    FROM read_csv_auto(
            ?, header=TRUE, union_by_name=TRUE, sample_size=-1)
),
mapped AS (
    SELECT
        CAST("Id"         AS INTEGER)                AS book_id,
        "ISBN"                                      AS isbn_raw,
        "Name"                                      AS title,
        regexp_replace("Authors", '\\\\.$', '')      AS authors,
        {SERIES_EXPR},                               -- ← dynamic!
        "Rating"::DOUBLE                            AS average_rating,
        "CountsOfReview"::INTEGER                   AS ratings_count
    FROM raw
),
cleaned AS (
    SELECT
        book_id, title, authors,
        series_raw               AS series,
        average_rating, ratings_count,
        CASE
            WHEN regexp_matches(isbn_raw,'^[0-9]{{13}}$')    THEN isbn_raw
            WHEN regexp_matches(isbn_raw,'^[0-9Xx]{{10}}$')  THEN isbn10_to13(isbn_raw)
        END                                   AS isbn13
    FROM mapped
    WHERE isbn_raw IS NOT NULL
),
dedup AS (
    SELECT *
    FROM   cleaned
    WHERE  isbn13 IS NOT NULL
      AND  length(isbn13)=13
    QUALIFY row_number() OVER (
              PARTITION BY isbn13
              ORDER BY ratings_count DESC NULLS LAST,
                       average_rating DESC NULLS LAST,
                       book_id
            ) = 1
)
SELECT  book_id, isbn13, title, authors, series,
        average_rating, ratings_count
FROM dedup;
"""
con.execute(sql, [FILES])

# ── indexes / constraints ──────────────────────────────────────────────
con.execute("CREATE UNIQUE INDEX IF NOT EXISTS goodreads_isbn13_uidx "
            "ON goodreads(isbn13);")
con.execute("CREATE INDEX IF NOT EXISTS goodreads_authors_idx "
            "ON goodreads(authors);")
con.execute("CREATE INDEX IF NOT EXISTS goodreads_series_idx "
            "ON goodreads(series);")

print(f"✓ Goodreads rows ingested: "
      f"{con.sql('SELECT COUNT(*) FROM goodreads').fetchone()[0]:,}")
print(f"🕒  finished in {time.time()-t0:.1f}s")
con.close()