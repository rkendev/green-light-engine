#!/usr/bin/env python
"""
NYT ⇄ Goodreads fuzzy matcher (two-stage)

Stage 1  author-surname(+prefix) filter + RapidFuzz token-sort
Stage 2  title-only fallback           + RapidFuzz WRatio

Extras
──────
--use-series   search goodreads.series as well
--show-misses  list NYT titles with zero GR candidates
"""
# ── std-lib ──────────────────────────────────────────────────────
import argparse
import pathlib
import re
import time
from typing import Dict, List

# ── 3rd-party ───────────────────────────────────────────────────
import duckdb
import pandas as pd
from rapidfuzz import fuzz, process

# ── CLI ---------------------------------------------------------
cli = argparse.ArgumentParser()
cli.add_argument("--threshold", type=int, default=85)
cli.add_argument("--max-cands", type=int, default=2_000)
cli.add_argument("--title-threshold", type=int, default=94)
cli.add_argument("--use-series", action="store_true")
cli.add_argument("--show-misses", action="store_true")
args = cli.parse_args()

# ── helpers -----------------------------------------------------
surname = lambda full: (full or "").split()[-1].lower()
_rx_title = re.compile(r"^\s*(?P<body>.*?)(?:\s*[:(].*)?$", re.VERBOSE)
clean_title = lambda t: _rx_title.match(t).group("body").strip().lower() if t else ""

# ── DB ----------------------------------------------------------
DB = pathlib.Path("data/green_light.duckdb")
con = duckdb.connect(DB, read_only=False)

nyt = con.sql(
    """
    SELECT isbn13, title, author
    FROM   green_light.nyt_raw
    WHERE  isbn13 NOT IN (SELECT isbn13 FROM goodreads)
"""
).df()

if nyt.empty:
    print("✓ Nothing left to match – all NYT ISBNs are in goodreads.")
    con.close()
    exit()

# ── Stage 1 -----------------------------------------------------
t0 = time.time()
matches: List[Dict] = []
no_cand: list[str] = []

for _, n in nyt.iterrows():
    sname = surname(n.author)
    if not sname:
        continue
    s5 = sname[:5]

    cond_sql = ["authors ILIKE '%' || ? || '%'", "authors ILIKE '%' || ? || '%'"]
    params = [sname, s5]

    if args.use_series:
        cond_sql += [
            "series ILIKE '%' || ? || '%'",
            "series ILIKE '%' || ? || '%'",
            "(authors = '' AND series ILIKE '%' || ? || '%')",
        ]
        params += [sname, s5, sname]

    where_clause = " OR ".join(cond_sql)
    params.append(args.max_cands)  # LIMIT

    cand = con.execute(
        f"""
        SELECT isbn13, title, average_rating, ratings_count, book_id
        FROM   goodreads
        WHERE  ({where_clause})
          AND  (average_rating IS NOT NULL OR authors = '')
        LIMIT  ?
    """,
        params,
    ).df()

    if cand.empty:  # ← fixed
        no_cand.append(n.title)
        continue

    cand["c_title"] = cand["title"].map(clean_title)
    best = process.extractOne(
        clean_title(n.title), cand["c_title"], scorer=fuzz.token_sort_ratio
    )
    if best and best[1] >= args.threshold:
        g = cand.loc[cand["c_title"] == best[0]].iloc[0]
        matches.append(
            dict(
                nyt_isbn13=n.isbn13,
                book_id=g.book_id,
                avg_rating=g.average_rating,
                ratings_count=g.ratings_count,
                score=best[1],
                stage="surname",
            )
        )

# ── Stage 2 ------------------------------------------------------
remaining = nyt[~nyt["isbn13"].isin([m["nyt_isbn13"] for m in matches])]
if not remaining.empty:
    gr_all = con.sql(
        """
        SELECT isbn13, title, average_rating, ratings_count, book_id
        FROM   goodreads
        WHERE  average_rating IS NOT NULL
    """
    ).df()
    gr_all["c_title"] = gr_all["title"].map(clean_title)

    for _, n in remaining.iterrows():
        best = process.extractOne(
            clean_title(n.title), gr_all["c_title"], scorer=fuzz.WRatio
        )
        if best and best[1] >= args.title_threshold:
            g = gr_all.loc[gr_all["c_title"] == best[0]].iloc[0]
            matches.append(
                dict(
                    nyt_isbn13=n.isbn13,
                    book_id=g.book_id,
                    avg_rating=g.average_rating,
                    ratings_count=g.ratings_count,
                    score=best[1],
                    stage="title",
                )
            )

# ── summary & upsert -------------------------------------------
elapsed = time.time() - t0
stage_ct = (
    pd.DataFrame(matches)
    .stage.value_counts()
    .reindex(["surname", "title"])
    .fillna(0)
    .astype(int)
    .to_dict()
)

print(
    f"✓ {len(matches)} matches "
    f"(surname {stage_ct.get('surname',0)} | title {stage_ct.get('title',0)}) "
    f"in {elapsed:,.1f}s"
)

if matches:
    con.register("m_stage", pd.DataFrame(matches))
    con.execute(
        """
        INSERT INTO goodreads (book_id, isbn13, average_rating, ratings_count)
        SELECT book_id, nyt_isbn13, avg_rating, ratings_count
        FROM   m_stage
        ON CONFLICT DO NOTHING
    """
    )
    print("✓ Ratings inserted into goodreads")

if args.show_misses and no_cand:
    print("\nNYT titles with no GR candidates:")
    for t in no_cand:
        print(" •", t)

con.close()
