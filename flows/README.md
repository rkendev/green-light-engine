Prefect flow scripts
# Green-Light Engine — **flows** directory

This folder hosts the one-off ETL / helper scripts that populate and enrich the project-wide **DuckDB** database.



_All commands below assume you are in the project root **and** the virtual-env is activated (`(.venv)` prompt)._



---



## 1  Prerequisites



| requirement | why |

|-------------|-----|

| **Python 3.11 +** | all scripts verified on 3.11 |

| `pip install -r requirements.txt` | pulls **duckdb**, **pandas**, **pyarrow**, **rapidfuzz**, … |

| **Raw data** |  |

| `data/raw/goodreads/` | 23 book-chunk **CSV** files (˜ 1.4 GB compressed) |

| `data/raw/nyt/` | NYT API snapshots – *one JSON per Sunday* |

| `data/raw/hardcover/` | scraped Hardcover catalog – *one JSON per book* |



<details>

<summary>Project tree (excerpt)</summary>



```text

project/

+- data/

¦  +- raw/

¦  ¦  +- goodreads/

¦  ¦  ¦   +- book0001-999999.csv …      # 23 files

¦  ¦  +- nyt/

¦  ¦  ¦   +- 2024-12-22.json …

¦  ¦  +- hardcover/

¦  ¦      +- abc123.json …

¦  +- green_light.duckdb                # created by the flows

+- flows/

   +- goodreads_ingest.py

   +- nyt_ingest.py

   +- hardcover_client.py / hardcover_probe.py

   +- fuzzy_nyt_gr.py

   +- models.py

```

2 Quick-start — full rebuild

# 1 Goodreads ? creates/overwrites table `goodreads`

python flows/goodreads_ingest.py --reset



# 2 NYT & Hardcover raw snapshots

python flows/nyt_ingest.py            # parses data/raw/nyt/*.json

python flows/hardcover_probe.py       # (or run hardcover_client.py first)



# 3 Fuzzy matching ? copies ratings onto the NYT ISBN-13s

python flows/fuzzy_nyt_gr.py \

       --threshold 85 \

       --title-threshold 94 \

       --max-cands 2000 \

       --use-series

Afterwards all NYT ISBN-13s hold average_rating and ratings_count.



3 Script reference

script	purpose	key CLI flags

goodreads_ingest.py	Load the 23 Goodreads book-chunk CSVs into DuckDB.

• Converts ISBN-10 ? 13

• Strips trailing “.” from Authors

• Keeps optional Series

• De-dupes on isbn13, adds indexes	--reset

nyt_ingest.py	Parse weekly NYT snapshots ? table gl.nyt_raw	(none)

hardcover_client.py	Fetch Hardcover metadata by ISBN, save JSON	--isbn --outfile

hardcover_probe.py	Load Hardcover JSON dump ? table gl.hc_raw	(none)

fuzzy_nyt_gr.py	Two-stage matcher that attaches Goodreads ratings to unmatched NYT ISBN-13s.

Stage 1 author-surname + token-sort • Stage 2 title-only WRatio = threshold	--threshold (85) · --title-threshold (94) · --max-cands (2000) · --use-series · --show-misses
models.py	Placeholder for downstream ML / evaluation code	—


4 fuzzy_nyt_gr.py — practical recipes
goal	recommended flags
Full first pass	--threshold 85 --title-threshold 94 --max-cands 2000 --use-series

Fast incremental run	--threshold 90 --max-cands 500
Debug unmatched titles	add --show-misses


The script is idempotent — the UNIQUE (isbn13) index on goodreads guards against duplicates.


5 Troubleshooting
symptom	fix
duckdb.duckdb.ConstraintException during ingest	Duplicate isbn13s already present — rebuild with:
python flows/goodreads_ingest.py --reset
Catalog Error: … nyt_raw does not exist	Run flows/nyt_ingest.py first (or attach the gl schema).
Fuzzy step feels slow	Lower --max-cands; raising --threshold to = 90 also shrinks candidate pools.


6 License & acknowledgements
Data © their respective owners (Goodreads, New York Times, Hardcover).
Code released under the MIT License — see project root.
