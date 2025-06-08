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


## 6  Prefect flow quick-start — _pull_latest_nyt_

| requirement | why |
|-------------|-----|
| `prefect>=2.14,<3` | the flow is written for Prefect 2.x |
| `export PREFECT_API_ENABLE=false` <br>(or Windows `set …`) | disables the embedded API server so the flow runs instantly in “ephemeral” mode |

```bash
# 1 activate the poetry v-env first
poetry shell         # or `poetry run …` for a one-off command

# 2 (optional) skip the API server for fast, fully local runs
export PREFECT_API_ENABLE=false

# 3 kick off the flow – creates one JSON under data/raw/nyt/
python -m flows.nytimes_flow
Expected console output (abridged):


▶ Fetching NYT snapshot for 2025-05-26
✓ Saved data/raw/nyt/2025-05-26.json
4.1 Running with the Prefect CLI (alternative)

prefect flow serve \
  --path flows/nytimes_flow.py \
  --name pull_latest_nyt
The serve sub-command is the recommended way to execute a single flow from the CLI in Prefect ≥ 2.14
docs-2.prefect.io
.
If you omit the PREFECT_API_ENABLE=false toggle, Prefect spins up a temporary API server before the run. That can take a few seconds ➜ disable it for quick dev loops
docs-2.prefect.io
.

4.2 Where the weekly snapshots live
Locally → data/raw/nyt/YYYY-MM-DD.json (ignored by Git)

CI / GitHub Actions → uploaded as an artefact named
nyt-<run-number> by .github/workflows/nyt_weekly.yml

You can download the artefact from the Actions → NYT weekly crawler run summary page and drop the file into data/raw/nyt/ if you need it for offline work.

(All older section numbers move down by +1.)

---

### Why these lines?

* **`PREFECT_API_ENABLE=false`** avoids the “Timed out while attempting to connect to ephemeral Prefect API server” error you hit during local tests :contentReference[oaicite:2]{index=2}.
* **`prefect flow serve`** is the modern CLI entry-point; Prefect ≥ 2.14 removed the old `prefect run -p …` syntax :contentReference[oaicite:3]{index=3}.
* A short note on the **GitHub Action artefact** explains where the weekly JSON goes now that raw data is no longer committed.

### Running Prefect flows locally

Most helper scripts can still be called directly, but the new
`flows/nytimes_flow.py` is wrapped in **Prefect 2.x** so you get retries
and structured logging:

```bash
# one-shot, no external API/DB needed
PREFECT_API_ENABLE=false poetry run python -m flows.nytimes_flow
Setting PREFECT_API_ENABLE=false skips the (slow) ephemeral server
startup and runs the flow entirely in-process. You’ll see the same
“Saved data/raw/nyt/…json” message as before.


(That keeps the README self-contained and explains the env-flag you now use in CI.)

---

## Sources

* Official workflow-concurrency docs (example with `cancel-in-progress`) :contentReference[oaicite:6]{index=6}
* GitHub Actions artefact `retention-days` option :contentReference[oaicite:7]{index=7}
* Prefect answer showing `PREFECT_API_ENABLE=false` to disable the API server in local runs
* Poetry cache discussion (why it’s optional here) :contentReference[oaicite:9]{index=9}
* Additional GitHub Actions best-practice snippets & examples: :contentReference[oaicite:10]{index=10}

No other edits are strictly required—the workflows will pass exactly as they are after the three line tweaks above.
::contentReference[oaicite:11]{index=11}

7 License & acknowledgements
Data © their respective owners (Goodreads, New York Times, Hardcover).
Code released under the MIT License — see project root.
