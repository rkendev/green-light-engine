from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb

DEFAULT_NYT_RAW_DIR = Path("data/raw/nyt")
DEFAULT_DUCKDB_PATH = Path("data/green_light.duckdb")


@dataclass
class Gate0Thresholds:
    """
    Thresholds for the Gate zero data sufficiency check.
    """

    min_weeks: int = 26
    min_goodreads_year_coverage: float = 0.90
    min_goodreads_series_coverage: float = 0.90
    min_join_rate: float = 0.80


@dataclass
class Gate0Metrics:
    """
    Measured metrics for Gate zero.
    All rates are expressed between zero and one.
    """

    nyt_weeks: int
    goodreads_year_coverage: Optional[float]
    goodreads_series_coverage: Optional[float]
    join_rate: Optional[float]
    thresholds: Gate0Thresholds

    def passes_nyt_weeks(self) -> bool:
        return self.nyt_weeks >= self.thresholds.min_weeks

    def passes_goodreads(self) -> bool:
        if self.goodreads_year_coverage is None:
            return False
        if self.goodreads_series_coverage is None:
            return False

        if self.goodreads_year_coverage < self.thresholds.min_goodreads_year_coverage:
            return False
        if (
            self.goodreads_series_coverage
            < self.thresholds.min_goodreads_series_coverage
        ):
            return False

        return True

    def passes_join_rate(self) -> bool:
        if self.join_rate is None:
            return False
        return self.join_rate >= self.thresholds.min_join_rate

    def overall_pass(self) -> bool:
        return (
            self.passes_nyt_weeks()
            and self.passes_goodreads()
            and self.passes_join_rate()
        )


@dataclass
class Gate0Config:
    """
    Configuration for Gate zero checks.
    """

    nyt_raw_dir: Path = DEFAULT_NYT_RAW_DIR
    duckdb_path: Path = DEFAULT_DUCKDB_PATH
    sample_size: int = 1000
    thresholds: Gate0Thresholds = field(default_factory=Gate0Thresholds)


def _count_nyt_weeks(nyt_raw_dir: Path) -> int:
    """
    Count how many snapshot files exist in the given directory.

    Files are expected to be named with an iso date such as
    YYYY minus MM minus DD dot json. Copies or variants with
    extra text in the stem will be counted as separate entries.
    """

    if not nyt_raw_dir.exists():
        return 0

    weeks = set()
    for path in nyt_raw_dir.glob("*.json"):
        weeks.add(path.stem)

    return len(weeks)


def _connect_duckdb(db_path: Path) -> Optional[duckdb.DuckDBPyConnection]:
    """
    Try to connect to DuckDB.
    If the file does not exist or is invalid, return None instead of raising.
    """

    if not db_path.exists():
        return None

    # If file exists but size is zero, treat as no DB
    if db_path.stat().st_size == 0:
        return None

    try:
        return duckdb.connect(str(db_path))
    except duckdb.IOException:
        # Invalid or corrupted file
        return None
    except duckdb.Error:
        return None


def _goodreads_coverage(
    con: duckdb.DuckDBPyConnection,
) -> tuple[Optional[float], Optional[float]]:
    """
    Compute coverage for publication_year and series columns in table goodreads.

    Returns (year_coverage, series_coverage) or (None, None) if table or columns
    are missing.
    """

    try:
        con.execute(
            "select 1 from information_schema.tables " "where table_name = 'goodreads'"
        )
        if con.fetchone() is None:
            return None, None

        con.execute(
            """
            select
                count(*) as total,
                sum(publication_year is not null) as year_non_null,
                sum(series is not null) as series_non_null
            from goodreads
            """
        )
        total, year_non_null, series_non_null = con.fetchone()
        if total == 0:
            return None, None

        year_cov = float(year_non_null) / float(total)
        series_cov = float(series_non_null) / float(total)
        return year_cov, series_cov
    except duckdb.Error:
        return None, None


def _join_rate(
    con: duckdb.DuckDBPyConnection,
    sample_size: int,
) -> Optional[float]:
    """
    Estimate join rate between New York Times and Goodreads on isbn13.

    The function expects tables nyt_titles and goodreads or nyt_raw and goodreads.
    It returns None if required tables or columns are missing.
    """

    try:
        con.execute(
            """
            select table_name
            from information_schema.tables
            where table_name in ('nyt_titles', 'nyt_raw')
            """
        )
        rows = [r[0] for r in con.fetchall()]
        if not rows:
            return None

        nyt_table = "nyt_titles" if "nyt_titles" in rows else "nyt_raw"

        con.execute(
            f"""
            with sample as (
                select distinct isbn13
                from {nyt_table}
                where isbn13 is not null
                limit {sample_size}
            )
            select
                count(*) as sample_size,
                sum(case when g.isbn13 is not null then 1 else 0 end) as joined
            from sample s
            left join goodreads g using (isbn13)
            """
        )
        sample_count, joined = con.fetchone()
        if sample_count == 0:
            return None

        return float(joined) / float(sample_count)
    except duckdb.Error:
        return None


def measure_gate0(config: Gate0Config) -> Gate0Metrics:
    """
    Measure Gate zero metrics using file system and DuckDB.
    """

    nyt_weeks = _count_nyt_weeks(config.nyt_raw_dir)

    year_cov: Optional[float]
    series_cov: Optional[float]
    join_rate: Optional[float]

    con = _connect_duckdb(config.duckdb_path)
    if con is None:
        year_cov = None
        series_cov = None
        join_rate = None
    else:
        year_cov, series_cov = _goodreads_coverage(con)
        join_rate = _join_rate(con, config.sample_size)
        con.close()

    return Gate0Metrics(
        nyt_weeks=nyt_weeks,
        goodreads_year_coverage=year_cov,
        goodreads_series_coverage=series_cov,
        join_rate=join_rate,
        thresholds=config.thresholds,
    )


def _format_rate(value: Optional[float]) -> str:
    if value is None:
        return "n a"
    return f"{value:.1%}"


def print_report(metrics: Gate0Metrics) -> None:
    """
    Print a short human readable report for Gate zero.
    """

    print("Gate zero data sufficiency report")
    print("")

    print(
        f"New York Times weeks          {metrics.nyt_weeks} "
        f"(required at least {metrics.thresholds.min_weeks})"
    )
    print(
        f"Goodreads year coverage       "
        f"{_format_rate(metrics.goodreads_year_coverage)} "
        f"(required at least "
        f"{metrics.thresholds.min_goodreads_year_coverage:.0%})"
    )
    print(
        f"Goodreads series coverage     "
        f"{_format_rate(metrics.goodreads_series_coverage)} "
        f"(required at least "
        f"{metrics.thresholds.min_goodreads_series_coverage:.0%})"
    )
    print(
        f"NYT to Goodreads join rate    "
        f"{_format_rate(metrics.join_rate)} "
        f"(required at least {metrics.thresholds.min_join_rate:.0%})"
    )

    print("")
    print(f"Pass weeks condition        {metrics.passes_nyt_weeks()}")
    print(f"Pass Goodreads condition    {metrics.passes_goodreads()}")
    print(f"Pass join condition         {metrics.passes_join_rate()}")
    print("")
    print(f"Gate zero overall pass      {metrics.overall_pass()}")


def main() -> None:
    config = Gate0Config()
    metrics = measure_gate0(config)
    print_report(metrics)

    if metrics.overall_pass():
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
