from pathlib import Path

from gle.gate0_check import Gate0Metrics, Gate0Thresholds, _count_nyt_weeks


def test_count_nyt_weeks_counts_unique_stems(tmp_path: Path) -> None:
    # Create fake NYT json files
    (tmp_path / "2025-01-06.json").write_text("{}", encoding="utf-8")
    (tmp_path / "2025-01-13.json").write_text("{}", encoding="utf-8")
    # Copy with extra text in the stem counts as another entry for now
    (tmp_path / "2025-01-13.copy.json").write_text("{}", encoding="utf-8")

    weeks = _count_nyt_weeks(tmp_path)
    assert weeks == 3


def test_gate0_metrics_pass_logic_all_true() -> None:
    thresholds = Gate0Thresholds(
        min_weeks=10,
        min_goodreads_year_coverage=0.8,
        min_goodreads_series_coverage=0.8,
        min_join_rate=0.7,
    )

    metrics = Gate0Metrics(
        nyt_weeks=12,
        goodreads_year_coverage=0.9,
        goodreads_series_coverage=0.85,
        join_rate=0.8,
        thresholds=thresholds,
    )

    assert metrics.passes_nyt_weeks()
    assert metrics.passes_goodreads()
    assert metrics.passes_join_rate()
    assert metrics.overall_pass()


def test_gate0_metrics_fail_logic_missing_values() -> None:
    thresholds = Gate0Thresholds(
        min_weeks=10,
        min_goodreads_year_coverage=0.8,
        min_goodreads_series_coverage=0.8,
        min_join_rate=0.7,
    )

    metrics = Gate0Metrics(
        nyt_weeks=5,
        goodreads_year_coverage=None,
        goodreads_series_coverage=None,
        join_rate=None,
        thresholds=thresholds,
    )

    assert not metrics.passes_nyt_weeks()
    assert not metrics.passes_goodreads()
    assert not metrics.passes_join_rate()
    assert not metrics.overall_pass()
