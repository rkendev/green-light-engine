from datetime import datetime, timezone

from gle.ingest_nyt import iter_mondays, last_monday_utc


def test_last_monday_utc_with_reference_date():
    # Reference is a Wednesday
    reference = datetime(2025, 11, 12, 15, 30, tzinfo=timezone.utc)
    monday = last_monday_utc(reference)

    assert monday.year == 2025
    assert monday.month == 11
    # Seven days back from Wednesday 12 November is Monday 3 November
    assert monday.day == 3
    assert monday.hour == 0
    assert monday.minute == 0
    assert monday.second == 0
    assert monday.microsecond == 0


def test_iter_mondays_simple_range():
    mondays = list(iter_mondays("2025-11-03", "2025-11-17"))
    assert mondays == [
        "2025-11-03",
        "2025-11-10",
        "2025-11-17",
    ]


def test_iter_mondays_single_day_range():
    mondays = list(iter_mondays("2025-11-03", "2025-11-03"))
    assert mondays == ["2025-11-03"]
