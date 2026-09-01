from datetime import datetime, timezone

from plugins.module_utils.date_window import resolve_run_window


def test_default_date_uses_configured_timezone():
    selected, start, end = resolve_run_window(
        None,
        "Asia/Kolkata",
        now=datetime(2026, 9, 1, 20, 0, tzinfo=timezone.utc),
    )

    assert selected.isoformat() == "2026-09-02"
    assert start == "2026-09-01T18:30:00+00:00"
    assert end == "2026-09-02T18:30:00+00:00"


def test_explicit_historical_date_overrides_today():
    selected, start, end = resolve_run_window(
        "2026-08-28",
        "Asia/Kolkata",
        now=datetime(2026, 9, 1, 20, 0, tzinfo=timezone.utc),
    )

    assert selected.isoformat() == "2026-08-28"
    assert start == "2026-08-27T18:30:00+00:00"
    assert end == "2026-08-28T18:30:00+00:00"
